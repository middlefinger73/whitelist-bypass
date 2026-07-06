package tunnel

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

const (
	defaultVP8FPS       = 24
	defaultVP8Batch     = 30
	keepaliveIdlePeriod = 100 * time.Millisecond
	keyframePeriod      = 2 * time.Second
	sendQueueDepth      = 128
	reliableRetryPeriod = 250 * time.Millisecond
	reliableAckPeriod   = 20 * time.Millisecond
	reliableWindowSize  = 1024
	reliableHeaderLen   = 9
	reliableMagic       = 0x57425231 // WBR1
	reliableKindData    = 1
	reliableKindAck     = 2
)

type reliablePendingPacket struct {
	data     []byte
	lastSent time.Time
	attempts int
}

type VP8DataTunnel struct {
	track     *webrtc.TrackLocalStaticSample
	trackMu   sync.RWMutex
	logFn     func(string, ...any)
	obf       *TunnelObfuscator
	stopCh    chan struct{}
	sendQueue chan []byte
	cfgChan   chan struct{}

	stopOnce sync.Once
	running  atomic.Bool
	paused   atomic.Bool
	reliable atomic.Bool

	cfgMu sync.Mutex
	fps   int
	batch int

	sentFrames atomic.Uint64
	recvFrames atomic.Uint64

	reliableSendMu sync.Mutex
	nextSendSeq    uint32
	pendingMu      sync.Mutex
	pending        map[uint32]*reliablePendingPacket
	recvMu         sync.Mutex
	nextRecvSeq    uint32
	recvPending    map[uint32][]byte
	ackSeq         atomic.Uint32
	ackDirty       atomic.Bool
	lastAckSent    time.Time

	OnData  func([]byte)
	OnClose func()
}

func (t *VP8DataTunnel) SetTrack(track *webrtc.TrackLocalStaticSample) {
	if track == nil {
		return
	}
	t.trackMu.Lock()
	t.track = track
	t.trackMu.Unlock()
	t.paused.Store(false)
	t.logFn("vp8tunnel: publisher track rotated")
}

func (t *VP8DataTunnel) PauseTrack() {
	if t.paused.CompareAndSwap(false, true) {
		t.logFn("vp8tunnel: publisher track paused")
	}
}

func (t *VP8DataTunnel) ResumeTrack() {
	if t.paused.CompareAndSwap(true, false) {
		t.logFn("vp8tunnel: publisher track resumed")
	}
}

func (t *VP8DataTunnel) SetOnData(fn func([]byte)) { t.OnData = fn }
func (t *VP8DataTunnel) SetOnClose(fn func())       { t.OnClose = fn }

// EnableReliableDelivery adds ordered delivery, cumulative acknowledgements,
// and retransmission to the lossy VP8 media transport. Both peers must enable it.
func (t *VP8DataTunnel) EnableReliableDelivery() {
	t.reliableSendMu.Lock()
	t.pendingMu.Lock()
	t.recvMu.Lock()
	if !t.reliable.Load() {
		t.nextSendSeq = 1
		t.nextRecvSeq = 1
		t.pending = make(map[uint32]*reliablePendingPacket)
		t.recvPending = make(map[uint32][]byte)
		t.reliable.Store(true)
		t.logFn("vp8tunnel: reliable delivery enabled")
	}
	t.recvMu.Unlock()
	t.pendingMu.Unlock()
	t.reliableSendMu.Unlock()
}

func NewVP8DataTunnel(track *webrtc.TrackLocalStaticSample, obf *TunnelObfuscator, logFn func(string, ...any)) *VP8DataTunnel {
	return &VP8DataTunnel{
		track:     track,
		obf:       obf,
		logFn:     logFn,
		stopCh:    make(chan struct{}),
		sendQueue: make(chan []byte, sendQueueDepth),
		cfgChan:   make(chan struct{}, 1),
		fps:       defaultVP8FPS,
		batch:     defaultVP8Batch,
	}
}

func (t *VP8DataTunnel) Reconfigure(fps, batch int) {
	if fps <= 0 && batch <= 0 {
		return
	}
	t.cfgMu.Lock()
	changed := false
	if fps > 0 && t.fps != fps {
		t.fps = fps
		changed = true
	}
	if batch > 0 && t.batch != batch {
		t.batch = batch
		changed = true
	}
	newFPS, newBatch := t.fps, t.batch
	t.cfgMu.Unlock()
	if !changed {
		return
	}
	t.logFn("vp8tunnel: reconfigure fps=%d batch=%d", newFPS, newBatch)
	select {
	case t.cfgChan <- struct{}{}:
	default:
	}
}

func (t *VP8DataTunnel) FPS() int {
	t.cfgMu.Lock()
	defer t.cfgMu.Unlock()
	return t.fps
}

func (t *VP8DataTunnel) Batch() int {
	t.cfgMu.Lock()
	defer t.cfgMu.Unlock()
	return t.batch
}

func (t *VP8DataTunnel) SendData(data []byte) {
	if len(data) == 0 {
		return
	}
	if t.reliable.Load() {
		t.reliableSendMu.Lock()
		seq := t.nextSendSeq
		t.nextSendSeq++
		data = encodeReliablePacket(reliableKindData, seq, data)
		select {
		case t.sendQueue <- data:
		case <-t.stopCh:
		}
		t.reliableSendMu.Unlock()
		return
	}
	select {
	case t.sendQueue <- data:
	case <-t.stopCh:
	}
}

func (t *VP8DataTunnel) Start(fps, batch int) {
	t.cfgMu.Lock()
	if fps > 0 {
		t.fps = fps
	}
	if batch > 0 {
		t.batch = batch
	}
	t.cfgMu.Unlock()
	if !t.running.CompareAndSwap(false, true) {
		return
	}
	go t.writerLoop()
}

func (t *VP8DataTunnel) Stop() {
	if !t.running.CompareAndSwap(true, false) {
		return
	}
	t.stopOnce.Do(func() { close(t.stopCh) })
	if t.OnClose != nil {
		t.OnClose()
	}
}

func (t *VP8DataTunnel) currentIntervals() (sampleInterval time.Duration, keepaliveEvery, fps, batch int) {
	t.cfgMu.Lock()
	fps = t.fps
	batch = t.batch
	t.cfgMu.Unlock()

	frameInterval := time.Second / time.Duration(fps)
	sampleInterval = frameInterval
	if batch > 1 {
		sampleInterval = frameInterval / time.Duration(batch)
	}
	if sampleInterval <= 0 {
		sampleInterval = time.Millisecond
	}

	keepaliveEvery = int(keepaliveIdlePeriod / sampleInterval)
	if keepaliveEvery < 1 {
		keepaliveEvery = 1
	}
	return
}

func (t *VP8DataTunnel) writerLoop() {
	for {
		sampleInterval, keepaliveEvery, fps, batch := t.currentIntervals()
		t.logFn("vp8tunnel: writer (re)started fps=%d batch=%d sampleInterval=%s keepaliveEvery=%d",
			fps, batch, sampleInterval, keepaliveEvery)

		ticker := time.NewTicker(sampleInterval)
		idleTicks := 0
		lastKeyframe := time.Time{}
		forcedKeyframes := 0
		reconfigure := false

		for !reconfigure {
			select {
			case <-t.stopCh:
				ticker.Stop()
				return
			case <-t.cfgChan:
				reconfigure = true
			case <-ticker.C:
				if t.paused.Load() {
					continue
				}
				var sample []byte
				now := time.Now()
				forceKeyframe := lastKeyframe.IsZero() || now.Sub(lastKeyframe) >= keyframePeriod
				if forceKeyframe {
					idleTicks = 0
					sample = vp8VideoKeyframe
					lastKeyframe = now
					forcedKeyframes++
				} else {
					data := t.nextOutboundData(now)
					if data != nil {
						sample = t.obf.EncodeData(data)
						idleTicks = 0
					} else {
						idleTicks++
						if idleTicks < keepaliveEvery {
							continue
						}
						idleTicks = 0
						sample = t.obf.EncodeKeepalive()
					}
				}
				if sample == nil {
					continue
				}
				t.trackMu.RLock()
				track := t.track
				t.trackMu.RUnlock()
				if err := track.WriteSample(media.Sample{Data: sample, Duration: sampleInterval}); err != nil {
					t.logFn("vp8tunnel: WriteSample error: %v", err)
					continue
				}
				n := t.sentFrames.Add(1)
				if forceKeyframe && (forcedKeyframes <= 3 || forcedKeyframes%30 == 0) {
					t.logFn("vp8tunnel: forced keyframe #%d at frame #%d", forcedKeyframes, n)
				}
				if n <= 5 || n%500 == 0 {
					t.logFn("vp8tunnel: sent frame #%d size=%d", n, len(sample))
				}
			}
		}
		ticker.Stop()
	}
}

func (t *VP8DataTunnel) HandleFrame(frame []byte) {
	res := t.obf.Decode(frame)
	if !res.HasFrame {
		return
	}
	if res.SelfEcho {
		return
	}
	if res.PeerRestart {
		t.logFn("vp8tunnel: peer restart detected, new epoch=0x%08x", res.PeerEpoch)
		t.ResetReliablePeer()
	}
	if res.Keepalive || len(res.Payload) == 0 {
		return
	}
	n := t.recvFrames.Add(1)
	if n <= 5 || n%500 == 0 {
		t.logFn("vp8tunnel: recv frame #%d size=%d", n, len(res.Payload))
	}
	t.HandlePayload(res.Payload)
}

// HandlePayload accepts an already decrypted VP8 payload. The creator uses it
// because its SFU track reader performs VP8 frame assembly and decryption.
func (t *VP8DataTunnel) HandlePayload(payload []byte) {
	if !t.reliable.Load() {
		if t.OnData != nil {
			t.OnData(payload)
		}
		return
	}
	kind, seq, body, ok := decodeReliablePacket(payload)
	if !ok {
		t.logFn("vp8tunnel: dropped non-reliable payload while reliable delivery is enabled")
		return
	}
	switch kind {
	case reliableKindAck:
		t.handleReliableAck(seq)
	case reliableKindData:
		t.handleReliableData(seq, body)
	}
}

func (t *VP8DataTunnel) nextOutboundData(now time.Time) []byte {
	if !t.reliable.Load() {
		select {
		case data := <-t.sendQueue:
			return data
		default:
			return nil
		}
	}
	if t.ackDirty.Load() && (t.lastAckSent.IsZero() || now.Sub(t.lastAckSent) >= reliableAckPeriod) && t.ackDirty.Swap(false) {
		t.lastAckSent = now
		return encodeReliablePacket(reliableKindAck, t.ackSeq.Load(), nil)
	}

	t.pendingMu.Lock()
	var retrySeq uint32
	var retry *reliablePendingPacket
	for seq, packet := range t.pending {
		if now.Sub(packet.lastSent) < reliableRetryPeriod {
			continue
		}
		if retry == nil || packet.lastSent.Before(retry.lastSent) {
			retrySeq, retry = seq, packet
		}
	}
	if retry != nil {
		retry.lastSent = now
		retry.attempts++
		data := retry.data
		attempts := retry.attempts
		t.pendingMu.Unlock()
		if attempts == 2 || attempts%20 == 0 {
			t.logFn("vp8tunnel: retransmit seq=%d attempt=%d", retrySeq, attempts)
		}
		return data
	}
	if len(t.pending) >= reliableWindowSize {
		t.pendingMu.Unlock()
		return nil
	}
	t.pendingMu.Unlock()

	select {
	case data := <-t.sendQueue:
		_, seq, _, ok := decodeReliablePacket(data)
		if !ok {
			return nil
		}
		t.pendingMu.Lock()
		t.pending[seq] = &reliablePendingPacket{data: data, lastSent: now, attempts: 1}
		t.pendingMu.Unlock()
		return data
	default:
		return nil
	}
}

func (t *VP8DataTunnel) handleReliableAck(ack uint32) {
	t.pendingMu.Lock()
	for seq := range t.pending {
		if seq <= ack {
			delete(t.pending, seq)
		}
	}
	t.pendingMu.Unlock()
}

func (t *VP8DataTunnel) handleReliableData(seq uint32, payload []byte) {
	t.recvMu.Lock()
	if seq < t.nextRecvSeq {
		ack := t.nextRecvSeq - 1
		t.recvMu.Unlock()
		t.queueReliableAck(ack)
		return
	}
	if seq > t.nextRecvSeq {
		if seq-t.nextRecvSeq <= reliableWindowSize {
			if _, exists := t.recvPending[seq]; !exists {
				t.recvPending[seq] = append([]byte(nil), payload...)
			}
		}
		ack := t.nextRecvSeq - 1
		t.recvMu.Unlock()
		t.queueReliableAck(ack)
		return
	}

	deliver := [][]byte{append([]byte(nil), payload...)}
	t.nextRecvSeq++
	for {
		buffered, ok := t.recvPending[t.nextRecvSeq]
		if !ok {
			break
		}
		delete(t.recvPending, t.nextRecvSeq)
		deliver = append(deliver, buffered)
		t.nextRecvSeq++
	}
	ack := t.nextRecvSeq - 1
	t.recvMu.Unlock()
	t.queueReliableAck(ack)
	for _, data := range deliver {
		if t.OnData != nil {
			t.OnData(data)
		}
	}
}

func (t *VP8DataTunnel) queueReliableAck(seq uint32) {
	t.ackSeq.Store(seq)
	t.ackDirty.Store(true)
}

// ResetReliablePeer drops receive reordering state after a real process restart.
// Publisher PC rotation keeps the same obfuscator epoch and does not call this.
func (t *VP8DataTunnel) ResetReliablePeer() {
	if !t.reliable.Load() {
		return
	}
	t.recvMu.Lock()
	t.nextRecvSeq = 1
	clear(t.recvPending)
	t.recvMu.Unlock()
}

func encodeReliablePacket(kind byte, seq uint32, payload []byte) []byte {
	packet := make([]byte, reliableHeaderLen+len(payload))
	binary.BigEndian.PutUint32(packet[0:4], reliableMagic)
	packet[4] = kind
	binary.BigEndian.PutUint32(packet[5:9], seq)
	copy(packet[reliableHeaderLen:], payload)
	return packet
}

func decodeReliablePacket(packet []byte) (kind byte, seq uint32, payload []byte, ok bool) {
	if len(packet) < reliableHeaderLen || binary.BigEndian.Uint32(packet[0:4]) != reliableMagic {
		return 0, 0, nil, false
	}
	kind = packet[4]
	if kind != reliableKindData && kind != reliableKindAck {
		return 0, 0, nil, false
	}
	seq = binary.BigEndian.Uint32(packet[5:9])
	if kind == reliableKindData && seq == 0 {
		return 0, 0, nil, false
	}
	return kind, seq, packet[reliableHeaderLen:], true
}
