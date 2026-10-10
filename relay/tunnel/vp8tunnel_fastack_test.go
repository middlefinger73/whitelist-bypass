package tunnel

import (
	"testing"
	"time"
)

func TestSelectiveAckFastRecoveryAfterBackoff(t *testing.T) {
	tun := newReliableTestTunnel(t)
	start := time.Unix(100, 0)
	for seq := uint32(1); seq <= 4; seq++ {
		tun.pending[seq] = &reliablePendingPacket{data: encodeReliablePacket(reliableKindData, seq, []byte("data")), firstSent: start, lastSent: start, attempts: 4}
	}
	// Packet 1 is missing while each ACK confirms a new, later packet.
	for i := 1; i <= 3; i++ {
		tun.handleReliableAckAt(0, []byte{byte(1 << i)}, start.Add(time.Duration(i)*50*time.Millisecond))
	}
	if got := tun.nextOutboundData(start.Add(249 * time.Millisecond)); got != nil {
		t.Fatal("fast retry bypassed minimum delay")
	}
	got := tun.nextOutboundData(start.Add(250 * time.Millisecond))
	_, seq, _, ok := decodeReliablePacket(got)
	if !ok || seq != 1 || tun.stats.fastRetries != 1 || tun.pending[1].attempts != 5 {
		t.Fatalf("gap not recovered: seq=%d stats=%+v", seq, tun.stats)
	}
	// Without fresh evidence, the same packet must return to normal backoff.
	if got := tun.nextOutboundData(start.Add(500 * time.Millisecond)); got != nil {
		t.Fatal("fast retry repeated without fresh evidence")
	}
}

func TestDuplicateSelectiveAckDoesNotTriggerFastRetry(t *testing.T) {
	tun := newReliableTestTunnel(t)
	start := time.Unix(100, 0)
	for seq := uint32(1); seq <= 2; seq++ {
		tun.pending[seq] = &reliablePendingPacket{data: encodeReliablePacket(reliableKindData, seq, nil), firstSent: start, lastSent: start, attempts: 1}
	}
	for i := 0; i < 10; i++ {
		tun.handleReliableAckAt(0, []byte{2}, start.Add(100*time.Millisecond))
	}
	if tun.pending[1].gapReports != 1 {
		t.Fatal("duplicate ACK counted as new gap evidence")
	}
	if got := tun.nextOutboundData(start.Add(300 * time.Millisecond)); got != nil {
		t.Fatal("duplicate ACK triggered fast retry")
	}
	if got := tun.nextOutboundData(start.Add(500 * time.Millisecond)); got == nil {
		t.Fatal("normal timeout recovery stopped")
	}
}
