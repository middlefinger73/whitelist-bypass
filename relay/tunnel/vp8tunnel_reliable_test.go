package tunnel

import (
	"reflect"
	"testing"
	"time"

	"whitelist-bypass/relay/common"
)

func newReliableTestTunnel(t *testing.T) *VP8DataTunnel {
	t.Helper()
	tun := NewVP8DataTunnel(nil, nil, t.Logf)
	tun.EnableReliableDelivery()
	return tun
}

func TestReliableMaxRelayPayloadFitsOneMediaPacket(t *testing.T) {
	obf, err := NewTunnelObfuscator([]byte("test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	relayFrame := EncodeFrame(1, MsgData, make([]byte, common.VP8BufSize))
	reliableFrame := encodeReliablePacket(reliableKindData, 1, relayFrame)
	mediaPayload := obf.EncodeData(reliableFrame)

	if len(mediaPayload) > 1200 {
		t.Fatalf("encrypted media payload = %d bytes, want <= 1200", len(mediaPayload))
	}
}

func TestReliableDeliveryReordersAndDeduplicates(t *testing.T) {
	tun := newReliableTestTunnel(t)
	var got []string
	tun.SetOnData(func(data []byte) { got = append(got, string(data)) })

	tun.HandlePayload(encodeReliablePacket(reliableKindData, 2, []byte("second")))
	if len(got) != 0 {
		t.Fatalf("delivered out-of-order packet early: %v", got)
	}
	tun.HandlePayload(encodeReliablePacket(reliableKindData, 1, []byte("first")))
	tun.HandlePayload(encodeReliablePacket(reliableKindData, 1, []byte("duplicate")))

	want := []string{"first", "second"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("delivery order = %v, want %v", got, want)
	}
	tun.ackMu.Lock()
	ack := tun.ackSeq
	tun.ackMu.Unlock()
	if ack != 2 {
		t.Fatalf("cumulative ack = %d, want 2", ack)
	}
}

func TestReliableAckRemovesPendingPackets(t *testing.T) {
	tun := newReliableTestTunnel(t)
	tun.pending[1] = &reliablePendingPacket{data: []byte("one")}
	tun.pending[2] = &reliablePendingPacket{data: []byte("two")}
	tun.pending[3] = &reliablePendingPacket{data: []byte("three")}

	tun.HandlePayload(encodeReliablePacket(reliableKindAck, 2, make([]byte, reliableAckMapBytes)))

	if len(tun.pending) != 1 || tun.pending[3] == nil {
		t.Fatalf("pending after ack = %#v, want only seq 3", tun.pending)
	}
}

func TestReliableSelectiveAckRemovesPacketsAfterGap(t *testing.T) {
	tun := newReliableTestTunnel(t)
	for seq := uint32(1); seq <= 4; seq++ {
		tun.pending[seq] = &reliablePendingPacket{data: []byte{byte(seq)}}
	}
	bitmap := make([]byte, reliableAckMapBytes)
	bitmap[0] = 0b00001110 // Sequences 2, 3, and 4 after cumulative ACK 0.

	tun.HandlePayload(encodeReliablePacket(reliableKindAck, 0, bitmap))

	if len(tun.pending) != 1 || tun.pending[1] == nil {
		t.Fatalf("pending after selective ack = %#v, want only seq 1", tun.pending)
	}
}

func TestReliablePacketIsRetransmitted(t *testing.T) {
	tun := newReliableTestTunnel(t)
	packet := encodeReliablePacket(reliableKindData, 7, []byte("payload"))
	tun.pending[7] = &reliablePendingPacket{
		data:     packet,
		lastSent: time.Now().Add(-reliableRetryPeriod),
		attempts: 1,
	}

	got := tun.nextOutboundData(time.Now())
	if !reflect.DeepEqual(got, packet) {
		t.Fatalf("retransmit = %x, want %x", got, packet)
	}
	if attempts := tun.pending[7].attempts; attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestReliableSendAssignsOrderedSequenceNumbers(t *testing.T) {
	tun := newReliableTestTunnel(t)
	tun.SendData([]byte("one"))
	tun.SendData([]byte("two"))

	first := <-tun.sendQueue
	second := <-tun.sendQueue
	_, firstSeq, firstBody, firstOK := decodeReliablePacket(first)
	_, secondSeq, secondBody, secondOK := decodeReliablePacket(second)
	if !firstOK || !secondOK || firstSeq != 1 || secondSeq != 2 {
		t.Fatalf("sequences = (%d, %d), ok = (%v, %v)", firstSeq, secondSeq, firstOK, secondOK)
	}
	if string(firstBody) != "one" || string(secondBody) != "two" {
		t.Fatalf("payloads = (%q, %q)", firstBody, secondBody)
	}
}

func TestReliablePeerResetStartsSequenceFromOne(t *testing.T) {
	tun := newReliableTestTunnel(t)
	tun.SendData([]byte("stale queued"))
	tun.pending[99] = &reliablePendingPacket{data: []byte("stale pending")}
	tun.nextSendSeq = 100
	tun.nextRecvSeq = 100
	tun.recvPending[101] = []byte("stale received")

	tun.ResetReliablePeer()
	tun.SendData([]byte("fresh"))

	packet := <-tun.sendQueue
	_, seq, body, ok := decodeReliablePacket(packet)
	if !ok || seq != 1 || string(body) != "fresh" {
		t.Fatalf("first packet after reset: ok=%v seq=%d body=%q", ok, seq, body)
	}
	if len(tun.pending) != 0 || len(tun.recvPending) != 0 || tun.nextRecvSeq != 1 {
		t.Fatalf("reset retained state: pending=%d recvPending=%d nextRecv=%d", len(tun.pending), len(tun.recvPending), tun.nextRecvSeq)
	}
}
