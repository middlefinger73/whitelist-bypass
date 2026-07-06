package tunnel

import (
	"reflect"
	"testing"
	"time"
)

func newReliableTestTunnel(t *testing.T) *VP8DataTunnel {
	t.Helper()
	tun := NewVP8DataTunnel(nil, nil, t.Logf)
	tun.EnableReliableDelivery()
	return tun
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
	if ack := tun.ackSeq.Load(); ack != 2 {
		t.Fatalf("cumulative ack = %d, want 2", ack)
	}
}

func TestReliableAckRemovesPendingPackets(t *testing.T) {
	tun := newReliableTestTunnel(t)
	tun.pending[1] = &reliablePendingPacket{data: []byte("one")}
	tun.pending[2] = &reliablePendingPacket{data: []byte("two")}
	tun.pending[3] = &reliablePendingPacket{data: []byte("three")}

	tun.HandlePayload(encodeReliablePacket(reliableKindAck, 2, nil))

	if len(tun.pending) != 1 || tun.pending[3] == nil {
		t.Fatalf("pending after ack = %#v, want only seq 3", tun.pending)
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
