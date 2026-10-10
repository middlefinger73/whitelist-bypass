package tunnel

import (
	"testing"
	"time"
)

func TestReliableStatsCountEveryRetry(t *testing.T) {
	tun := newReliableTestTunnel(t)
	start := time.Unix(100, 0)
	tun.SendData([]byte("data"))
	tun.nextOutboundData(start)
	now := start
	for attempt := 1; attempt <= 5; attempt++ {
		now = now.Add(reliableRetryDelay(attempt))
		if tun.nextOutboundData(now) == nil {
			t.Fatal("expected retry")
		}
	}
	if tun.stats.sent != 1 || tun.stats.retries != 5 || tun.stats.retryAttempts != [4]uint64{1, 1, 1, 2} {
		t.Fatalf("incorrect counters: %+v", tun.stats)
	}
	tun.handleReliableAckAt(1, nil, now.Add(100*time.Millisecond))
	if tun.stats.acked != 1 || tun.stats.rttCount != 0 || tun.stats.ackDelayCount != 1 {
		t.Fatalf("retransmitted ACK incorrectly counted as clean RTT: %+v", tun.stats)
	}
}

func TestReliableStatsSelectiveAckAndDuplicateAck(t *testing.T) {
	tun := newReliableTestTunnel(t)
	start := time.Unix(100, 0)
	for seq := uint32(1); seq <= 3; seq++ {
		tun.pending[seq] = &reliablePendingPacket{firstSent: start, lastSent: start, attempts: 1}
	}
	// Cumulative ACK 1 and selective ACK 3 leave packet 2 pending.
	tun.handleReliableAckAt(1, []byte{2}, start.Add(200*time.Millisecond))
	tun.handleReliableAckAt(1, []byte{2}, start.Add(300*time.Millisecond))
	if len(tun.pending) != 1 || tun.pending[2] == nil || tun.stats.acked != 2 || tun.stats.ackMessages != 2 || tun.stats.rttCount != 2 || tun.stats.rttTotal != 400*time.Millisecond {
		t.Fatalf("incorrect ACK accounting: %+v", tun.stats)
	}
}
