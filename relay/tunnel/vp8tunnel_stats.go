package tunnel

import "time"

type reliableTransportStats struct {
	fastRetries                                                         uint64
	sent, sentBytes, retries, retryBytes, ackMessages, acked, discarded uint64
	retryAttempts                                                       [4]uint64 // Attempts 2, 3, 4, and 5 or higher.
	ackDelayCount                                                       uint64
	ackDelayTotal, ackDelayMax                                          time.Duration
	rttCount                                                            uint64
	rttTotal, rttMax                                                    time.Duration
}

func (s *reliableTransportStats) recordAck(packet *reliablePendingPacket, now time.Time) {
	s.acked++
	if packet.firstSent.IsZero() || now.Before(packet.firstSent) {
		return
	}
	delay := now.Sub(packet.firstSent)
	s.ackDelayCount++
	s.ackDelayTotal += delay
	if delay > s.ackDelayMax {
		s.ackDelayMax = delay
	}
	// Retransmitted ACKs cannot identify which transmission was acknowledged.
	// Only packets sent once provide an unambiguous transport RTT sample.
	if packet.attempts == 1 {
		s.rttCount++
		s.rttTotal += delay
		if delay > s.rttMax {
			s.rttMax = delay
		}
	}
}

func meanMilliseconds(total time.Duration, count uint64) float64 {
	if count == 0 {
		return 0
	}
	return float64(total) / float64(count) / float64(time.Millisecond)
}

func (t *VP8DataTunnel) logReliableStats(now time.Time) {
	if !t.reliable.Load() {
		return
	}
	t.pendingMu.Lock()
	if t.statsSince.IsZero() {
		t.statsSince = now
		t.pendingMu.Unlock()
		return
	}
	elapsed := now.Sub(t.statsSince)
	if elapsed < 30*time.Second {
		t.pendingMu.Unlock()
		return
	}
	s := t.stats
	pending := len(t.pending)
	oldest := time.Duration(0)
	for _, packet := range t.pending {
		if !packet.firstSent.IsZero() && now.Sub(packet.firstSent) > oldest {
			oldest = now.Sub(packet.firstSent)
		}
	}
	t.stats = reliableTransportStats{}
	t.statsSince = now
	t.pendingMu.Unlock()
	if s.sent+s.retries+s.ackMessages+s.discarded == 0 && pending == 0 {
		return
	}
	t.logFn("vp8tunnel: stats interval_s=%.1f new=%d new_bytes=%d retries=%d retry_bytes=%d fast_retries=%d attempt2=%d attempt3=%d attempt4=%d attempt5plus=%d ack_messages=%d acked=%d discarded=%d pending=%d oldest_ms=%.1f ack_delay_n=%d ack_delay_avg_ms=%.1f ack_delay_max_ms=%.1f rtt_clean_n=%d rtt_clean_avg_ms=%.1f rtt_clean_max_ms=%.1f",
		elapsed.Seconds(), s.sent, s.sentBytes, s.retries, s.retryBytes, s.fastRetries,
		s.retryAttempts[0], s.retryAttempts[1], s.retryAttempts[2], s.retryAttempts[3],
		s.ackMessages, s.acked, s.discarded, pending, float64(oldest)/float64(time.Millisecond),
		s.ackDelayCount, meanMilliseconds(s.ackDelayTotal, s.ackDelayCount), float64(s.ackDelayMax)/float64(time.Millisecond),
		s.rttCount, meanMilliseconds(s.rttTotal, s.rttCount), float64(s.rttMax)/float64(time.Millisecond))
}
