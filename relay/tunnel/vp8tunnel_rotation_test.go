package tunnel

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

func TestPublisherRotationImmediatelySendsKeyframe(t *testing.T) {
	newTrack := func() *webrtc.TrackLocalStaticSample {
		track, err := webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "video", "test")
		if err != nil {
			t.Fatal(err)
		}
		return track
	}
	obf, err := NewTunnelObfuscator([]byte("test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	keyframes := make(chan string, 10)
	tun := NewVP8DataTunnel(newTrack(), obf, func(format string, args ...any) {
		msg := fmt.Sprintf(format, args...)
		if strings.Contains(msg, "forced keyframe") {
			keyframes <- msg
		}
	})
	tun.Start(24, 10)
	defer tun.Stop()
	waitKeyframe := func() {
		t.Helper()
		select {
		case <-keyframes:
		case <-time.After(2 * time.Second):
			t.Fatal("keyframe missing; rotation must not wait for the 30-second periodic keyframe")
		}
	}
	waitKeyframe()
	tun.SetTrack(newTrack())
	waitKeyframe()
}
