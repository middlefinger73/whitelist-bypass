package joiner

import (
	"bytes"
	"testing"

	"whitelist-bypass/relay/tunnel"
)

func TestTelemostReconnectChangesReliableEpoch(t *testing.T) {
	const link = "https://telemost.yandex.ru/j/12345678901234"
	j := &TelemostHeadlessJoiner{joinLink: link, logFn: t.Logf}
	server, err := tunnel.NewTunnelObfuscator(tunnel.DeriveSecretFromJoinLink(link))
	if err != nil {
		t.Fatal(err)
	}
	if err := j.refreshSessionObfuscator(); err != nil {
		t.Fatal(err)
	}
	old := j.obf
	first := server.Decode(old.EncodeKeepalive())
	if !first.HasFrame || first.PeerRestart {
		t.Fatalf("initial session: %+v", first)
	}
	if err := j.refreshSessionObfuscator(); err != nil {
		t.Fatal(err)
	}
	if old.LocalEpoch() == j.obf.LocalEpoch() {
		t.Fatal("reconnection retained the old reliable epoch")
	}
	payload := []byte("new session data")
	next := server.Decode(j.obf.EncodeData(payload))
	if !next.PeerRestart || !bytes.Equal(next.Payload, payload) {
		t.Fatalf("creator did not detect the new session: %+v", next)
	}
	stable := server.Decode(j.obf.EncodeKeepalive())
	if !stable.HasFrame || stable.PeerRestart {
		t.Fatalf("same session unexpectedly reset: %+v", stable)
	}
}

func TestTelemostSessionObfuscatorRejectsEmptyLink(t *testing.T) {
	j := &TelemostHeadlessJoiner{logFn: t.Logf}
	if err := j.refreshSessionObfuscator(); err == nil {
		t.Fatal("expected empty join link to fail")
	}
}
