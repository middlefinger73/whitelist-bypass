package main

import "testing"

func TestExtractSDKVersion(t *testing.T) {
	tests := []struct {
		name   string
		bundle string
		want   string
	}{
		{
			name:   "legacy package version",
			bundle: `{"@yandex-video-platform/goloom-sdk":"5.28.0"}`,
			want:   "5.28.0",
		},
		{
			name: "embedded SDK metadata",
			bundle: `const other={version:"99.1.0",date:"2026-01-01"};` +
				`const an={version:"6.5.0",date:"2026-09-16T06:49:21.978Z"};` +
				`const info={implementation:"browser",version:an.version,userAgent:navigator.userAgent};`,
			want: "6.5.0",
		},
		{
			name:   "unreferenced metadata is ignored",
			bundle: `const other={version:"99.1.0",date:"2026-01-01"};`,
			want:   "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := extractSDKVersion([]byte(test.bundle)); got != test.want {
				t.Fatalf("extractSDKVersion() = %q, want %q", got, test.want)
			}
		})
	}
}
