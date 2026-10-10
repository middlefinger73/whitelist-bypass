package main

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"

	"whitelist-bypass/relay/common"
)

type TMConfig struct {
	AppVersion string
	SDKVersion string
}

func fetchConfig() (TMConfig, error) {
	var cfg TMConfig

	page, err := common.HttpGet("https://telemost.yandex.ru/")
	if err != nil {
		return cfg, fmt.Errorf("failed to fetch telemost.yandex.ru: %w", err)
	}

	stateRe := regexp.MustCompile(`<script[^>]*id="preloaded-state"[^>]*>([\s\S]*?)</script>`)
	stateMatch := stateRe.FindSubmatch(page)
	if stateMatch != nil {
		var state struct {
			Config struct {
				AppVersion string `json:"appVersion"`
			} `json:"config"`
			AppVersion string `json:"appVersion"`
		}
		if err := json.Unmarshal(stateMatch[1], &state); err != nil {
			return cfg, fmt.Errorf("failed to parse preloaded-state: %w", err)
		}
		cfg.AppVersion = state.Config.AppVersion
		if cfg.AppVersion == "" {
			cfg.AppVersion = state.AppVersion
		}
	} else {
		// Since September 2026 Telemost is served inside Yandex Messenger.
		// Its page exposes the client version through globalParams instead of
		// the old preloaded-state script.
		paramsRe := regexp.MustCompile(`var\s+globalParams\s*=\s*(\{[\s\S]*?\});`)
		paramsMatch := paramsRe.FindSubmatch(page)
		if paramsMatch == nil {
			return cfg, fmt.Errorf("Telemost config not found in page")
		}
		var params struct {
			Version string `json:"version"`
		}
		if err := json.Unmarshal(paramsMatch[1], &params); err != nil {
			return cfg, fmt.Errorf("failed to parse globalParams: %w", err)
		}
		cfg.AppVersion = params.Version
	}
	if cfg.AppVersion == "" {
		return cfg, fmt.Errorf("appVersion not found in Telemost page")
	}
	log.Printf("[config] appVersion=%s", cfg.AppVersion)

	bundleRe := regexp.MustCompile(`https://telemost\.yastatic\.net/s3/telemost/_/main\.\w+\.[a-f0-9]+\.js`)
	bundleURL := bundleRe.FindString(string(page))
	if bundleURL == "" {
		// New Messenger-hosted Telemost bundle.
		messengerBundleRe := regexp.MustCompile(`(?:https?:)?//yastatic\.net/s3/chat-static/telemessenger/_/[^"']+/web/app\.js`)
		bundleURL = messengerBundleRe.FindString(string(page))
		if strings.HasPrefix(bundleURL, "//") {
			bundleURL = "https:" + bundleURL
		}
	}
	if bundleURL == "" {
		return cfg, fmt.Errorf("Telemost main bundle URL not found in page")
	}
	log.Printf("[config] Found bundle: %s", bundleURL)

	bundle, err := common.HttpGet(bundleURL)
	if err != nil {
		return cfg, fmt.Errorf("failed to fetch bundle: %w", err)
	}

	cfg.SDKVersion = extractSDKVersion(bundle)
	if cfg.SDKVersion == "" {
		return cfg, fmt.Errorf("goloom SDK version not found in bundle")
	}

	log.Printf("[config] app=%s sdk=%s", cfg.AppVersion, cfg.SDKVersion)
	return cfg, nil
}

func extractSDKVersion(bundle []byte) string {
	sdkVerPatterns := []*regexp.Regexp{
		regexp.MustCompile(`goloom_sdk_version:"(\d+\.\d+\.\d+)"`),
		regexp.MustCompile(`"@yandex-video-platform/goloom-sdk":"(\d+\.\d+\.\d+)"`),
		regexp.MustCompile(`goloom-sdk\.(\d+\.\d+\.\d+)\.js`),
		regexp.MustCompile(`goloom-sdk[@+]([0-9]+\.[0-9]+\.[0-9]+)`),
	}
	for _, re := range sdkVerPatterns {
		if m := re.FindSubmatch(bundle); m != nil {
			return string(m[1])
		}
	}

	// New Messenger bundles embed Goloom directly and expose its build metadata
	// through the same object used to populate sdkInfo.version.
	metadataRe := regexp.MustCompile(`(?:const|let|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)=\{version:"([0-9]+\.[0-9]+\.[0-9]+)",date:"[^"]+"\}`)
	for _, match := range metadataRe.FindAllSubmatch(bundle, -1) {
		usageRe := regexp.MustCompile(`version:\s*` + regexp.QuoteMeta(string(match[1])) + `\.version\s*,\s*userAgent:`)
		if usageRe.Match(bundle) {
			return string(match[2])
		}
	}

	return ""
}
