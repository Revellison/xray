package conf_test

import (
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/limiter"
	"github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/infra/conf/serial"
)

// resetLimiter keeps the process-wide limiter from leaking into other tests in
// this binary.
func resetLimiter(t *testing.T) {
	t.Helper()
	t.Cleanup(func() { limiter.Global.LoadConfig(nil, limiter.Speed{}) })
	limiter.Global.LoadConfig(nil, limiter.Speed{})
}

// TestSpeedLimitConfig checks the documented JSON shape end to end: the root
// "speedLimit" key is picked up by the real config decoder, and its kilobit
// values reach the limiter converted to bytes per second.
func TestSpeedLimitConfig(t *testing.T) {
	resetLimiter(t)

	const configJSON = `{
		"speedLimit": {
			"userLimits": {
				"user123": { "up": 10000, "down": 10000 },
				"user456": { "up": 5000,  "down": 20000 }
			},
			"default": { "up": 1000, "down": 2000 }
		}
	}`

	config, err := serial.DecodeJSONConfig(strings.NewReader(configJSON))
	if err != nil {
		t.Fatalf("failed to decode config: %v", err)
	}
	if config.SpeedLimit == nil {
		t.Fatal(`the "speedLimit" section was not picked up by the decoder`)
	}
	if err := config.SpeedLimit.Apply(); err != nil {
		t.Fatalf("failed to apply the speed limit config: %v", err)
	}

	for _, tc := range []struct {
		email string
		want  limiter.Speed
	}{
		{"user123", limiter.Speed{Up: 1250000, Down: 1250000}}, // 10000 kbps
		{"user456", limiter.Speed{Up: 625000, Down: 2500000}},  // 5000 / 20000 kbps
		{"unlisted", limiter.Speed{Up: 125000, Down: 250000}},  // the default
	} {
		if got := limiter.Global.Limit(tc.email); got != tc.want {
			t.Errorf("limit of %q = %+v, want %+v", tc.email, got, tc.want)
		}
	}
	if !limiter.Global.Enabled() {
		t.Error("the limiter should be enabled after loading limits")
	}
}

func TestSpeedLimitConfigWithoutDefault(t *testing.T) {
	resetLimiter(t)

	const configJSON = `{
		"speedLimit": {
			"userLimits": { "capped": { "up": 800, "down": 800 } }
		}
	}`

	config, err := serial.DecodeJSONConfig(strings.NewReader(configJSON))
	if err != nil {
		t.Fatalf("failed to decode config: %v", err)
	}
	if err := config.SpeedLimit.Apply(); err != nil {
		t.Fatalf("failed to apply the speed limit config: %v", err)
	}

	if got := limiter.Global.Limit("capped"); got != (limiter.Speed{Up: 100000, Down: 100000}) {
		t.Errorf("limit of the listed user = %+v", got)
	}
	if limiter.Global.Limit("unlisted").IsLimited() {
		t.Error("without a default, unlisted users must stay unlimited")
	}
}

func TestSpeedLimitConfigRejectsNegative(t *testing.T) {
	resetLimiter(t)

	limits := &conf.SpeedLimitConfig{
		UserLimits: map[string]conf.SpeedLimitEntry{"broken": {Up: -1, Down: 100}},
	}
	if err := limits.Apply(); err == nil {
		t.Error("a negative limit must be rejected")
	}
	// Apply validates everything before loading anything, so a bad config must
	// not leave the limiter half configured.
	if limiter.Global.Limit("broken").IsLimited() {
		t.Error("a rejected config must not reach the limiter")
	}
}
