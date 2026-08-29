package conf

import (
	"github.com/xtls/xray-core/app/limiter"
	"github.com/xtls/xray-core/common/errors"
)

// SpeedLimitEntry is a per-user speed limit in kilobits per second. 0 means
// unlimited for that direction.
type SpeedLimitEntry struct {
	Up   int64 `json:"up"`
	Down int64 `json:"down"`
}

// SpeedLimitConfig is the "speedLimit" section of the config. It is kept apart
// from the inbound "clients" settings on purpose, so that a panel rewriting the
// user list does not touch the limits.
//
//	"speedLimit": {
//	  "userLimits": {
//	    "user123": { "up": 10000, "down": 10000 }
//	  },
//	  "default": { "up": 10000, "down": 10000 }
//	}
type SpeedLimitConfig struct {
	UserLimits map[string]SpeedLimitEntry `json:"userLimits"`
	Default    *SpeedLimitEntry           `json:"default"`
}

// kbpsToBytesPerSecond converts a limit as written in the config to the unit the
// limiter works in. Values of 0 stay 0, the marker for "unlimited".
func (e SpeedLimitEntry) kbpsToBytesPerSecond() (limiter.Speed, error) {
	if e.Up < 0 || e.Down < 0 {
		return limiter.Speed{}, errors.New("speed limit cannot be negative: up: ", e.Up, " down: ", e.Down)
	}
	return limiter.Speed{
		Up:   e.Up * 1000 / 8,
		Down: e.Down * 1000 / 8,
	}, nil
}

// Apply loads the limits into the global limiter. Unlike the other sections of
// the config this one does not build a message for an app module: the limiter is
// a plain singleton read by app/dispatcher, which keeps the patch out of the
// core's feature machinery.
func (c *SpeedLimitConfig) Apply() error {
	userLimits := make(map[string]limiter.Speed, len(c.UserLimits))
	for email, entry := range c.UserLimits {
		speed, err := entry.kbpsToBytesPerSecond()
		if err != nil {
			return errors.New("invalid speed limit for user ", email).Base(err)
		}
		userLimits[email] = speed
	}

	var def limiter.Speed
	if c.Default != nil {
		speed, err := c.Default.kbpsToBytesPerSecond()
		if err != nil {
			return errors.New("invalid default speed limit").Base(err)
		}
		def = speed
	}

	limiter.Global.LoadConfig(userLimits, def)
	return nil
}
