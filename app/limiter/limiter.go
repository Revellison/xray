// Package limiter implements per-user (per-email) traffic shaping.
//
// Limits come from the "speedLimit" section of the JSON config (see
// infra/conf/speedlimit.go) and are enforced by app/dispatcher, which wraps the
// reader/writer of every connection belonging to a limited user. A user gets
// one token bucket per direction, shared by all of their concurrent
// connections, so a limit caps the sum of that user's traffic rather than each
// connection separately.
//
// Traffic is never dropped and connections are never closed to enforce a limit:
// writes and reads are only delayed, which reaches the peer as ordinary
// transport-level backpressure.
package limiter

import (
	"context"
	"math"
	"sync"
	"sync/atomic"

	"golang.org/x/time/rate"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

// Global is the process-wide limiter, loaded once at config build time.
//
// The dispatcher reads it directly instead of receiving a limiter through the
// core's feature machinery: WrapLink is a package-level function with no access
// to a dispatcher instance, and leaving the core's DI untouched keeps this patch
// small and easy to rebase onto upstream releases.
var Global = New()

// Speed is a pair of limits in bytes per second. Zero or negative means
// "unlimited" for that direction.
type Speed struct {
	Up   int64 // client -> server -> internet
	Down int64 // internet -> server -> client
}

// IsLimited reports whether at least one direction is limited.
func (s Speed) IsLimited() bool {
	return s.Up > 0 || s.Down > 0
}

type direction int

const (
	directionUp direction = iota
	directionDown
)

// Limiter holds the configured limits and the token bucket of every user seen
// so far. It is safe for concurrent use.
type Limiter struct {
	mu      sync.RWMutex
	up      map[string]*rate.Limiter // email -> upload bucket
	down    map[string]*rate.Limiter // email -> download bucket
	config  map[string]Speed         // email -> configured limit
	def     Speed                    // limit for users not listed in config
	enabled atomic.Bool
}

func New() *Limiter {
	return &Limiter{
		up:     make(map[string]*rate.Limiter),
		down:   make(map[string]*rate.Limiter),
		config: make(map[string]Speed),
	}
}

// LoadConfig replaces the current limits. Values are in bytes per second.
func (l *Limiter) LoadConfig(userLimits map[string]Speed, def Speed) {
	limits := make(map[string]Speed, len(userLimits))
	for email, s := range userLimits {
		limits[email] = s
	}

	enabled := def.IsLimited()
	if !enabled {
		for _, s := range limits {
			if s.IsLimited() {
				enabled = true
				break
			}
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.config = limits
	l.def = def
	// Drop the cached buckets: after a reload they would still be enforcing the
	// rates of the previous config.
	l.up = make(map[string]*rate.Limiter)
	l.down = make(map[string]*rate.Limiter)
	l.enabled.Store(enabled)
}

// Enabled reports whether any limit is configured. It lets callers skip all
// per-connection work when the feature is unused.
func (l *Limiter) Enabled() bool {
	return l.enabled.Load()
}

// Limit returns the limits that apply to email.
func (l *Limiter) Limit(email string) Speed {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if s, ok := l.config[email]; ok {
		return s
	}
	return l.def
}

// storeLocked returns the bucket cache of dir. Callers must hold l.mu.
func (l *Limiter) storeLocked(dir direction) map[string]*rate.Limiter {
	if dir == directionDown {
		return l.down
	}
	return l.up
}

// bucket returns the token bucket of email for dir, creating it on first use.
// It returns nil when bps is not a limit, meaning "let the traffic through".
func (l *Limiter) bucket(dir direction, email string, bps int64) *rate.Limiter {
	if bps <= 0 {
		return nil
	}

	l.mu.RLock()
	bucket, ok := l.storeLocked(dir)[email]
	l.mu.RUnlock()
	if ok {
		return bucket
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	store := l.storeLocked(dir)
	if bucket, ok := store[email]; ok { // another connection won the race
		return bucket
	}
	// A burst of one second of traffic keeps interactive bursts responsive
	// without letting the average rate exceed the limit. Clamped because Burst
	// is an int and 32-bit builds are common on routers.
	burst := bps
	if burst > math.MaxInt {
		burst = math.MaxInt
	}
	bucket = rate.NewLimiter(rate.Limit(bps), int(burst))
	store[email] = bucket
	return bucket
}

// WrapUplinkWriter throttles a writer carrying traffic from the client towards
// the internet. It returns w untouched when the user has no upload limit.
func (l *Limiter) WrapUplinkWriter(ctx context.Context, email string, w buf.Writer) buf.Writer {
	if bucket := l.bucket(directionUp, email, l.Limit(email).Up); bucket != nil {
		return &RateWriter{writer: w, bucket: bucket, ctx: ctx}
	}
	return w
}

// WrapDownlinkWriter throttles a writer carrying traffic from the internet
// towards the client. It returns w untouched when the user has no download
// limit.
func (l *Limiter) WrapDownlinkWriter(ctx context.Context, email string, w buf.Writer) buf.Writer {
	if bucket := l.bucket(directionDown, email, l.Limit(email).Down); bucket != nil {
		return &RateWriter{writer: w, bucket: bucket, ctx: ctx}
	}
	return w
}

// WrapUplinkReader throttles a reader that pulls traffic from the client. It is
// needed because on the DispatchLink path the uplink reaches the dispatcher as
// a reader rather than a writer. It returns r untouched when the user has no
// upload limit.
func (l *Limiter) WrapUplinkReader(ctx context.Context, email string, r buf.Reader) buf.Reader {
	if bucket := l.bucket(directionUp, email, l.Limit(email).Up); bucket != nil {
		return &RateReader{reader: r, bucket: bucket, ctx: ctx}
	}
	return r
}

// RateWriter delays writes so the traffic passing through it stays within the
// rate of its bucket. Data is held back, never dropped.
type RateWriter struct {
	writer buf.Writer
	bucket *rate.Limiter
	ctx    context.Context
}

func (w *RateWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	if n := mb.Len(); n > 0 {
		if err := wait(w.ctx, w.bucket, int64(n)); err != nil {
			// Nothing was handed downstream, so releasing the buffers is on us,
			// same contract as a failed write in transport/pipe.
			buf.ReleaseMulti(mb)
			return err
		}
	}
	return w.writer.WriteMultiBuffer(mb)
}

func (w *RateWriter) Close() error {
	return common.Close(w.writer)
}

func (w *RateWriter) Interrupt() {
	common.Interrupt(w.writer)
}

// RateReader holds data back after reading it so the traffic passing through
// stays within the rate of its bucket. Stalling the reader is what applies
// backpressure to the sender.
type RateReader struct {
	reader buf.Reader
	bucket *rate.Limiter
	ctx    context.Context
}

func (r *RateReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	mb, err := r.reader.ReadMultiBuffer()
	if n := mb.Len(); n > 0 {
		if werr := wait(r.ctx, r.bucket, int64(n)); werr != nil {
			buf.ReleaseMulti(mb)
			return nil, werr
		}
	}
	return mb, err
}

func (r *RateReader) Close() error {
	return common.Close(r.reader)
}

func (r *RateReader) Interrupt() {
	common.Interrupt(r.reader)
}

// wait blocks until n bytes worth of tokens have been taken from bucket.
//
// The request is split into chunks no larger than the burst because WaitN fails
// outright once n exceeds it, and a single MultiBuffer is easily larger than one
// second of a low limit. Without the split, every large write on a slow user
// would fail instead of being delayed.
func wait(ctx context.Context, bucket *rate.Limiter, n int64) error {
	burst := int64(bucket.Burst())
	for n > 0 {
		chunk := n
		if chunk > burst {
			chunk = burst
		}
		if err := bucket.WaitN(ctx, int(chunk)); err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}
