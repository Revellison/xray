package limiter_test

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/limiter"
	"github.com/xtls/xray-core/common/buf"
)

// discardWriter accepts everything immediately, so the elapsed time of a test
// measures the throttling and nothing else.
type discardWriter struct{}

func (discardWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	buf.ReleaseMulti(mb)
	return nil
}

// bufferOf returns a MultiBuffer holding size bytes.
func bufferOf(size int32) buf.MultiBuffer {
	var mb buf.MultiBuffer
	for size > 0 {
		b := buf.New()
		n := size
		if n > b.Cap() {
			n = b.Cap()
		}
		b.Extend(n)
		mb = append(mb, b)
		size -= n
	}
	return mb
}

const kbps = 1000 / 8 // bytes per second in one kilobit per second

func TestWriterThrottles(t *testing.T) {
	l := limiter.New()
	// 800 kbps == 100000 bytes/s. The bucket starts full, so the first 100000
	// bytes pass instantly and the remaining 100000 take about a second.
	l.LoadConfig(map[string]limiter.Speed{"slow": {Up: 800 * kbps, Down: 800 * kbps}}, limiter.Speed{})

	w := l.WrapDownlinkWriter(context.Background(), "slow", discardWriter{})
	if _, ok := w.(*limiter.RateWriter); !ok {
		t.Fatalf("writer was not wrapped: got %T", w)
	}

	start := time.Now()
	for range 10 {
		if err := w.WriteMultiBuffer(bufferOf(20000)); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}
	elapsed := time.Since(start)

	if elapsed < 900*time.Millisecond {
		t.Errorf("200000 bytes at 100000 bytes/s took %v, expected roughly 1s", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Errorf("throttling is far slower than configured: %v", elapsed)
	}
}

// TestWriterAcceptsBufferLargerThanBurst covers the reason wait() chunks its
// request: rate.Limiter.WaitN fails outright when asked for more tokens than the
// burst, which a single MultiBuffer easily exceeds on a low limit.
func TestWriterAcceptsBufferLargerThanBurst(t *testing.T) {
	l := limiter.New()
	// 100 kbps == 12500 bytes/s, so a 40000 byte write is over three bursts.
	l.LoadConfig(map[string]limiter.Speed{"slow": {Down: 100 * kbps}}, limiter.Speed{})
	w := l.WrapDownlinkWriter(context.Background(), "slow", discardWriter{})

	done := make(chan error, 1)
	go func() { done <- w.WriteMultiBuffer(bufferOf(40000)) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write of a buffer larger than the burst failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("write of a buffer larger than the burst did not finish")
	}
}

func TestUnlimitedUsersAreNotWrapped(t *testing.T) {
	l := limiter.New()
	l.LoadConfig(map[string]limiter.Speed{"slow": {Up: 800 * kbps, Down: 800 * kbps}}, limiter.Speed{})

	if l.Limit("other").IsLimited() {
		t.Error("a user absent from the config must be unlimited without a default")
	}
	if w := l.WrapDownlinkWriter(context.Background(), "other", discardWriter{}); w != buf.Writer(discardWriter{}) {
		t.Errorf("writer of an unlimited user was wrapped: got %T", w)
	}
	source := &buf.MultiBufferContainer{}
	if r := l.WrapUplinkReader(context.Background(), "other", source); r != buf.Reader(source) {
		t.Errorf("reader of an unlimited user was wrapped: got %T", r)
	}
}

func TestDefaultAppliesToUnlistedUsers(t *testing.T) {
	l := limiter.New()
	l.LoadConfig(nil, limiter.Speed{Up: 500 * kbps, Down: 1000 * kbps})

	got := l.Limit("nobody")
	if got.Up != 500*kbps || got.Down != 1000*kbps {
		t.Errorf("unlisted user got %+v, expected the default", got)
	}
	if !l.Enabled() {
		t.Error("a default limit must enable the limiter")
	}
}

func TestPerUserLimitOverridesDefault(t *testing.T) {
	l := limiter.New()
	l.LoadConfig(
		map[string]limiter.Speed{"vip": {Up: 0, Down: 0}},
		limiter.Speed{Up: 500 * kbps, Down: 500 * kbps},
	)

	if l.Limit("vip").IsLimited() {
		t.Error("an explicit zero limit must override the default, not fall back to it")
	}
	if !l.Limit("someone-else").IsLimited() {
		t.Error("the default must still apply to other users")
	}
}

func TestDisabledWhenNothingConfigured(t *testing.T) {
	l := limiter.New()
	if l.Enabled() {
		t.Error("a fresh limiter must be disabled")
	}
	l.LoadConfig(map[string]limiter.Speed{"user": {}}, limiter.Speed{})
	if l.Enabled() {
		t.Error("a config with no actual limit must leave the limiter disabled")
	}
}

// TestBucketIsSharedBetweenConnections is the point of keying buckets by email:
// a user's limit must cap the sum of their connections, not each one separately.
func TestBucketIsSharedBetweenConnections(t *testing.T) {
	l := limiter.New()
	l.LoadConfig(map[string]limiter.Speed{"slow": {Down: 800 * kbps}}, limiter.Speed{})

	first := l.WrapDownlinkWriter(context.Background(), "slow", discardWriter{})
	second := l.WrapDownlinkWriter(context.Background(), "slow", discardWriter{})

	// Drain the initial burst through one connection, then time the other.
	if err := first.WriteMultiBuffer(bufferOf(100000)); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	start := time.Now()
	if err := second.WriteMultiBuffer(bufferOf(50000)); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
		t.Errorf("second connection wrote 50000 bytes in %v, so it is not sharing the bucket", elapsed)
	}
}

func TestWaitFailsOnCanceledContext(t *testing.T) {
	l := limiter.New()
	l.LoadConfig(map[string]limiter.Speed{"slow": {Down: 8 * kbps}}, limiter.Speed{})

	ctx, cancel := context.WithCancel(context.Background())
	w := l.WrapDownlinkWriter(ctx, "slow", discardWriter{})
	// Empty the bucket so the next write has to wait.
	if err := w.WriteMultiBuffer(bufferOf(1000)); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	cancel()

	if err := w.WriteMultiBuffer(bufferOf(1000)); err == nil {
		t.Error("write on a canceled context must fail instead of blocking")
	}
}

func TestReaderThrottles(t *testing.T) {
	l := limiter.New()
	l.LoadConfig(map[string]limiter.Speed{"slow": {Up: 800 * kbps}}, limiter.Speed{})

	source := &buf.MultiBufferContainer{}
	for range 10 {
		if err := source.WriteMultiBuffer(bufferOf(20000)); err != nil {
			t.Fatalf("failed to fill the source: %v", err)
		}
	}

	r := l.WrapUplinkReader(context.Background(), "slow", source)
	if _, ok := r.(*limiter.RateReader); !ok {
		t.Fatalf("reader was not wrapped: got %T", r)
	}

	start := time.Now()
	var total int32
	for total < 200000 {
		mb, err := r.ReadMultiBuffer()
		if err != nil {
			t.Fatalf("read failed after %d bytes: %v", total, err)
		}
		if mb.IsEmpty() {
			t.Fatalf("source drained after %d bytes, expected 200000", total)
		}
		total += mb.Len()
		buf.ReleaseMulti(mb)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Errorf("read 200000 bytes at 100000 bytes/s in %v, expected roughly 1s", elapsed)
	}
}
