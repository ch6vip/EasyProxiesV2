package pool

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	boxlog "github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type halfCloseConn struct {
	net.Conn
	closeWriteCalls atomic.Int32
	closeReadCalls  atomic.Int32
}

type fakeOutbound struct {
	adapter.Outbound
	dial func(context.Context, string, M.Socksaddr) (net.Conn, error)
}

func (o *fakeOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *fakeOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return o.dial(ctx, network, destination)
}

func TestDialContextFallsBackToAnotherMember(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)
	firstErr := errors.New("first unavailable")
	first := &memberState{
		tag:    "first",
		shared: acquireSharedState("first"),
		outbound: &fakeOutbound{dial: func(context.Context, string, M.Socksaddr) (net.Conn, error) {
			return nil, firstErr
		}},
	}
	client, server := net.Pipe()
	defer server.Close()
	second := &memberState{
		tag:    "second",
		shared: acquireSharedState("second"),
		outbound: &fakeOutbound{dial: func(context.Context, string, M.Socksaddr) (net.Conn, error) {
			return client, nil
		}},
	}
	p := &poolOutbound{
		logger:  boxlog.NewNOPFactory().Logger(),
		mode:    modeSequential,
		members: []*memberState{first, second},
		options: Options{FailureThreshold: 3, BlacklistDuration: time.Minute},
	}
	conn, err := p.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("example.com:443"))
	if err != nil {
		t.Fatal(err)
	}
	if first.shared.failures != 2 {
		t.Fatalf("first member failure score = %d, want 2", first.shared.failures)
	}
	if second.shared.activeCount() != 1 {
		t.Fatalf("second member active count = %d, want 1", second.shared.activeCount())
	}
	_ = conn.Close()
}

func (c *halfCloseConn) CloseWrite() error {
	c.closeWriteCalls.Add(1)
	return nil
}

func (c *halfCloseConn) CloseRead() error {
	c.closeReadCalls.Add(1)
	return nil
}

func TestTrackedConnPreservesHalfClose(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	underlying := &halfCloseConn{Conn: left}
	var releases atomic.Int32
	conn := &trackedConn{Conn: underlying, release: func() { releases.Add(1) }}

	if err := conn.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if err := conn.CloseRead(); err != nil {
		t.Fatal(err)
	}
	if underlying.closeWriteCalls.Load() != 1 || underlying.closeReadCalls.Load() != 1 {
		t.Fatal("half-close methods were not forwarded")
	}
	if releases.Load() != 0 {
		t.Fatal("half-close released the active connection")
	}
	_ = conn.Close()
	_ = conn.Close()
	if releases.Load() != 1 {
		t.Fatalf("release called %d times, want 1", releases.Load())
	}
}

type failingConn struct {
	net.Conn
	err error
}

func (c *failingConn) Read([]byte) (int, error)  { return 0, c.err }
func (c *failingConn) Write([]byte) (int, error) { return 0, c.err }

func TestTrackedConnRecordsUnexpectedIOErrorOnce(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int32
	}{
		{name: "connection failure", err: errors.New("reset"), want: 1},
		{name: "EOF", err: io.EOF, want: 0},
		{name: "closed", err: net.ErrClosed, want: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var failures atomic.Int32
			conn := &trackedConn{
				Conn:    &failingConn{err: tc.err},
				release: func() {},
				onError: func(error) { failures.Add(1) },
			}
			_, _ = conn.Read(nil)
			_, _ = conn.Write(nil)
			if failures.Load() != tc.want {
				t.Fatalf("recorded %d failures, want %d", failures.Load(), tc.want)
			}
		})
	}
}

func TestActiveConnections(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)
	first := acquireSharedState("first")
	second := acquireSharedState("second")
	first.incActive()
	first.incActive()
	second.incActive()
	if got := ActiveConnections(); got != 3 {
		t.Fatalf("ActiveConnections() = %d, want 3", got)
	}
	first.decActive()
	second.decActive()
	first.decActive()
}

func testPoolOptions() Options {
	return normalizeOptions(Options{
		FailureThreshold: 8,
		MinimumFailures:  5,
		FailureWindow:    time.Minute,
		HalfOpenInterval: 10 * time.Millisecond,
		BackoffBase:      10 * time.Millisecond,
		BackoffMax:       time.Second,
		LatencyThreshold: 2 * time.Second,
		LatencySamples:   5,
	})
}

func TestSlidingWindowDoesNotBlacklistAfterThreeFailures(t *testing.T) {
	state := &sharedMemberState{}
	options := testPoolOptions()
	for i := 0; i < 3; i++ {
		state.recordFailure(errors.New("dial timeout"), failureDialTimeout, options, "example.com:443")
	}
	if state.isBlacklisted(time.Now()) {
		t.Fatal("three failures must not blacklist a node")
	}
	if state.failures != 6 {
		t.Fatalf("failure score = %d, want 6", state.failures)
	}
}

func TestSlidingWindowBlacklistsOnlyAfterCountAndScoreThresholds(t *testing.T) {
	state := &sharedMemberState{}
	options := testPoolOptions()
	for i := 0; i < 4; i++ {
		result := state.recordFailure(errors.New("dial timeout"), failureDialTimeout, options, "example.com:443")
		if result.triggered {
			t.Fatalf("cooldown triggered after %d events, before minimum failures", i+1)
		}
	}
	result := state.recordFailure(errors.New("dial timeout"), failureDialTimeout, options, "example.com:443")
	if !result.triggered || !state.isBlacklisted(time.Now()) {
		t.Fatalf("fifth severe failure should trigger cooldown: %+v", result)
	}
}
func TestSlidingWindowPrunesExpiredFailures(t *testing.T) {
	state := &sharedMemberState{
		failures: 2,
		failureEvents: []failureEvent{{
			at:     time.Now().Add(-2 * time.Minute),
			weight: 2,
			class:  failureDialTimeout,
		}},
	}
	options := testPoolOptions()
	state.recordFailure(errors.New("dial timeout"), failureDialTimeout, options, "example.com:443")
	if len(state.failureEvents) != 1 || state.failures != 2 {
		t.Fatalf("expired failures were not pruned: events=%d score=%d", len(state.failureEvents), state.failures)
	}
}

func TestHalfOpenUsesSingleProbeAndExponentialBackoff(t *testing.T) {
	state := &sharedMemberState{}
	options := testPoolOptions()
	options.FailureThreshold = 1
	options.MinimumFailures = 1

	first := state.recordFailure(errors.New("dial timeout"), failureDialTimeout, options, "example.com:443")
	if !first.triggered || !state.isBlacklisted(time.Now()) {
		t.Fatal("first threshold breach should enter cooldown")
	}

	state.mu.Lock()
	state.blacklistedUntil = time.Now().Add(-time.Millisecond)
	state.mu.Unlock()
	if !state.tryAcquireHalfOpen(time.Now()) {
		t.Fatal("due node should allow one half-open probe")
	}
	if state.tryAcquireHalfOpen(time.Now()) {
		t.Fatal("concurrent half-open probe should be rejected")
	}

	before := time.Now()
	second := state.recordFailure(errors.New("reset by peer"), failureRealityReset, options, "example.com:443")
	if !second.triggered || state.backoffLevel != 1 {
		t.Fatalf("failed half-open probe did not increase backoff: triggered=%v level=%d", second.triggered, state.backoffLevel)
	}
	if second.nextRetryAt.Sub(before) < 19*time.Millisecond {
		t.Fatalf("second cooldown = %v, want about 20ms or more", second.nextRetryAt.Sub(before))
	}

	state.mu.Lock()
	state.blacklistedUntil = time.Now().Add(-time.Millisecond)
	state.mu.Unlock()
	if !state.tryAcquireHalfOpen(time.Now()) {
		t.Fatal("node should become half-open again after cooldown")
	}
	state.recordSuccess("example.com:443")
	if state.isBlacklisted(time.Now()) || state.halfOpen || state.backoffLevel != 0 {
		t.Fatal("successful half-open probe should restore healthy state")
	}
}

func TestFailureClassesHaveDifferentScores(t *testing.T) {
	state := &sharedMemberState{}
	options := testPoolOptions()

	state.recordFailure(errors.New("connection reset by peer"), failureTargetClosed, options, "target")
	if state.failures != 0 {
		t.Fatalf("target close score = %d, want 0", state.failures)
	}
	state.recordFailure(errors.New("connection reset by peer"), failureRealityReset, options, "target")
	if state.failures != 1 {
		t.Fatalf("reality reset score = %d, want 1", state.failures)
	}
	state.recordFailure(context.DeadlineExceeded, failureDialTimeout, options, "target")
	if state.failures != 3 {
		t.Fatalf("dial timeout cumulative score = %d, want 3", state.failures)
	}

	if got := classifyFailure(errors.New("read: connection reset by peer"), failurePhaseDial); got != failureRealityReset {
		t.Fatalf("dial reset classified as %s", got)
	}
	if got := classifyFailure(errors.New("read: connection reset by peer"), failurePhaseStream); got != failureTargetClosed {
		t.Fatalf("stream reset classified as %s", got)
	}
	if got := classifyFailure(io.EOF, failurePhaseStream); got != failureIgnored {
		t.Fatalf("EOF classified as %s", got)
	}
}

func TestHighLatencyMembersAreBypassed(t *testing.T) {
	fast := &memberState{tag: "fast", shared: &sharedMemberState{latencySamples: []time.Duration{100 * time.Millisecond}}}
	slow := &memberState{tag: "slow", shared: &sharedMemberState{latencySamples: []time.Duration{5 * time.Second}}}
	p := &poolOutbound{options: testPoolOptions()}

	got := p.filterHighLatencyMembers([]*memberState{fast, slow})
	if len(got) != 1 || got[0] != fast {
		t.Fatalf("filtered members = %v, want only fast", got)
	}

	fast.shared.latencySamples = []time.Duration{4 * time.Second}
	slow.shared.latencySamples = []time.Duration{8 * time.Second}
	got = p.filterHighLatencyMembers([]*memberState{fast, slow})
	if len(got) != 1 || got[0] != fast {
		t.Fatalf("all-slow fallback = %v, want fastest tier", got)
	}

	got = p.filterHighLatencyMembers([]*memberState{slow})
	if len(got) != 1 {
		t.Fatal("single-node pool must remain usable even when latency is high")
	}
}

var _ net.Conn = (*halfCloseConn)(nil)
var _ interface{ CloseWrite() error } = (*trackedConn)(nil)
var _ interface{ CloseRead() error } = (*trackedConn)(nil)
