package pool

import (
	"sync"
	"sync/atomic"
	"time"

	"easy_proxies/internal/monitor"
)

type failureClass string

const (
	failureIgnored      failureClass = "ignored"
	failureDialTimeout  failureClass = "dial_timeout"
	failureDial         failureClass = "dial"
	failureRealityReset failureClass = "reality_reset"
	failureTargetClosed failureClass = "target_closed"
	failureProtocol     failureClass = "protocol"
	failureUnknown      failureClass = "unknown"
)

type failureEvent struct {
	at     time.Time
	weight int
	class  failureClass
}

type failureResult struct {
	class       failureClass
	events      int
	score       int
	triggered   bool
	halfOpen    bool
	nextRetryAt time.Time
}

// sharedMemberState holds health state shared across all pool instances.
// This enables hybrid mode where pool and multi-port modes share the same node state.
type sharedMemberState struct {
	mu sync.Mutex

	// failures is retained as a compatibility/debug counter and mirrors the
	// current weighted score inside the sliding window.
	failures         int
	failureEvents    []failureEvent
	blacklisted      bool
	blacklistedUntil time.Time
	halfOpen         bool
	backoffLevel     int
	latencySamples   []time.Duration

	entry         atomic.Pointer[monitor.EntryHandle]
	active        atomic.Int32
	totalUpload   atomic.Int64
	totalDownload atomic.Int64
}

var sharedStateStore sync.Map // map[tag]*sharedMemberState

func acquireSharedState(tag string) *sharedMemberState {
	if v, ok := sharedStateStore.Load(tag); ok {
		return v.(*sharedMemberState)
	}
	state := &sharedMemberState{}
	actual, _ := sharedStateStore.LoadOrStore(tag, state)
	return actual.(*sharedMemberState)
}

func lookupSharedState(tag string) (*sharedMemberState, bool) {
	v, ok := sharedStateStore.Load(tag)
	if !ok {
		return nil, false
	}
	return v.(*sharedMemberState), true
}

func ResetSharedStateStore() {
	sharedStateStore.Range(func(key, _ any) bool {
		sharedStateStore.Delete(key)
		return true
	})
}

func ActiveConnections() int32 {
	var total int32
	sharedStateStore.Range(func(_, value any) bool {
		total += value.(*sharedMemberState).activeCount()
		return true
	})
	return total
}

func (s *sharedMemberState) attachEntry(entry *monitor.EntryHandle) {
	if entry != nil {
		s.entry.Store(entry)
	}
}

func (s *sharedMemberState) entryHandle() *monitor.EntryHandle { return s.entry.Load() }

func failureWeight(class failureClass) int {
	switch class {
	case failureDialTimeout, failureDial, failureProtocol:
		return 2
	case failureRealityReset, failureUnknown:
		return 1
	default:
		// Target-side closes are observable, but must not reduce node health.
		return 0
	}
}

func (s *sharedMemberState) pruneFailuresLocked(now time.Time, window time.Duration) {
	cutoff := now.Add(-window)
	first := 0
	for first < len(s.failureEvents) && s.failureEvents[first].at.Before(cutoff) {
		first++
	}
	if first > 0 {
		copy(s.failureEvents, s.failureEvents[first:])
		s.failureEvents = s.failureEvents[:len(s.failureEvents)-first]
	}
	s.failures = 0
	for _, event := range s.failureEvents {
		s.failures += event.weight
	}
}

func (s *sharedMemberState) cooldownLocked(options Options) time.Duration {
	level := s.backoffLevel
	if level > 30 {
		level = 30
	}
	delay := options.BackoffBase
	for i := 0; i < level && delay < options.BackoffMax; i++ {
		if delay > options.BackoffMax/2 {
			delay = options.BackoffMax
			break
		}
		delay *= 2
	}
	if delay < options.HalfOpenInterval {
		delay = options.HalfOpenInterval
	}
	if delay > options.BackoffMax {
		delay = options.BackoffMax
	}
	return delay
}

func (s *sharedMemberState) recordFailure(cause error, class failureClass, options Options, destination string) failureResult {
	now := time.Now()
	weight := failureWeight(class)
	result := failureResult{class: class}

	s.mu.Lock()
	wasHalfOpen := s.halfOpen
	if class == failureIgnored {
		if wasHalfOpen {
			s.halfOpen = false
			s.blacklistedUntil = now.Add(options.HalfOpenInterval)
		}
		result.halfOpen = wasHalfOpen
		result.nextRetryAt = s.blacklistedUntil
		s.mu.Unlock()
		return result
	}

	s.pruneFailuresLocked(now, options.FailureWindow)
	if weight > 0 {
		s.failureEvents = append(s.failureEvents, failureEvent{at: now, weight: weight, class: class})
		s.failures += weight
	}
	result.events = len(s.failureEvents)
	result.score = s.failures

	// A failed half-open/manual probe immediately returns to cooling down.
	// Healthy nodes enter cooldown only when both event count and weighted
	// score cross their sliding-window thresholds.
	thresholdScore := options.FailureThreshold
	shouldCoolDown := wasHalfOpen || (weight > 0 && result.events >= options.MinimumFailures && result.score >= thresholdScore)
	if shouldCoolDown {
		s.halfOpen = false
		s.blacklisted = true
		if wasHalfOpen || !s.blacklistedUntil.IsZero() {
			s.backoffLevel++
		}
		delay := s.cooldownLocked(options)
		s.blacklistedUntil = now.Add(delay)
		result.triggered = true
		result.halfOpen = wasHalfOpen
		result.nextRetryAt = s.blacklistedUntil
	}
	s.mu.Unlock()

	if entry := s.entry.Load(); entry != nil {
		entry.RecordFailure(cause, destination)
		if result.triggered {
			entry.Blacklist(result.nextRetryAt)
		}
	}
	return result
}

func (s *sharedMemberState) recordSuccess(destination string) {
	s.mu.Lock()
	recovered := s.blacklisted || s.halfOpen
	if recovered {
		s.failureEvents = s.failureEvents[:0]
		s.failures = 0
		s.blacklisted = false
		s.blacklistedUntil = time.Time{}
		s.halfOpen = false
		s.backoffLevel = 0
	} else if len(s.failureEvents) > 0 {
		// A normal success repays one recent failure point instead of wiping the
		// whole window, preventing success/failure alternation from hiding flaps.
		last := len(s.failureEvents) - 1
		if s.failureEvents[last].weight > 1 {
			s.failureEvents[last].weight--
		} else {
			s.failureEvents = s.failureEvents[:last]
		}
		s.failures--
		if s.failures < 0 {
			s.failures = 0
		}
	}
	s.mu.Unlock()

	if entry := s.entry.Load(); entry != nil {
		if recovered {
			entry.ClearBlacklist()
		}
		entry.RecordSuccess(destination)
	}
}

func (s *sharedMemberState) recordProbeSuccess(duration time.Duration, sampleLimit int) {
	s.mu.Lock()
	recovered := s.blacklisted || s.halfOpen
	s.failureEvents = s.failureEvents[:0]
	s.failures = 0
	s.blacklisted = false
	s.blacklistedUntil = time.Time{}
	s.halfOpen = false
	s.backoffLevel = 0
	if duration > 0 {
		s.latencySamples = append(s.latencySamples, duration)
		if len(s.latencySamples) > sampleLimit {
			copy(s.latencySamples, s.latencySamples[len(s.latencySamples)-sampleLimit:])
			s.latencySamples = s.latencySamples[:sampleLimit]
		}
	}
	s.mu.Unlock()

	if entry := s.entry.Load(); entry != nil {
		if recovered {
			entry.ClearBlacklist()
		}
		entry.RecordSuccessWithLatency(duration)
	}
}

func (s *sharedMemberState) averageLatency() (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.latencySamples) == 0 {
		return 0, false
	}
	var total time.Duration
	for _, sample := range s.latencySamples {
		total += sample
	}
	return total / time.Duration(len(s.latencySamples)), true
}

func (s *sharedMemberState) isBlacklisted(_ time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.blacklisted
}

func (s *sharedMemberState) halfOpenReadyAt() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.blacklistedUntil, s.blacklisted && !s.halfOpen
}

func (s *sharedMemberState) tryAcquireHalfOpen(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.blacklisted || s.halfOpen || now.Before(s.blacklistedUntil) {
		return false
	}
	s.halfOpen = true
	return true
}

func (s *sharedMemberState) forceRelease() {
	s.mu.Lock()
	s.failureEvents = s.failureEvents[:0]
	s.failures = 0
	s.blacklisted = false
	s.blacklistedUntil = time.Time{}
	s.halfOpen = false
	s.backoffLevel = 0
	s.mu.Unlock()

	if entry := s.entry.Load(); entry != nil {
		entry.ClearBlacklist()
	}
}

func (s *sharedMemberState) incActive() {
	s.active.Add(1)
	if entry := s.entry.Load(); entry != nil {
		entry.IncActive()
	}
}

func (s *sharedMemberState) decActive() {
	s.active.Add(-1)
	if entry := s.entry.Load(); entry != nil {
		entry.DecActive()
	}
}

func (s *sharedMemberState) activeCount() int32 { return s.active.Load() }

func (s *sharedMemberState) addTraffic(upload, download int64) {
	if upload > 0 {
		s.totalUpload.Add(upload)
	}
	if download > 0 {
		s.totalDownload.Add(download)
	}
	if entry := s.entry.Load(); entry != nil {
		entry.AddTraffic(upload, download)
	}
}

func releaseSharedMember(tag string) {
	if state, ok := lookupSharedState(tag); ok {
		state.forceRelease()
	}
}
