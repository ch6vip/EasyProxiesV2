package pool

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"easy_proxies/internal/monitor"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

const (
	// Type is the outbound type name exposed to sing-box.
	Type = "pool"
	// Tag is the default outbound tag used by builder.
	Tag = "proxy-pool"

	modeSequential = "sequential"
	modeRandom     = "random"
	modeBalance    = "balance"
)

// Options controls pool outbound behaviour.
type Options struct {
	Mode              string
	Members           []string
	FailureThreshold  int
	MinimumFailures   int
	FailureWindow     time.Duration
	BlacklistDuration time.Duration // legacy compatibility; BackoffMax controls the actual cap
	HalfOpenInterval  time.Duration
	BackoffBase       time.Duration
	BackoffMax        time.Duration
	LatencyThreshold  time.Duration
	LatencySamples    int
	Metadata          map[string]MemberMeta
}

// MemberMeta carries optional descriptive information for monitoring UI.
type MemberMeta struct {
	Name          string
	URI           string
	Mode          string
	ListenAddress string
	Port          uint16
	Region        string // GeoIP region code: "jp", "kr", "us", "hk", "tw", "other"
	Country       string // Full country name from GeoIP
}

// Register wires the pool outbound into the registry.
func Register(registry *outbound.Registry) {
	outbound.Register[Options](registry, Type, newPool)
}

type memberState struct {
	outbound adapter.Outbound
	tag      string
	entry    *monitor.EntryHandle
	shared   *sharedMemberState
}

type poolOutbound struct {
	outbound.Adapter
	ctx            context.Context
	logger         log.ContextLogger
	manager        adapter.OutboundManager
	options        Options
	mode           string
	members        []*memberState
	mu             sync.Mutex
	rrCounter      atomic.Uint32
	rng            *rand.Rand
	rngMu          sync.Mutex // protects rng for random mode
	monitor        *monitor.Manager
	candidatesPool sync.Pool
}

func newPool(ctx context.Context, _ adapter.Router, logger log.ContextLogger, tag string, options Options) (adapter.Outbound, error) {
	if len(options.Members) == 0 {
		return nil, E.New("pool requires at least one member")
	}
	manager := service.FromContext[adapter.OutboundManager](ctx)
	if manager == nil {
		return nil, E.New("missing outbound manager in context")
	}
	monitorMgr := monitor.FromContext(ctx)
	normalized := normalizeOptions(options)
	memberCount := len(normalized.Members)
	p := &poolOutbound{
		Adapter: outbound.NewAdapter(Type, tag, []string{N.NetworkTCP, N.NetworkUDP}, normalized.Members),
		ctx:     ctx,
		logger:  logger,
		manager: manager,
		options: normalized,
		mode:    normalized.Mode,
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
		monitor: monitorMgr,
		candidatesPool: sync.Pool{
			New: func() any {
				return make([]*memberState, 0, memberCount)
			},
		},
	}

	// Register nodes immediately if monitor is available
	if monitorMgr != nil {
		logger.Info("registering ", len(normalized.Members), " nodes to monitor")
		for _, memberTag := range normalized.Members {
			// Acquire shared state for this tag (creates if not exists)
			state := acquireSharedState(memberTag)

			meta := normalized.Metadata[memberTag]
			info := monitor.NodeInfo{
				Tag:           memberTag,
				Name:          meta.Name,
				URI:           meta.URI,
				Mode:          meta.Mode,
				ListenAddress: meta.ListenAddress,
				Port:          meta.Port,
				Region:        meta.Region,
				Country:       meta.Country,
			}
			entry := monitorMgr.Register(info)
			if entry != nil {
				// Attach entry to shared state so all pool instances share it
				state.attachEntry(entry)
				logger.Info("registered node: ", memberTag)
				// Set probe and release functions immediately
				entry.SetRelease(p.makeReleaseByTagFunc(memberTag))
				if probeFn := p.makeProbeByTagFunc(memberTag); probeFn != nil {
					entry.SetProbe(probeFn)
				}
			} else {
				logger.Warn("failed to register node: ", memberTag)
			}
		}
	} else {
		logger.Warn("monitor manager is nil, skipping node registration")
	}

	return p, nil
}

func normalizeOptions(options Options) Options {
	if options.FailureThreshold <= 0 {
		options.FailureThreshold = 8
	}
	if options.MinimumFailures <= 0 {
		options.MinimumFailures = 5
	}
	if options.FailureWindow <= 0 {
		options.FailureWindow = time.Minute
	}
	if options.HalfOpenInterval <= 0 {
		options.HalfOpenInterval = 15 * time.Second
	}
	if options.BackoffBase <= 0 {
		options.BackoffBase = 5 * time.Second
	}
	if options.BackoffMax <= 0 {
		options.BackoffMax = options.BlacklistDuration
		if options.BackoffMax <= 0 || options.BackoffMax > 5*time.Minute {
			options.BackoffMax = 5 * time.Minute
		}
	}
	if options.BackoffMax < options.BackoffBase {
		options.BackoffMax = options.BackoffBase
	}
	if options.BackoffMax < options.HalfOpenInterval {
		options.BackoffMax = options.HalfOpenInterval
	}
	if options.BlacklistDuration <= 0 {
		options.BlacklistDuration = options.BackoffMax
	}
	if options.LatencyThreshold <= 0 {
		options.LatencyThreshold = 2 * time.Second
	}
	if options.LatencySamples <= 0 {
		options.LatencySamples = 5
	}
	if options.Metadata == nil {
		options.Metadata = make(map[string]MemberMeta)
	}
	switch strings.ToLower(options.Mode) {
	case modeRandom:
		options.Mode = modeRandom
	case modeBalance:
		options.Mode = modeBalance
	default:
		options.Mode = modeSequential
	}
	return options
}

type failurePhase uint8

const (
	failurePhaseDial failurePhase = iota
	failurePhaseStream
)

func classifyFailure(err error, phase failurePhase) failureClass {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
		return failureIgnored
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return failureDialTimeout
	}
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return failureDialTimeout
	}

	message := strings.ToLower(err.Error())
	if strings.Contains(message, "context canceled") || strings.Contains(message, "use of closed network connection") {
		return failureIgnored
	}
	if strings.Contains(message, "timeout") || strings.Contains(message, "i/o timeout") {
		return failureDialTimeout
	}
	if strings.Contains(message, "connection reset") || strings.Contains(message, "reset by peer") {
		if phase == failurePhaseDial {
			return failureRealityReset
		}
		return failureTargetClosed
	}
	if strings.Contains(message, "broken pipe") || strings.Contains(message, "connection aborted") {
		return failureTargetClosed
	}
	if strings.Contains(message, "reality") || strings.Contains(message, "tls:") || strings.Contains(message, "handshake") || strings.Contains(message, "crypto_error") || strings.Contains(message, "xtls") {
		return failureProtocol
	}
	if phase == failurePhaseDial {
		return failureDial
	}
	return failureUnknown
}
func (p *poolOutbound) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	p.mu.Lock()
	err := p.initializeMembersLocked()
	p.mu.Unlock()
	if err != nil {
		return err
	}
	// 在初始化完成后，立即在后台触发健康检查
	if p.monitor != nil {
		go p.probeAllMembersOnStartup()
	}
	return nil
}

// initializeMembersLocked must be called with p.mu held
func (p *poolOutbound) initializeMembersLocked() error {
	if len(p.members) > 0 {
		return nil // Already initialized
	}

	members := make([]*memberState, 0, len(p.options.Members))
	for _, tag := range p.options.Members {
		detour, loaded := p.manager.Outbound(tag)
		if !loaded {
			return E.New("pool member not found: ", tag)
		}

		// Acquire shared state (creates if not exists, reuses if already created)
		state := acquireSharedState(tag)

		member := &memberState{
			outbound: detour,
			tag:      tag,
			shared:   state,
			entry:    state.entryHandle(),
		}

		// Connect to existing monitor entry if available
		if p.monitor != nil {
			meta := p.options.Metadata[tag]
			info := monitor.NodeInfo{
				Tag:           tag,
				Name:          meta.Name,
				URI:           meta.URI,
				Mode:          meta.Mode,
				ListenAddress: meta.ListenAddress,
				Port:          meta.Port,
				Region:        meta.Region,
				Country:       meta.Country,
			}
			entry := p.monitor.Register(info)
			if entry != nil {
				state.attachEntry(entry)
				member.entry = entry
				entry.SetRelease(p.makeReleaseFunc(member))
				if probe := p.makeProbeFunc(member); probe != nil {
					entry.SetProbe(probe)
				}
			}
		}
		members = append(members, member)
	}
	p.members = members
	p.logger.Info("pool initialized with ", len(members), " members")

	return nil
}

// probeAllMembersOnStartup performs initial health checks on all members
func (p *poolOutbound) probeAllMembersOnStartup() {
	destination, ok := p.monitor.DestinationForProbe()
	if !ok {
		p.logger.Warn("probe target not configured, skipping initial health check")
		// 没有配置探测目标时，标记所有节点为可用
		p.mu.Lock()
		for _, member := range p.members {
			if member.entry != nil {
				member.entry.MarkInitialCheckDone(true)
			}
		}
		p.mu.Unlock()
		return
	}

	p.logger.Info("starting initial health check for all nodes")

	p.mu.Lock()
	members := make([]*memberState, len(p.members))
	copy(members, p.members)
	p.mu.Unlock()

	availableCount := 0
	failedCount := 0

	for _, member := range members {
		// Create a timeout context for each probe
		ctx, cancel := context.WithTimeout(p.ctx, 15*time.Second)

		start := time.Now()
		conn, err := member.outbound.DialContext(ctx, N.NetworkTCP, destination)

		probeDst := destination.String()
		if err != nil {
			p.logger.Warn("initial probe failed for ", member.tag, ": ", err)
			failedCount++
			p.recordFailure(member, err, failurePhaseDial, probeDst)
			if member.entry != nil {
				member.entry.MarkInitialCheckDone(false) // 标记为不可用
			}
			cancel()
			continue
		}

		// Perform HTTP probe to measure actual latency (TTFB)
		_, err = httpProbe(conn, destination.AddrString())
		conn.Close()

		if err != nil {
			p.logger.Warn("initial HTTP probe failed for ", member.tag, ": ", err)
			failedCount++
			p.recordFailure(member, err, failurePhaseDial, probeDst)
			if member.entry != nil {
				member.entry.MarkInitialCheckDone(false)
			}
			cancel()
			continue
		}

		// Total latency = dial + HTTP probe
		latency := time.Since(start)
		latencyMs := latency.Milliseconds()
		p.logger.Info("initial probe success for ", member.tag, ", latency: ", latencyMs, "ms")
		availableCount++
		p.recordProbeSuccess(member, latency)
		if member.entry != nil {
			member.entry.MarkInitialCheckDone(true)
		}

		cancel()
	}

	p.logger.Info("initial health check completed: ", availableCount, " available, ", failedCount, " failed")
}

func (p *poolOutbound) memberName(member *memberState) string {
	if meta, ok := p.options.Metadata[member.tag]; ok && meta.Name != "" {
		return meta.Name
	}
	return member.tag
}

func (p *poolOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	dst := destination.String()
	attempted := make(map[*memberState]struct{})
	var dialErr error
	for {
		member, err := p.pickMemberExcluding(network, attempted)
		if err != nil {
			if dialErr != nil {
				return nil, fmt.Errorf("all available proxies failed for %s: %w", dst, dialErr)
			}
			return nil, err
		}
		attempted[member] = struct{}{}
		p.logger.Info("→ ", dst, " ⇒ ", p.memberName(member), " [", network, "]")
		p.incActive(member)
		conn, err := member.outbound.DialContext(ctx, network, destination)
		if err != nil {
			p.decActive(member)
			p.recordFailure(member, err, failurePhaseDial, dst)
			dialErr = err
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			continue
		}
		p.recordSuccess(member, dst)
		return p.wrapConn(conn, member, dst), nil
	}
}

func (p *poolOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	dst := destination.String()
	attempted := make(map[*memberState]struct{})
	var listenErr error
	for {
		member, err := p.pickMemberExcluding(N.NetworkUDP, attempted)
		if err != nil {
			if listenErr != nil {
				return nil, fmt.Errorf("all available proxies failed for %s: %w", dst, listenErr)
			}
			return nil, err
		}
		attempted[member] = struct{}{}
		p.logger.Info("→ ", dst, " ⇒ ", p.memberName(member), " [udp]")
		p.incActive(member)
		conn, err := member.outbound.ListenPacket(ctx, destination)
		if err != nil {
			p.decActive(member)
			p.recordFailure(member, err, failurePhaseDial, dst)
			listenErr = err
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			continue
		}
		p.recordSuccess(member, dst)
		return p.wrapPacketConn(conn, member, dst), nil
	}
}

func (p *poolOutbound) pickMember(network string) (*memberState, error) {
	return p.pickMemberExcluding(network, nil)
}

func (p *poolOutbound) pickMemberExcluding(network string, excluded map[*memberState]struct{}) (*memberState, error) {
	now := time.Now()
	candidates := p.getCandidateBuffer()

	p.mu.Lock()
	if len(p.members) == 0 {
		if err := p.initializeMembersLocked(); err != nil {
			p.mu.Unlock()
			p.putCandidateBuffer(candidates)
			return nil, err
		}
	}

	// Due cooling nodes are admitted one at a time. Reserving the node before
	// normal selection prevents concurrent requests from stampeding it.
	if member := p.acquireHalfOpenMemberLocked(now, network, excluded); member != nil {
		p.mu.Unlock()
		p.putCandidateBuffer(candidates)
		p.logger.Info("proxy ", member.tag, " entered half-open probe")
		return member, nil
	}

	candidates = p.availableMembersLocked(now, network, candidates)
	candidates = excludeMembers(candidates, excluded)
	candidates = p.filterHighLatencyMembers(candidates)
	p.mu.Unlock()

	if len(candidates) == 0 {
		p.putCandidateBuffer(candidates)
		return nil, E.New("no healthy proxy available (cooling nodes will retry in half-open mode)")
	}

	member := p.selectMember(candidates)
	p.putCandidateBuffer(candidates)
	return member, nil
}

func (p *poolOutbound) acquireHalfOpenMemberLocked(now time.Time, network string, excluded map[*memberState]struct{}) *memberState {
	var selected *memberState
	var earliest time.Time
	for _, member := range p.members {
		if member.shared == nil {
			continue
		}
		if _, skip := excluded[member]; skip {
			continue
		}
		if network != "" && !common.Contains(member.outbound.Network(), network) {
			continue
		}
		readyAt, cooling := member.shared.halfOpenReadyAt()
		if !cooling || now.Before(readyAt) {
			continue
		}
		if selected == nil || readyAt.Before(earliest) {
			selected = member
			earliest = readyAt
		}
	}
	if selected != nil && selected.shared.tryAcquireHalfOpen(now) {
		return selected
	}
	return nil
}
func excludeMembers(candidates []*memberState, excluded map[*memberState]struct{}) []*memberState {
	if len(excluded) == 0 {
		return candidates
	}
	result := candidates[:0]
	for _, member := range candidates {
		if _, ok := excluded[member]; !ok {
			result = append(result, member)
		}
	}
	return result
}

func (p *poolOutbound) availableMembersLocked(now time.Time, network string, buf []*memberState) []*memberState {
	result := buf[:0]
	for _, member := range p.members {
		// Check blacklist via shared state (auto-clears if expired)
		if member.shared != nil && member.shared.isBlacklisted(now) {
			continue
		}
		if network != "" && !common.Contains(member.outbound.Network(), network) {
			continue
		}
		result = append(result, member)
	}
	return result
}

func (p *poolOutbound) filterHighLatencyMembers(candidates []*memberState) []*memberState {
	if len(candidates) <= 1 || p.options.LatencyThreshold <= 0 {
		return candidates
	}

	var minLatency time.Duration
	hasUnknown := false
	hasFast := false
	for _, member := range candidates {
		if member.shared == nil {
			hasUnknown = true
			continue
		}
		latency, ok := member.shared.averageLatency()
		if !ok {
			hasUnknown = true
			continue
		}
		if minLatency == 0 || latency < minLatency {
			minLatency = latency
		}
		if latency <= p.options.LatencyThreshold {
			hasFast = true
		}
	}
	if minLatency == 0 {
		return candidates
	}

	cutoff := p.options.LatencyThreshold
	if !hasFast && !hasUnknown {
		// If every measured node is slow, retain the fastest tier rather than
		// making the pool unavailable.
		cutoff = minLatency + minLatency/4
	}
	result := candidates[:0]
	for _, member := range candidates {
		if member.shared == nil {
			result = append(result, member)
			continue
		}
		latency, ok := member.shared.averageLatency()
		if !ok || latency <= cutoff {
			result = append(result, member)
		}
	}
	if len(result) == 0 {
		return candidates
	}
	return result
}
func (p *poolOutbound) selectMember(candidates []*memberState) *memberState {
	switch p.mode {
	case modeRandom:
		p.rngMu.Lock()
		idx := p.rng.Intn(len(candidates))
		p.rngMu.Unlock()
		return candidates[idx]
	case modeBalance:
		var selected *memberState
		var minActive int32
		for _, member := range candidates {
			var active int32
			if member.shared != nil {
				active = member.shared.activeCount()
			}
			if selected == nil || active < minActive {
				selected = member
				minActive = active
			}
		}
		return selected
	default:
		idx := int(p.rrCounter.Add(1)-1) % len(candidates)
		return candidates[idx]
	}
}

func (p *poolOutbound) recordFailure(member *memberState, cause error, phase failurePhase, destination string) {
	class := classifyFailure(cause, phase)
	if member.shared == nil {
		p.logger.Warn("proxy ", member.tag, " failure (no shared state, class=", class, "): ", cause)
		return
	}
	result := member.shared.recordFailure(cause, class, p.options, destination)
	if class == failureIgnored {
		return
	}
	if result.triggered {
		p.logger.Warn("proxy ", member.tag, " cooling down until ", result.nextRetryAt.Format(time.RFC3339), " (class=", class, ", events=", result.events, ", score=", result.score, "): ", cause)
	} else {
		p.logger.Warn("proxy ", member.tag, " failure class=", class, " events=", result.events, " score=", result.score, "/", p.options.FailureThreshold, ": ", cause)
	}
}

func (p *poolOutbound) recordSuccess(member *memberState, destination string) {
	if member.shared != nil {
		member.shared.recordSuccess(destination)
	}
}

func (p *poolOutbound) recordProbeSuccess(member *memberState, duration time.Duration) {
	if member.shared != nil {
		member.shared.recordProbeSuccess(duration, p.options.LatencySamples)
		return
	}
	if member.entry != nil {
		member.entry.RecordSuccessWithLatency(duration)
	}
}

func (p *poolOutbound) wrapConn(conn net.Conn, member *memberState, destination string) net.Conn {
	return &trackedConn{
		Conn: conn,
		release: func() {
			p.decActive(member)
		},
		onTraffic: func(upload, download int64) {
			if member.shared != nil {
				member.shared.addTraffic(upload, download)
			}
		},
		onError: func(err error) {
			p.recordFailure(member, err, failurePhaseStream, destination)
		},
	}
}

func (p *poolOutbound) wrapPacketConn(conn net.PacketConn, member *memberState, destination string) net.PacketConn {
	return &trackedPacketConn{
		PacketConn: conn,
		release: func() {
			p.decActive(member)
		},
		onTraffic: func(upload, download int64) {
			if member.shared != nil {
				member.shared.addTraffic(upload, download)
			}
		},
		onError: func(err error) {
			p.recordFailure(member, err, failurePhaseStream, destination)
		},
	}
}

func (p *poolOutbound) makeReleaseFunc(member *memberState) func() {
	return func() {
		if member.shared != nil {
			member.shared.forceRelease()
		}
	}
}

// httpProbe performs an HTTP probe through the connection and measures TTFB.
// It sends a minimal HTTP request and waits for the first byte of response.
func httpProbe(conn net.Conn, host string) (time.Duration, error) {
	// Build HTTP request
	req := fmt.Sprintf("GET /generate_204 HTTP/1.1\r\nHost: %s\r\nConnection: close\r\nUser-Agent: Mozilla/5.0\r\n\r\n", host)

	// Try to set write deadline (ignore errors for connections that don't support it)
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	// Record time just before sending request
	start := time.Now()

	// Send HTTP request
	if _, err := conn.Write([]byte(req)); err != nil {
		return 0, fmt.Errorf("write request: %w", err)
	}

	// Try to set read deadline (ignore errors for connections that don't support it)
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	// Read first byte (TTFB - Time To First Byte)
	reader := bufio.NewReader(conn)
	_, err := reader.ReadByte()
	if err != nil {
		return 0, fmt.Errorf("read response: %w", err)
	}

	// Calculate TTFB
	ttfb := time.Since(start)
	return ttfb, nil
}

func (p *poolOutbound) makeProbeFunc(member *memberState) func(ctx context.Context) (time.Duration, error) {
	if p.monitor == nil {
		return nil
	}
	// 仅在创建时检查是否有探测目标，实际目标在执行时动态获取
	if _, ok := p.monitor.DestinationForProbe(); !ok {
		return nil
	}
	return func(ctx context.Context) (time.Duration, error) {
		// 每次执行时动态获取最新的探测目标
		destination, ok := p.monitor.DestinationForProbe()
		if !ok {
			return 0, E.New("probe target not configured")
		}

		start := time.Now()
		probeDst := destination.String()
		conn, err := member.outbound.DialContext(ctx, N.NetworkTCP, destination)
		if err != nil {
			p.recordFailure(member, err, failurePhaseDial, probeDst)
			return 0, err
		}
		defer conn.Close()

		// Perform HTTP probe to measure actual latency (TTFB)
		_, err = httpProbe(conn, destination.AddrString())
		if err != nil {
			p.recordFailure(member, err, failurePhaseDial, probeDst)
			return 0, err
		}

		// Total duration = dial time + HTTP probe
		duration := time.Since(start)
		p.recordProbeSuccess(member, duration)
		return duration, nil
	}
}

// makeProbeByTagFunc creates a probe function that works before member initialization
func (p *poolOutbound) makeProbeByTagFunc(tag string) func(ctx context.Context) (time.Duration, error) {
	if p.monitor == nil {
		return nil
	}
	// 仅在创建时检查是否有探测目标，实际目标在执行时动态获取
	if _, ok := p.monitor.DestinationForProbe(); !ok {
		return nil
	}
	return func(ctx context.Context) (time.Duration, error) {
		// 每次执行时动态获取最新的探测目标
		destination, ok := p.monitor.DestinationForProbe()
		if !ok {
			return 0, E.New("probe target not configured")
		}

		// Ensure members are initialized
		p.mu.Lock()
		if len(p.members) == 0 {
			if err := p.initializeMembersLocked(); err != nil {
				p.mu.Unlock()
				return 0, err
			}
		}

		// Find the member by tag
		var member *memberState
		for _, m := range p.members {
			if m.tag == tag {
				member = m
				break
			}
		}
		p.mu.Unlock()

		if member == nil {
			return 0, E.New("member not found: ", tag)
		}

		start := time.Now()
		probeDst := destination.String()
		conn, err := member.outbound.DialContext(ctx, N.NetworkTCP, destination)
		if err != nil {
			p.recordFailure(member, err, failurePhaseDial, probeDst)
			return 0, err
		}
		defer conn.Close()

		// Perform HTTP probe to measure actual latency (TTFB)
		_, err = httpProbe(conn, destination.AddrString())
		if err != nil {
			p.recordFailure(member, err, failurePhaseDial, probeDst)
			return 0, err
		}

		// Total duration = dial time + TTFB
		duration := time.Since(start)
		p.recordProbeSuccess(member, duration)
		return duration, nil
	}
}

// makeReleaseByTagFunc creates a release function that works before member initialization
func (p *poolOutbound) makeReleaseByTagFunc(tag string) func() {
	return func() {
		releaseSharedMember(tag)
	}
}

type trackedConn struct {
	net.Conn
	once      sync.Once
	release   func()
	onTraffic func(upload, download int64)
	onError   func(error)
	errorOnce sync.Once
}

func (c *trackedConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 && c.onTraffic != nil {
		c.onTraffic(0, int64(n))
	}
	c.recordIOError(err)
	return n, err
}

func (c *trackedConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if n > 0 && c.onTraffic != nil {
		c.onTraffic(int64(n), 0)
	}
	c.recordIOError(err)
	return n, err
}

func (c *trackedConn) recordIOError(err error) {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
		return
	}
	c.errorOnce.Do(func() {
		if c.onError != nil {
			c.onError(err)
		}
	})
}

func (c *trackedConn) CloseWrite() error {
	conn, ok := c.Conn.(interface{ CloseWrite() error })
	if !ok {
		return E.New("underlying connection does not support CloseWrite")
	}
	return conn.CloseWrite()
}

func (c *trackedConn) CloseRead() error {
	conn, ok := c.Conn.(interface{ CloseRead() error })
	if !ok {
		return E.New("underlying connection does not support CloseRead")
	}
	return conn.CloseRead()
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

type trackedPacketConn struct {
	net.PacketConn
	once      sync.Once
	release   func()
	onTraffic func(upload, download int64)
	onError   func(error)
	errorOnce sync.Once
}

func (c *trackedPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(b)
	if n > 0 && c.onTraffic != nil {
		c.onTraffic(0, int64(n))
	}
	c.recordIOError(err)
	return n, addr, err
}

func (c *trackedPacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	n, err := c.PacketConn.WriteTo(b, addr)
	if n > 0 && c.onTraffic != nil {
		c.onTraffic(int64(n), 0)
	}
	c.recordIOError(err)
	return n, err
}

func (c *trackedPacketConn) recordIOError(err error) {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
		return
	}
	c.errorOnce.Do(func() {
		if c.onError != nil {
			c.onError(err)
		}
	})
}

func (c *trackedPacketConn) Close() error {
	err := c.PacketConn.Close()
	c.once.Do(c.release)
	return err
}

func (p *poolOutbound) incActive(member *memberState) {
	if member.shared != nil {
		member.shared.incActive()
	}
}

func (p *poolOutbound) decActive(member *memberState) {
	if member.shared != nil {
		member.shared.decActive()
	}
}

func (p *poolOutbound) getCandidateBuffer() []*memberState {
	if buf := p.candidatesPool.Get(); buf != nil {
		return buf.([]*memberState)
	}
	return make([]*memberState, 0, len(p.options.Members))
}

func (p *poolOutbound) putCandidateBuffer(buf []*memberState) {
	if buf == nil {
		return
	}
	const maxCached = 4096
	if cap(buf) > maxCached {
		return
	}
	p.candidatesPool.Put(buf[:0])
}
