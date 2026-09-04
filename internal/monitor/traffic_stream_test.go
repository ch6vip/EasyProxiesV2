package monitor

import (
	"testing"
	"time"
)

func TestTrafficSummaryIncludesRealtimeNodeState(t *testing.T) {
	mgr, err := NewManager(Config{})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer mgr.Stop()

	handle := mgr.Register(NodeInfo{Tag: "node-1", Name: "Node 1"})
	handle.MarkInitialCheckDone(true)
	handle.RecordSuccessWithLatency(25 * time.Millisecond)
	handle.IncActive()
	handle.AddTraffic(128, 256)

	summary := mgr.TrafficSummary(true)
	if summary.NodeCount != 1 || len(summary.Nodes) != 1 {
		t.Fatalf("unexpected summary size: %+v", summary)
	}

	node := summary.Nodes[0]
	if node.Tag != "node-1" {
		t.Fatalf("node tag = %q, want node-1", node.Tag)
	}
	if !node.InitialCheckDone || !node.Available {
		t.Fatalf("unexpected health state: %+v", node)
	}
	if node.LastLatencyMs != 25 {
		t.Fatalf("last latency = %dms, want 25ms", node.LastLatencyMs)
	}
	if node.ActiveConnections != 1 {
		t.Fatalf("active connections = %d, want 1", node.ActiveConnections)
	}
	if node.TotalUpload != 128 || node.TotalDownload != 256 {
		t.Fatalf("unexpected traffic totals: %+v", node)
	}
}

func TestTrafficSubscriberKeepsLatestSnapshot(t *testing.T) {
	server := &Server{
		trafficSubscribers: make(map[chan TrafficSummary]struct{}),
	}

	updates, unsubscribe := server.subscribeTraffic()
	defer unsubscribe()

	// Drain the initial cached snapshot, then publish faster than the subscriber
	// reads. The buffered channel should retain only the newest sample.
	<-updates
	server.publishTrafficSnapshot(TrafficSummary{NodeCount: 1})
	server.publishTrafficSnapshot(TrafficSummary{NodeCount: 2})

	select {
	case summary := <-updates:
		if summary.NodeCount != 2 {
			t.Fatalf("node count = %d, want latest value 2", summary.NodeCount)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for traffic snapshot")
	}
}
