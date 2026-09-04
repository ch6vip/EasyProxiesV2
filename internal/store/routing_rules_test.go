package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSubscriptionRoutingRuleSnapshots(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "rules.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	first := &Subscription{Name: "first", URL: "https://example.com/first", Enabled: true, SortOrder: 1,
		RefreshIntervalSeconds: 60, RefreshTimeoutSeconds: 10}
	second := &Subscription{Name: "second", URL: "https://example.com/second", Enabled: true, SortOrder: 2,
		RefreshIntervalSeconds: 60, RefreshTimeoutSeconds: 10}
	if err := db.CreateSubscription(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateSubscription(ctx, second); err != nil {
		t.Fatal(err)
	}
	snapshot := SubscriptionSnapshot{Attempt: time.Now().UTC(), Success: time.Now().UTC()}
	if err := db.CommitSnapshotWithRules(ctx, first.ID, nil, []SubscriptionRuleInput{
		{Type: "DOMAIN", Value: "first.example", Action: "direct", Raw: "DOMAIN,first.example,DIRECT"},
	}, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := db.CommitSnapshotWithRules(ctx, second.ID, nil, []SubscriptionRuleInput{
		{Type: "MATCH", Action: "proxy", Raw: "MATCH,Proxy"},
	}, snapshot); err != nil {
		t.Fatal(err)
	}

	automatic, err := db.ListEffectiveSubscriptionRules(ctx, 0)
	if err != nil || len(automatic) != 1 || automatic[0].Value != "first.example" {
		t.Fatalf("automatic rules = %+v, err=%v", automatic, err)
	}
	selected, err := db.ListEffectiveSubscriptionRules(ctx, second.ID)
	if err != nil || len(selected) != 1 || selected[0].Type != "MATCH" {
		t.Fatalf("selected rules = %+v, err=%v", selected, err)
	}
	stored, err := db.GetSubscription(ctx, first.ID)
	if err != nil || stored.RuleCount != 1 {
		t.Fatalf("subscription = %+v, err=%v", stored, err)
	}
	if err := db.SetSubscriptionEnabled(ctx, second.ID, false); err != nil {
		t.Fatal(err)
	}
	selected, err = db.ListEffectiveSubscriptionRules(ctx, second.ID)
	if err != nil || len(selected) != 0 {
		t.Fatalf("disabled selected rules = %+v, err=%v", selected, err)
	}
}
