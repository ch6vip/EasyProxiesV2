package builder

import (
	"context"
	"testing"

	"easy_proxies/internal/config"

	C "github.com/sagernet/sing-box/constant"
	singrule "github.com/sagernet/sing-box/route/rule"
	"github.com/sagernet/sing/common/logger"
)

func TestBuildTrafficRoutingRules(t *testing.T) {
	rules, err := buildTrafficRoutingRules(config.RoutingConfig{Mode: config.RoutingModeRule, Rules: []config.RoutingRule{
		{Type: "DOMAIN-SUFFIX", Value: "example.com", Action: config.RoutingActionDirect},
		{Type: "IP-CIDR", Value: "1.1.1.1/32", Action: config.RoutingActionReject},
		{Type: "MATCH", Action: config.RoutingActionProxy},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rules) != 3 {
		t.Fatalf("rules = %d, want 3", len(rules))
	}
	if got := rules[0].DefaultOptions; len(got.DomainSuffix) != 1 || got.DomainSuffix[0] != "example.com" || got.RuleAction.Action != C.RuleActionTypeDirect {
		t.Fatalf("unexpected domain rule: %+v", got)
	}
	if got := rules[1].DefaultOptions.RuleAction.Action; got != C.RuleActionTypeReject {
		t.Fatalf("reject action = %q", got)
	}
	if got := rules[1].DefaultOptions.RuleAction.RejectOptions.Method; got != C.RuleActionRejectMethodDefault {
		t.Fatalf("reject method = %q, want %q", got, C.RuleActionRejectMethodDefault)
	}
	rejectAction, err := singrule.NewRuleAction(context.Background(), logger.NOP(), rules[1].DefaultOptions.RuleAction)
	if err != nil {
		t.Fatal(err)
	}
	runtimeReject, ok := rejectAction.(*singrule.RuleActionReject)
	if !ok {
		t.Fatalf("runtime reject action type = %T", rejectAction)
	}
	if rejectErr := runtimeReject.Error(context.Background()); !singrule.IsRejected(rejectErr) {
		t.Fatalf("reject action returned %v, want rejected error", rejectErr)
	}
	if got := rules[2].DefaultOptions.RuleAction.RouteOptions.Outbound; got != "proxy-pool" {
		t.Fatalf("proxy outbound = %q, want proxy-pool", got)
	}
}

func TestBuildTrafficRoutingDirectMode(t *testing.T) {
	rules, err := buildTrafficRoutingRules(config.RoutingConfig{Mode: config.RoutingModeDirect})
	if err != nil {
		t.Fatal(err)
	}
	if len(rules) != 1 || rules[0].DefaultOptions.RuleAction.Action != C.RuleActionTypeDirect {
		t.Fatalf("unexpected direct rules: %+v", rules)
	}
}
