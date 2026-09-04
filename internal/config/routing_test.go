package config

import "testing"

func TestParseSubscriptionBundleExtractsClashRules(t *testing.T) {
	content := `
proxies:
  - name: demo
    type: vless
    server: example.com
    port: 443
    uuid: 11111111-1111-1111-1111-111111111111
    tls: true
rules:
  - DOMAIN,example.org,DIRECT
  - DOMAIN-SUFFIX,google.com,Proxy
  - IP-CIDR,1.1.1.1/32,Proxy,no-resolve
  - REJECT-UNKNOWN,ignored,REJECT
  - MATCH,Proxy
`
	bundle, err := ParseSubscriptionBundle(content)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Nodes) != 1 {
		t.Fatalf("nodes = %d, want 1", len(bundle.Nodes))
	}
	if len(bundle.Rules) != 4 {
		t.Fatalf("rules = %d, want 4: %+v", len(bundle.Rules), bundle.Rules)
	}
	if got := bundle.Rules[0]; got.Type != "DOMAIN" || got.Value != "example.org" || got.Action != RoutingActionDirect {
		t.Fatalf("unexpected direct rule: %+v", got)
	}
	if got := bundle.Rules[2]; !got.NoResolve || got.Action != RoutingActionProxy {
		t.Fatalf("unexpected CIDR rule: %+v", got)
	}
	if got := bundle.Rules[3]; got.Type != "MATCH" || got.Action != RoutingActionProxy {
		t.Fatalf("unexpected final rule: %+v", got)
	}
}

func TestParseClashRuleNormalizesActions(t *testing.T) {
	tests := []struct {
		raw, action string
	}{
		{"DOMAIN,ads.example,REJECT", RoutingActionReject},
		{"DOMAIN,intranet.example,DIRECT", RoutingActionDirect},
		{"DOMAIN,overseas.example,provider-group", RoutingActionProxy},
		{"FINAL,DIRECT", RoutingActionDirect},
	}
	for _, test := range tests {
		rule, ok := ParseClashRule(test.raw)
		if !ok || rule.Action != test.action {
			t.Fatalf("ParseClashRule(%q) = %+v, %v; want action %q", test.raw, rule, ok, test.action)
		}
	}
}
