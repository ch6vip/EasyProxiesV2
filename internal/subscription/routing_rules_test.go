package subscription

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/store"
)

func TestRefreshExtractsRulesFromClashProfile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.UserAgent() != "Clash.Meta" {
			http.Error(w, "Clash client required", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`
proxies:
  - name: demo
    type: vless
    server: example.com
    port: 443
    uuid: 11111111-1111-1111-1111-111111111111
    tls: true
rules:
  - DOMAIN-SUFFIX,example.org,DIRECT
  - MATCH,Proxy
`))
	}))
	defer server.Close()

	db, err := store.Open(filepath.Join(t.TempDir(), "rules.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := &config.Config{}
	cfg.SubscriptionRefresh.Timeout = time.Second
	mgr := New(cfg, nil, WithStore(db), WithHTTPClient(server.Client()))
	defer mgr.Stop()

	sub, err := mgr.Create(context.Background(), store.Subscription{Name: "rules", URL: server.URL, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.RefreshOne(context.Background(), sub.ID); err != nil {
		t.Fatal(err)
	}
	stored, err := mgr.Get(context.Background(), sub.ID)
	if err != nil || stored.NodeCount != 1 || stored.RuleCount != 2 {
		t.Fatalf("subscription = %+v, err=%v", stored, err)
	}
	rules, err := db.ListSubscriptionRules(context.Background(), sub.ID)
	if err != nil || len(rules) != 2 || rules[0].Action != config.RoutingActionDirect {
		t.Fatalf("rules = %+v, err=%v", rules, err)
	}
}
