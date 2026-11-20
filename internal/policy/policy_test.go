package policy

import (
	"strings"
	"testing"
	"time"
)

func i64(v int64) *int64 { return &v }

func TestNames(t *testing.T) {
	db, owner, err := Names(Claim{Namespace: "team-a", Name: "orders"})
	if err != nil || db != "team_a_orders" || owner != "team_a_orders" {
		t.Fatalf("got %q %q %v", db, owner, err)
	}
	db, owner, err = Names(Claim{Namespace: "x", Name: "y", Database: "shop", Owner: "shop_owner"})
	if err != nil || db != "shop" || owner != "shop_owner" {
		t.Fatalf("got %q %q %v", db, owner, err)
	}
	for _, c := range []Claim{
		{Namespace: "a", Name: "b", Database: "postgres"},
		{Namespace: "a", Name: "b", Owner: "streaming_replica"},
		{Namespace: "a", Name: "b", Owner: "pg_monitor"},
		{Namespace: "a", Name: "b", Database: "Bad-Name"},
		{Namespace: "a", Name: "b", Extensions: []string{"x;drop"}},
		{Namespace: "a", Name: "b", ReclaimPolicy: "purge"},
		{Namespace: "a", Name: "b", ConnectionLimit: i64(-2)},
		{Namespace: strings.Repeat("n", 40), Name: strings.Repeat("m", 30)},
	} {
		if _, _, err := Names(c); err == nil {
			t.Errorf("expected error for %+v", c)
		}
	}
}

func TestObjectNameDistinct(t *testing.T) {
	// The collision case from the CNPG review: namespace a-b + name c versus
	// namespace a + name b-c must not share an object name.
	if ObjectName("a-b", "c") == ObjectName("a", "b-c") {
		t.Fatal("object names collide")
	}
	if n := ObjectName("team-a", "orders"); !strings.HasPrefix(n, "dbclaim-") || len(n) != 24 {
		t.Fatalf("unexpected name %q", n)
	}
}

func TestAdmits(t *testing.T) {
	g := Grant{Namespace: "db", Name: "g", Cluster: "shared", FromNamespaces: []string{"team-a"},
		AllowedExtensions: []string{"pg_trgm"}, MaxConnectionLimit: 20}
	ok := Claim{Namespace: "team-a", Name: "orders", Cluster: ClusterRef{"db", "shared"}, Extensions: []string{"pg_trgm"}}
	if r := g.Admits(ok, nil); r != "" {
		t.Fatalf("expected admit, got %q", r)
	}
	if ConnectionLimit(ok, g) != 20 {
		t.Fatalf("connection limit should default to the grant cap")
	}
	cases := map[string]Claim{
		"does not include namespace": {Namespace: "team-b", Name: "x", Cluster: ClusterRef{"db", "shared"}},
		"is for cluster":             {Namespace: "team-a", Name: "x", Cluster: ClusterRef{"db", "other"}},
		"allows only names matching": {Namespace: "team-a", Name: "x", Database: "team_b_x", Cluster: ClusterRef{"db", "shared"}},
		"reclaimPolicy delete":       {Namespace: "team-a", Name: "x", ReclaimPolicy: "delete", Cluster: ClusterRef{"db", "shared"}},
		"does not allow extension":   {Namespace: "team-a", Name: "x", Extensions: []string{"postgis"}, Cluster: ClusterRef{"db", "shared"}},
		"caps connectionLimit":       {Namespace: "team-a", Name: "x", ConnectionLimit: i64(-1), Cluster: ClusterRef{"db", "shared"}},
	}
	for want, c := range cases {
		if r := g.Admits(c, nil); !strings.Contains(r, want) {
			t.Errorf("%s: got %q", want, r)
		}
	}
	// The owner is checked against the pattern too.
	c := Claim{Namespace: "team-a", Name: "x", Owner: "admin", Cluster: ClusterRef{"db", "shared"}}
	if r := g.Admits(c, nil); !strings.Contains(r, `not "admin"`) {
		t.Errorf("owner outside pattern admitted: %q", r)
	}
}

func TestSelectorAndPattern(t *testing.T) {
	g := Grant{Namespace: "db", Name: "g", Cluster: "shared", NamespaceSelector: map[string]string{"tenant": "yes"}, NamePattern: "app_{namespace}_*"}
	c := Claim{Namespace: "web-1", Name: "x", Database: "app_web_1_main", Cluster: ClusterRef{"db", "shared"}}
	if r := g.Admits(c, map[string]string{"tenant": "yes"}); r != "" {
		t.Fatalf("expected admit, got %q", r)
	}
	if r := g.Admits(c, map[string]string{"tenant": "no"}); r == "" {
		t.Fatal("selector mismatch admitted")
	}
	// An empty grant admits nobody.
	if (Grant{Namespace: "db", Cluster: "shared"}).CoversNamespace("x", nil) {
		t.Fatal("empty from admitted a namespace")
	}
}

func TestSortClaims(t *testing.T) {
	t0 := time.Unix(100, 0)
	cs := []Claim{{Name: "c", UID: "2", Created: t0}, {Name: "a", UID: "9", Created: t0.Add(-time.Second)}, {Name: "b", UID: "1", Created: t0}}
	SortClaims(cs)
	if cs[0].Name != "a" || cs[1].Name != "b" || cs[2].Name != "c" {
		t.Fatalf("order %v", cs)
	}
}
