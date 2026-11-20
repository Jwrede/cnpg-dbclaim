package controller

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Jwrede/cnpg-dbclaim/internal/policy"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

var ctx = context.Background()

func obj(apiVersion, kind, ns, name string, spec map[string]interface{}) *unstructured.Unstructured {
	o := &unstructured.Unstructured{Object: map[string]interface{}{}}
	o.SetAPIVersion(apiVersion)
	o.SetKind(kind)
	o.SetNamespace(ns)
	o.SetName(name)
	if spec != nil {
		o.Object["spec"] = spec
	}
	return o
}

func namespace(name string, labels map[string]string) *unstructured.Unstructured {
	o := obj("v1", "Namespace", "", name, nil)
	o.SetLabels(labels)
	return o
}

func cluster(ns, name string) *unstructured.Unstructured {
	return obj("postgresql.cnpg.io/v1", "Cluster", ns, name, map[string]interface{}{"instances": int64(1)})
}

func grant(ns, name string, spec map[string]interface{}) *unstructured.Unstructured {
	return obj(Group+"/"+Version, "DatabaseGrant", ns, name, spec)
}

var seq int64

func claim(ns, name string, spec map[string]interface{}) *unstructured.Unstructured {
	o := obj(Group+"/"+Version, "DatabaseClaim", ns, name, spec)
	seq++
	o.SetUID(types.UID("uid-" + ns + "-" + name))
	o.SetCreationTimestamp(metav1.NewTime(time.Unix(1000+seq, 0)))
	return o
}

type env struct {
	t  *testing.T
	c  *Controller
	fk *dynamicfake.FakeDynamicClient
}

func newEnv(t *testing.T, objs ...runtime.Object) *env {
	fk := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), ListKinds, objs...)
	return &env{t: t, c: &Controller{Client: fk}, fk: fk}
}

func baseObjects(extra ...runtime.Object) []runtime.Object {
	objs := []runtime.Object{
		namespace("db", nil), namespace("team-a", nil), namespace("team-b", map[string]string{"tenant": "b"}), namespace("team-c", nil),
		cluster("db", "shared"),
		grant("db", "teams", map[string]interface{}{
			"cluster":            "shared",
			"from":               []interface{}{map[string]interface{}{"namespace": "team-a"}, map[string]interface{}{"namespaceSelector": map[string]interface{}{"matchLabels": map[string]interface{}{"tenant": "b"}}}},
			"allowedExtensions":  []interface{}{"pg_trgm"},
			"maxDatabases":       int64(2),
			"maxConnectionLimit": int64(20),
		}),
	}
	return append(objs, extra...)
}

func (e *env) pass() {
	e.t.Helper()
	if err := e.c.Reconcile(ctx); err != nil {
		e.t.Fatalf("reconcile: %v", err)
	}
}

func (e *env) get(gvr schema.GroupVersionResource, ns, name string) *unstructured.Unstructured {
	e.t.Helper()
	o, err := e.fk.Resource(gvr).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil
	}
	return o
}

func (e *env) list(gvr schema.GroupVersionResource, ns string) []unstructured.Unstructured {
	l, err := e.fk.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		e.t.Fatal(err)
	}
	return l.Items
}

// cnpg plays the CloudNativePG operator: it marks every Database and
// DatabaseRole as applied.
func (e *env) cnpg() {
	for _, gvr := range []schema.GroupVersionResource{DatabaseGVR, RoleGVR} {
		for _, o := range e.list(gvr, "") {
			o := o
			unstructured.SetNestedField(o.Object, true, "status", "applied")
			if _, err := e.fk.Resource(gvr).Namespace(o.GetNamespace()).UpdateStatus(ctx, &o, metav1.UpdateOptions{}); err != nil {
				e.t.Fatal(err)
			}
		}
	}
}

func (e *env) phase(ns, name string) (string, string) {
	o := e.get(ClaimGVR, ns, name)
	conds, _, _ := unstructured.NestedSlice(o.Object, "status", "conditions")
	msg := ""
	if len(conds) == 1 {
		msg, _ = conds[0].(map[string]interface{})["message"].(string)
	}
	return str(o, "status", "phase"), msg
}

func (e *env) wantPhase(ns, name, want, msgPart string) {
	e.t.Helper()
	ph, msg := e.phase(ns, name)
	if ph != want || !strings.Contains(msg, msgPart) {
		e.t.Fatalf("claim %s/%s: phase %q message %q, want %q containing %q", ns, name, ph, msg, want, msgPart)
	}
}

func TestReadyPath(t *testing.T) {
	e := newEnv(t, baseObjects(claim("team-a", "orders", map[string]interface{}{
		"cluster":    map[string]interface{}{"namespace": "db", "name": "shared"},
		"extensions": []interface{}{"pg_trgm"},
	}))...)
	e.pass()
	e.wantPhase("team-a", "orders", PhasePending, "not applied yet")
	name := policy.ObjectName("team-a", "orders")
	db := e.get(DatabaseGVR, "db", name)
	role := e.get(RoleGVR, "db", name)
	pw := e.get(SecretGVR, "db", name+"-password")
	if db == nil || role == nil || pw == nil {
		t.Fatalf("objects missing: db=%v role=%v secret=%v", db != nil, role != nil, pw != nil)
	}
	if str(db, "spec", "name") != "team_a_orders" || str(db, "spec", "owner") != "team_a_orders" || str(db, "spec", "databaseReclaimPolicy") != "retain" {
		t.Fatalf("database spec %v", db.Object["spec"])
	}
	if l, _ := integer(db, "spec", "connectionLimit"); l != 20 {
		t.Fatalf("connection limit %d", l)
	}
	exts, _, _ := unstructured.NestedSlice(db.Object, "spec", "extensions")
	if len(exts) != 1 || exts[0].(map[string]interface{})["name"] != "pg_trgm" {
		t.Fatalf("extensions %v", exts)
	}
	for _, f := range []string{"superuser", "createdb", "createrole", "replication", "bypassrls"} {
		if v, ok, _ := unstructured.NestedBool(role.Object, "spec", f); !ok || v {
			t.Fatalf("role %s = %v (set %v)", f, v, ok)
		}
	}
	if !boolean(role, "spec", "login") || str(role, "spec", "passwordSecret", "name") != name+"-password" {
		t.Fatalf("role spec %v", role.Object["spec"])
	}
	if pw.GetLabels()["cnpg.io/reload"] != "true" || pw.Object["type"] != "kubernetes.io/basic-auth" {
		t.Fatalf("password secret %v", pw.Object)
	}
	if !hasFinalizer(e.get(ClaimGVR, "team-a", "orders")) {
		t.Fatal("finalizer not added")
	}
	if e.get(SecretGVR, "team-a", "orders-db") != nil {
		t.Fatal("app secret written before CNPG applied the objects")
	}

	e.cnpg()
	e.pass()
	e.wantPhase("team-a", "orders", PhaseReady, "team_a_orders")
	app := secretData(e.get(SecretGVR, "team-a", "orders-db"))
	password := secretData(pw)["password"]
	if len(password) != 32 || app["password"] != password || app["username"] != "team_a_orders" || app["dbname"] != "team_a_orders" {
		t.Fatalf("app secret %v", app)
	}
	if app["host"] != "shared-rw.db.svc" || !strings.HasPrefix(app["uri"], "postgresql://team_a_orders:"+password+"@shared-rw.db.svc:5432/team_a_orders") {
		t.Fatalf("connection data %v", app)
	}
	// A further pass changes nothing.
	rv := e.get(DatabaseGVR, "db", name).GetResourceVersion()
	e.pass()
	if e.get(DatabaseGVR, "db", name).GetResourceVersion() != rv {
		t.Fatal("idle pass updated the Database")
	}
	var buf bytes.Buffer
	e.c.Metrics(&buf)
	if !strings.Contains(buf.String(), `cnpg_dbclaim_claims{phase="Ready"} 1`) {
		t.Fatalf("metrics:\n%s", buf.String())
	}
}

func TestNotGranted(t *testing.T) {
	ref := map[string]interface{}{"namespace": "db", "name": "shared"}
	e := newEnv(t, baseObjects(
		claim("team-c", "x", map[string]interface{}{"cluster": ref}),
		claim("team-a", "steal", map[string]interface{}{"cluster": ref, "database": "team_b_main"}),
		claim("team-a", "drop", map[string]interface{}{"cluster": ref, "reclaimPolicy": "delete"}),
		claim("team-b", "gis", map[string]interface{}{"cluster": ref, "extensions": []interface{}{"postgis"}}),
		claim("team-a", "nocluster", map[string]interface{}{"cluster": map[string]interface{}{"namespace": "db", "name": "missing"}}),
	)...)
	e.pass()
	e.wantPhase("team-c", "x", PhaseNotGranted, "does not include namespace team-c")
	e.wantPhase("team-a", "steal", PhaseNotGranted, `not "team_b_main"`)
	e.wantPhase("team-a", "drop", PhaseNotGranted, "reclaimPolicy delete")
	e.wantPhase("team-b", "gis", PhaseNotGranted, `extension "postgis"`)
	e.wantPhase("team-a", "nocluster", PhasePending, "not found")
	if n := len(e.list(DatabaseGVR, "db")); n != 0 {
		t.Fatalf("%d Database objects created for refused claims", n)
	}
	if n := len(e.list(RoleGVR, "db")); n != 0 {
		t.Fatalf("%d DatabaseRole objects created for refused claims", n)
	}
}

func TestConflicts(t *testing.T) {
	ref := map[string]interface{}{"namespace": "db", "name": "shared"}
	unmanaged := obj("postgresql.cnpg.io/v1", "Database", "db", "legacy", map[string]interface{}{
		"cluster": map[string]interface{}{"name": "shared"}, "name": "team_a_legacy", "owner": "app"})
	e := newEnv(t, baseObjects(unmanaged,
		claim("team-a", "legacy", map[string]interface{}{"cluster": ref}),
		claim("team-a", "first", map[string]interface{}{"cluster": ref, "database": "team_a_shop"}),
		claim("team-a", "second", map[string]interface{}{"cluster": ref, "database": "team_a_shop"}),
	)...)
	other := obj("v1", "Secret", "team-a", "first-db", nil)
	if _, err := e.fk.Resource(SecretGVR).Namespace("team-a").Create(ctx, other, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	e.pass()
	e.wantPhase("team-a", "legacy", PhaseConflict, "already used by Database legacy")
	e.wantPhase("team-a", "first", PhaseConflict, "is not managed by cnpg-dbclaim")
	e.wantPhase("team-a", "second", PhasePending, "not applied yet")
	if err := e.fk.Resource(SecretGVR).Namespace("team-a").Delete(ctx, "first-db", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	// Now "first" is older and admitted, but "second" already holds the
	// name through its Database object, so "first" stays in conflict.
	e.pass()
	e.wantPhase("team-a", "first", PhaseConflict, "already used by team-a/second")
	e.wantPhase("team-a", "second", PhasePending, "not applied yet")
}

func TestReservedAppDatabase(t *testing.T) {
	g := grant("db", "open", map[string]interface{}{"cluster": "shared", "namePattern": "*",
		"from": []interface{}{map[string]interface{}{"namespace": "team-a"}}})
	e := newEnv(t, namespace("db", nil), namespace("team-a", nil), cluster("db", "shared"), g,
		claim("team-a", "x", map[string]interface{}{"cluster": map[string]interface{}{"namespace": "db", "name": "shared"}, "database": "app", "owner": "team_a_x"}))
	e.pass()
	e.wantPhase("team-a", "x", PhaseConflict, "already used by cluster shared")
}

func TestMaxDatabases(t *testing.T) {
	ref := map[string]interface{}{"namespace": "db", "name": "shared"}
	e := newEnv(t, baseObjects(
		claim("team-a", "one", map[string]interface{}{"cluster": ref}),
		claim("team-a", "two", map[string]interface{}{"cluster": ref}),
		claim("team-a", "three", map[string]interface{}{"cluster": ref}),
		claim("team-b", "one", map[string]interface{}{"cluster": ref}),
	)...)
	e.pass()
	e.wantPhase("team-a", "one", PhasePending, "")
	e.wantPhase("team-a", "two", PhasePending, "")
	e.wantPhase("team-a", "three", PhaseNotGranted, "allows 2 database(s) for namespace team-a")
	e.wantPhase("team-b", "one", PhasePending, "")
}

func TestDeletionOrder(t *testing.T) {
	ref := map[string]interface{}{"namespace": "db", "name": "shared"}
	e := newEnv(t, baseObjects(claim("team-a", "orders", map[string]interface{}{"cluster": ref}))...)
	e.pass()
	e.cnpg()
	e.pass()
	e.wantPhase("team-a", "orders", PhaseReady, "")
	name := policy.ObjectName("team-a", "orders")

	o := e.get(ClaimGVR, "team-a", "orders")
	now := metav1.Now()
	o.SetDeletionTimestamp(&now)
	if _, err := e.fk.Resource(ClaimGVR).Namespace("team-a").Update(ctx, o, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	e.pass()
	if e.get(DatabaseGVR, "db", name) != nil || e.get(RoleGVR, "db", name) == nil {
		t.Fatal("first pass must delete the Database and keep the role")
	}
	e.pass()
	if e.get(RoleGVR, "db", name) != nil || e.get(SecretGVR, "db", name+"-password") == nil {
		t.Fatal("second pass must delete the role")
	}
	e.pass()
	if e.get(SecretGVR, "db", name+"-password") != nil {
		t.Fatal("password secret left behind")
	}
	if hasFinalizer(e.get(ClaimGVR, "team-a", "orders")) {
		t.Fatal("finalizer not removed")
	}
}

func TestRotationAndRevocation(t *testing.T) {
	ref := map[string]interface{}{"namespace": "db", "name": "shared"}
	e := newEnv(t, baseObjects(claim("team-a", "orders", map[string]interface{}{"cluster": ref}))...)
	e.pass()
	e.cnpg()
	e.pass()
	before := secretData(e.get(SecretGVR, "team-a", "orders-db"))["password"]

	o := e.get(ClaimGVR, "team-a", "orders")
	o.SetAnnotations(map[string]string{AnnRotate: "2026-11-20"})
	if _, err := e.fk.Resource(ClaimGVR).Namespace("team-a").Update(ctx, o, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	e.pass()
	after := secretData(e.get(SecretGVR, "team-a", "orders-db"))["password"]
	stored := secretData(e.get(SecretGVR, "db", policy.ObjectName("team-a", "orders")+"-password"))["password"]
	if after == before || after != stored {
		t.Fatalf("rotation: before %q after %q stored %q", before, after, stored)
	}
	e.pass()
	if again := secretData(e.get(SecretGVR, "team-a", "orders-db"))["password"]; again != after {
		t.Fatal("password rotated again without a new annotation value")
	}

	if err := e.fk.Resource(GrantGVR).Namespace("db").Delete(ctx, "teams", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	e.pass()
	e.wantPhase("team-a", "orders", PhaseNotGranted, "existing database and role are kept")
	if e.get(DatabaseGVR, "db", policy.ObjectName("team-a", "orders")) == nil {
		t.Fatal("revoking the grant deleted the database")
	}
}

func TestOrphanCleanup(t *testing.T) {
	name := policy.ObjectName("team-z", "gone")
	labels := map[string]string{LabelManagedBy: ManagedBy, LabelClaimNS: "team-z", LabelClaimName: "gone"}
	db := obj("postgresql.cnpg.io/v1", "Database", "db", name, map[string]interface{}{"cluster": map[string]interface{}{"name": "shared"}, "name": "team_z_gone", "owner": "team_z_gone"})
	db.SetLabels(labels)
	role := obj("postgresql.cnpg.io/v1", "DatabaseRole", "db", name, map[string]interface{}{"cluster": map[string]interface{}{"name": "shared"}, "name": "team_z_gone"})
	role.SetLabels(labels)
	e := newEnv(t, baseObjects(db, role)...)
	e.pass()
	if e.get(DatabaseGVR, "db", name) != nil {
		t.Fatal("orphaned Database not deleted")
	}
	e.pass()
	if e.get(RoleGVR, "db", name) != nil {
		t.Fatal("orphaned DatabaseRole not deleted")
	}
}

func TestClusterNamespacesFlag(t *testing.T) {
	e := newEnv(t, baseObjects(claim("team-a", "orders", map[string]interface{}{"cluster": map[string]interface{}{"namespace": "db", "name": "shared"}}))...)
	e.c.ClusterNamespaces = []string{"other"}
	e.pass()
	e.wantPhase("team-a", "orders", PhaseInvalid, "does not manage cluster namespace db")
}
