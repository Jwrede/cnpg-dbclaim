// Package controller reconciles DatabaseClaims into CloudNativePG Database
// and DatabaseRole objects in the cluster's namespace and delivers the
// credentials as a Secret in the claim's namespace.
package controller

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Jwrede/cnpg-dbclaim/internal/policy"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// Phases written to DatabaseClaim status.phase.
const (
	PhaseReady      = "Ready"
	PhasePending    = "Pending"
	PhaseNotGranted = "NotGranted"
	PhaseConflict   = "Conflict"
	PhaseInvalid    = "Invalid"
	PhaseDeleting   = "Deleting"
)

// Controller holds the client and the counters exported on /metrics.
type Controller struct {
	Client dynamic.Interface
	// ClusterNamespaces limits the namespaces in which grants, Clusters and
	// the created CNPG objects are read and written. Empty means all.
	ClusterNamespaces []string
	Log               *log.Logger
	Rand              io.Reader

	mu      sync.Mutex
	phases  map[string]int
	errors  int64
	passes  int64
	lastDur time.Duration
}

type clusterNS struct {
	clusters map[string]*unstructured.Unstructured
	dbs      map[string]*unstructured.Unstructured // by object name
	roles    map[string]*unstructured.Unstructured
	secrets  map[string]*unstructured.Unstructured // managed password secrets
	grants   []policy.Grant
}

// pass is the state of one reconcile run.
type pass struct {
	c        *Controller
	ctx      context.Context
	nsLabels map[string]map[string]string
	cns      map[string]*clusterNS
	// taken maps "clusterNS/cluster" -> "db:"+name or "role:"+name -> holder
	taken  map[string]map[string]string
	counts map[string]int
	phases map[string]int
	errs   []error
}

func (c *Controller) logf(format string, a ...interface{}) {
	if c.Log != nil {
		c.Log.Printf(format, a...)
	}
}

func (c *Controller) watched(ns string) bool {
	if len(c.ClusterNamespaces) == 0 {
		return true
	}
	for _, n := range c.ClusterNamespaces {
		if n == ns {
			return true
		}
	}
	return false
}

func (c *Controller) list(ctx context.Context, gvr schema.GroupVersionResource, ns, selector string) ([]unstructured.Unstructured, error) {
	l, err := c.Client.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("list %s in %q: %w", gvr.Resource, ns, err)
	}
	return l.Items, nil
}

// Reconcile runs one full pass over all claims. Errors on single claims
// are logged and counted; an error is returned only when the pass could not
// read the state it needs.
func (c *Controller) Reconcile(ctx context.Context) error {
	start := time.Now()
	p := &pass{c: c, ctx: ctx, nsLabels: map[string]map[string]string{}, cns: map[string]*clusterNS{},
		taken: map[string]map[string]string{}, counts: map[string]int{}, phases: map[string]int{}}
	err := p.run()
	c.mu.Lock()
	c.passes++
	c.lastDur = time.Since(start)
	if err != nil {
		c.errors++
	} else {
		c.errors += int64(len(p.errs))
		c.phases = p.phases
	}
	c.mu.Unlock()
	for _, e := range p.errs {
		c.logf("error: %v", e)
	}
	return err
}

func (p *pass) run() error {
	ctx, c := p.ctx, p.c
	claimItems, err := c.list(ctx, ClaimGVR, "", "")
	if err != nil {
		return err
	}
	nsItems, err := c.list(ctx, NamespaceGVR, "", "")
	if err != nil {
		return err
	}
	for i := range nsItems {
		p.nsLabels[nsItems[i].GetName()] = nsItems[i].GetLabels()
	}
	// Cluster namespaces to load: those referenced by claims, plus those
	// holding managed objects (for orphan cleanup).
	need := map[string]bool{}
	for i := range claimItems {
		cl := claimFrom(&claimItems[i])
		if c.watched(cl.Cluster.Namespace) {
			need[cl.Cluster.Namespace] = true
		}
	}
	if len(c.ClusterNamespaces) > 0 {
		for _, n := range c.ClusterNamespaces {
			need[n] = true
		}
	} else {
		for _, gvr := range []schema.GroupVersionResource{DatabaseGVR, RoleGVR} {
			items, err := c.list(ctx, gvr, "", LabelManagedBy+"="+ManagedBy)
			if err != nil {
				return err
			}
			for i := range items {
				need[items[i].GetNamespace()] = true
			}
		}
	}
	for ns := range need {
		if err := p.load(ns); err != nil {
			return err
		}
	}

	live := map[string]*unstructured.Unstructured{}
	var claims []policy.Claim
	objs := map[string]*unstructured.Unstructured{}
	for i := range claimItems {
		o := &claimItems[i]
		key := o.GetNamespace() + "/" + o.GetName()
		live[key] = o
		objs[key] = o
		claims = append(claims, claimFrom(o))
	}
	policy.SortClaims(claims)

	for _, cl := range claims {
		o := objs[cl.Namespace+"/"+cl.Name]
		if o.GetDeletionTimestamp() != nil {
			p.phases[PhaseDeleting]++
			if hasFinalizer(o) {
				p.deleteClaim(o, cl)
			}
			continue
		}
	}
	for _, cl := range claims {
		o := objs[cl.Namespace+"/"+cl.Name]
		if o.GetDeletionTimestamp() != nil {
			continue
		}
		phase := p.claim(o, cl)
		p.phases[phase]++
	}
	p.orphans(live)
	return nil
}

func (p *pass) load(ns string) error {
	c := p.c
	s := &clusterNS{clusters: map[string]*unstructured.Unstructured{}, dbs: map[string]*unstructured.Unstructured{},
		roles: map[string]*unstructured.Unstructured{}, secrets: map[string]*unstructured.Unstructured{}}
	items, err := c.list(p.ctx, ClusterGVR, ns, "")
	if err != nil {
		return err
	}
	for i := range items {
		cl := &items[i]
		s.clusters[cl.GetName()] = cl
		// The cluster's own application database and owner stay reserved.
		db := str(cl, "spec", "bootstrap", "initdb", "database")
		owner := str(cl, "spec", "bootstrap", "initdb", "owner")
		if db == "" {
			db = "app"
		}
		if owner == "" {
			owner = db
		}
		p.take(ns, cl.GetName(), "db:"+db, "cluster "+cl.GetName())
		p.take(ns, cl.GetName(), "role:"+owner, "cluster "+cl.GetName())
	}
	for _, x := range []struct {
		gvr  schema.GroupVersionResource
		into map[string]*unstructured.Unstructured
		kind string
	}{{DatabaseGVR, s.dbs, "db:"}, {RoleGVR, s.roles, "role:"}} {
		items, err := c.list(p.ctx, x.gvr, ns, "")
		if err != nil {
			return err
		}
		for i := range items {
			o := &items[i]
			holder := "Database " + o.GetName()
			if x.kind == "role:" {
				holder = "DatabaseRole " + o.GetName()
			}
			if o.GetLabels()[LabelManagedBy] == ManagedBy {
				x.into[o.GetName()] = o
				holder = o.GetLabels()[LabelClaimNS] + "/" + o.GetLabels()[LabelClaimName]
			}
			p.take(ns, str(o, "spec", "cluster", "name"), x.kind+str(o, "spec", "name"), holder)
		}
	}
	items, err = c.list(p.ctx, SecretGVR, ns, LabelManagedBy+"="+ManagedBy)
	if err != nil {
		return err
	}
	for i := range items {
		s.secrets[items[i].GetName()] = &items[i]
	}
	items, err = c.list(p.ctx, GrantGVR, ns, "")
	if err != nil {
		return err
	}
	for i := range items {
		s.grants = append(s.grants, grantFrom(&items[i]))
	}
	policy.SortGrants(s.grants)
	p.cns[ns] = s
	return nil
}

func (p *pass) take(ns, cluster, what, holder string) {
	k := ns + "/" + cluster
	if p.taken[k] == nil {
		p.taken[k] = map[string]string{}
	}
	if _, ok := p.taken[k][what]; !ok {
		p.taken[k][what] = holder
	}
}

func (p *pass) holder(ref policy.ClusterRef, what string) string {
	return p.taken[ref.String()][what]
}

func hasFinalizer(o *unstructured.Unstructured) bool {
	for _, f := range o.GetFinalizers() {
		if f == Finalizer {
			return true
		}
	}
	return false
}

// claim reconciles one live claim and returns its phase.
func (p *pass) claim(o *unstructured.Unstructured, cl policy.Claim) string {
	key := cl.Namespace + "/" + cl.Name
	gen := o.GetGeneration()
	st := map[string]interface{}{"observedGeneration": gen, "secretName": policy.SecretName(cl)}
	set := func(phase, reason, msg string) string {
		st["phase"] = phase
		p.setStatus(o, st, phase == PhaseReady, reason, msg)
		return phase
	}

	db, owner, err := policy.Names(cl)
	if err != nil {
		return set(PhaseInvalid, "InvalidSpec", err.Error())
	}
	st["database"], st["owner"] = db, owner
	if cl.Cluster.Name == "" {
		return set(PhaseInvalid, "InvalidSpec", "spec.cluster.name is required")
	}
	if !p.c.watched(cl.Cluster.Namespace) {
		return set(PhaseInvalid, "NamespaceNotWatched", fmt.Sprintf("the controller does not manage cluster namespace %s", cl.Cluster.Namespace))
	}
	s := p.cns[cl.Cluster.Namespace]
	cluster := s.clusters[cl.Cluster.Name]
	if cluster == nil {
		return set(PhasePending, "ClusterNotFound", fmt.Sprintf("cluster %s not found", cl.Cluster))
	}
	name := policy.ObjectName(cl.Namespace, cl.Name)
	existingDB, existingRole := s.dbs[name], s.roles[name]
	if existingDB != nil && str(existingDB, "spec", "name") != db {
		return set(PhaseInvalid, "Immutable", fmt.Sprintf("the database name cannot change (it is %q)", str(existingDB, "spec", "name")))
	}
	if existingRole != nil && str(existingRole, "spec", "name") != owner {
		return set(PhaseInvalid, "Immutable", fmt.Sprintf("the owner role cannot change (it is %q)", str(existingRole, "spec", "name")))
	}

	// Pick the first grant that admits the claim and still has room.
	var grant *policy.Grant
	var reasons []string
	var countKey string
	for i := range s.grants {
		g := s.grants[i]
		if g.Cluster != cl.Cluster.Name {
			continue
		}
		if r := g.Admits(cl, p.nsLabels[cl.Namespace]); r != "" {
			reasons = append(reasons, r)
			continue
		}
		ck := g.Name + "|" + cl.Namespace
		if g.MaxDatabases > 0 && p.counts[ck] >= g.MaxDatabases {
			reasons = append(reasons, fmt.Sprintf("grant %s allows %d database(s) for namespace %s", g.Name, g.MaxDatabases, cl.Namespace))
			continue
		}
		p.counts[ck]++
		countKey = ck
		grant = &g
		break
	}
	if grant == nil {
		msg := fmt.Sprintf("no DatabaseGrant in namespace %s admits this claim for cluster %s", cl.Cluster.Namespace, cl.Cluster.Name)
		if len(reasons) > 0 {
			msg += ": " + strings.Join(reasons, "; ")
		}
		if existingDB != nil || existingRole != nil {
			msg += " (the existing database and role are kept)"
		}
		return set(PhaseNotGranted, "NotGranted", msg)
	}
	st["grant"] = grant.Name

	// The credentials Secret in the claim namespace must be ours.
	appSecretName := policy.SecretName(cl)
	appSecret, err := p.c.Client.Resource(SecretGVR).Namespace(cl.Namespace).Get(p.ctx, appSecretName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		p.errs = append(p.errs, err)
		return set(PhasePending, "Error", err.Error())
	}
	if err == nil && appSecret.GetLabels()[LabelManagedBy] != ManagedBy {
		p.counts[countKey]--
		return set(PhaseConflict, "SecretExists", fmt.Sprintf("secret %s/%s exists and is not managed by cnpg-dbclaim", cl.Namespace, appSecretName))
	}
	if apierrors.IsNotFound(err) {
		appSecret = nil
	}

	for _, w := range []struct{ what, v string }{{"db:", db}, {"role:", owner}} {
		if h := p.holder(cl.Cluster, w.what+w.v); h != "" && h != key {
			kind := "database"
			if w.what == "role:" {
				kind = "role"
			}
			p.counts[countKey]--
			return set(PhaseConflict, "NameTaken", fmt.Sprintf("%s %q on cluster %s is already used by %s", kind, w.v, cl.Cluster, h))
		}
	}
	p.take(cl.Cluster.Namespace, cl.Cluster.Name, "db:"+db, key)
	p.take(cl.Cluster.Namespace, cl.Cluster.Name, "role:"+owner, key)

	if !hasFinalizer(o) {
		o.SetFinalizers(append(o.GetFinalizers(), Finalizer))
		upd, err := p.c.Client.Resource(ClaimGVR).Namespace(cl.Namespace).Update(p.ctx, o, metav1.UpdateOptions{})
		if err != nil {
			p.errs = append(p.errs, fmt.Errorf("add finalizer to %s: %w", key, err))
			return PhasePending
		}
		*o = *upd
	}

	limit := policy.ConnectionLimit(cl, *grant)
	reclaim := policy.ReclaimPolicy(cl)
	labels := managedLabels(cl)
	ann := map[string]string{AnnClaimUID: cl.UID}

	pw, err := p.ensurePassword(s, cl, name, owner, o.GetAnnotations()[AnnRotate], labels, ann)
	if err != nil {
		p.errs = append(p.errs, err)
		return set(PhasePending, "Error", err.Error())
	}
	clusterRef := map[string]interface{}{"name": cl.Cluster.Name}
	roleSpec := map[string]interface{}{
		"cluster":                   clusterRef,
		"name":                      owner,
		"login":                     true,
		"superuser":                 false,
		"createdb":                  false,
		"createrole":                false,
		"replication":               false,
		"bypassrls":                 false,
		"inherit":                   true,
		"connectionLimit":           limit,
		"passwordSecret":            map[string]interface{}{"name": name + "-password"},
		"databaseRoleReclaimPolicy": reclaim,
		"ensure":                    "present",
		"comment":                   "cnpg-dbclaim " + key,
	}
	role, err := p.ensure(RoleGVR, "DatabaseRole", cl.Cluster.Namespace, name, existingRole, roleSpec, labels, ann)
	if err != nil {
		p.errs = append(p.errs, err)
		return set(PhasePending, "Error", err.Error())
	}
	var exts []interface{}
	for _, e := range cl.Extensions {
		exts = append(exts, map[string]interface{}{"name": e, "ensure": "present"})
	}
	dbSpec := map[string]interface{}{
		"cluster":               clusterRef,
		"name":                  db,
		"owner":                 owner,
		"databaseReclaimPolicy": reclaim,
		"connectionLimit":       limit,
		"ensure":                "present",
	}
	if len(exts) > 0 {
		dbSpec["extensions"] = exts
	}
	dbo, err := p.ensure(DatabaseGVR, "Database", cl.Cluster.Namespace, name, existingDB, dbSpec, labels, ann)
	if err != nil {
		p.errs = append(p.errs, err)
		return set(PhasePending, "Error", err.Error())
	}

	host := cl.Cluster.Name + "-rw." + cl.Cluster.Namespace + ".svc"
	st["host"] = host
	for _, x := range []struct {
		what string
		o    *unstructured.Unstructured
	}{{"role", role}, {"database", dbo}} {
		if !applied(x.o) {
			msg := str(x.o, "status", "message")
			if msg == "" {
				msg = "waiting for CloudNativePG"
			}
			return set(PhasePending, "Reconciling", fmt.Sprintf("%s %s/%s not applied yet: %s", x.what, cl.Cluster.Namespace, name, msg))
		}
	}
	data := connectionData(cl.Cluster, owner, pw, db)
	if err := p.ensureAppSecret(o, appSecret, appSecretName, data); err != nil {
		p.errs = append(p.errs, err)
		return set(PhasePending, "Error", err.Error())
	}
	return set(PhaseReady, "Ready", fmt.Sprintf("database %s with owner %s is ready on %s", db, owner, cl.Cluster))
}

// applied reports whether CNPG has applied the current generation.
func applied(o *unstructured.Unstructured) bool {
	if !boolean(o, "status", "applied") {
		return false
	}
	if g, ok := integer(o, "status", "observedGeneration"); ok && g < o.GetGeneration() {
		return false
	}
	return true
}

const pwAlphabet = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"

func (c *Controller) password() (string, error) {
	r := c.Rand
	if r == nil {
		r = rand.Reader
	}
	b := make([]byte, 32)
	max := big.NewInt(int64(len(pwAlphabet)))
	for i := range b {
		n, err := rand.Int(r, max)
		if err != nil {
			return "", err
		}
		b[i] = pwAlphabet[n.Int64()]
	}
	return string(b), nil
}

func secretData(o *unstructured.Unstructured) map[string]string {
	out := map[string]string{}
	d, _, _ := unstructured.NestedMap(o.Object, "data")
	for k, v := range d {
		s, _ := v.(string)
		b, err := base64.StdEncoding.DecodeString(s)
		if err == nil {
			out[k] = string(b)
		}
	}
	return out
}

func setSecretData(o *unstructured.Unstructured, data map[string]string) {
	d := map[string]interface{}{}
	for k, v := range data {
		d[k] = base64.StdEncoding.EncodeToString([]byte(v))
	}
	o.Object["data"] = d
}

// ensurePassword makes sure the basic-auth Secret that the DatabaseRole
// points at exists and returns the password. A changed value of the claim's
// rotate annotation creates a new password.
func (p *pass) ensurePassword(s *clusterNS, cl policy.Claim, name, user, rotate string, labels, ann map[string]string) (string, error) {
	sname := name + "-password"
	sec := s.secrets[sname]
	lb := map[string]string{"cnpg.io/reload": "true"}
	for k, v := range labels {
		lb[k] = v
	}
	if sec != nil {
		d := secretData(sec)
		if d["username"] == user && d["password"] != "" && sec.GetAnnotations()[AnnRotatedFor] == rotate {
			return d["password"], nil
		}
	}
	pw, err := p.c.password()
	if err != nil {
		return "", err
	}
	a := map[string]string{AnnRotatedFor: rotate}
	for k, v := range ann {
		a[k] = v
	}
	cli := p.c.Client.Resource(SecretGVR).Namespace(cl.Cluster.Namespace)
	if sec == nil {
		n := newObject(SecretGVR, "Secret", cl.Cluster.Namespace, sname, lb, a)
		n.Object["type"] = "kubernetes.io/basic-auth"
		setSecretData(n, map[string]string{"username": user, "password": pw})
		created, err := cli.Create(p.ctx, n, metav1.CreateOptions{})
		if err != nil {
			return "", fmt.Errorf("create secret %s/%s: %w", cl.Cluster.Namespace, sname, err)
		}
		s.secrets[sname] = created
		p.c.logf("created password secret %s/%s for claim %s/%s", cl.Cluster.Namespace, sname, cl.Namespace, cl.Name)
		return pw, nil
	}
	sec.SetLabels(lb)
	sec.SetAnnotations(a)
	setSecretData(sec, map[string]string{"username": user, "password": pw})
	upd, err := cli.Update(p.ctx, sec, metav1.UpdateOptions{})
	if err != nil {
		return "", fmt.Errorf("update secret %s/%s: %w", cl.Cluster.Namespace, sname, err)
	}
	s.secrets[sname] = upd
	p.c.logf("rotated password of claim %s/%s", cl.Namespace, cl.Name)
	return pw, nil
}

func sameJSON(a, b interface{}) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

// ensure creates the object or updates the fields of spec that differ.
// Fields that CNPG's webhook adds and that spec does not mention are left
// alone.
func (p *pass) ensure(gvr schema.GroupVersionResource, kind, ns, name string, cur *unstructured.Unstructured, spec map[string]interface{}, labels, ann map[string]string) (*unstructured.Unstructured, error) {
	cli := p.c.Client.Resource(gvr).Namespace(ns)
	if cur == nil {
		o := newObject(gvr, kind, ns, name, labels, ann)
		o.Object["spec"] = spec
		created, err := cli.Create(p.ctx, o, metav1.CreateOptions{})
		if err != nil {
			return nil, fmt.Errorf("create %s %s/%s: %w", kind, ns, name, err)
		}
		p.c.logf("created %s %s/%s", kind, ns, name)
		return created, nil
	}
	curSpec, _, _ := unstructured.NestedMap(cur.Object, "spec")
	if curSpec == nil {
		curSpec = map[string]interface{}{}
	}
	changed := false
	for k, v := range spec {
		if !sameJSON(curSpec[k], v) {
			curSpec[k] = v
			changed = true
		}
	}
	if _, ok := spec["extensions"]; !ok {
		if _, had := curSpec["extensions"]; had && kind == "Database" {
			delete(curSpec, "extensions")
			changed = true
		}
	}
	a := cur.GetAnnotations()
	if a[AnnClaimUID] != ann[AnnClaimUID] {
		if a == nil {
			a = map[string]string{}
		}
		a[AnnClaimUID] = ann[AnnClaimUID]
		cur.SetAnnotations(a)
		changed = true
	}
	if !changed {
		return cur, nil
	}
	cur.Object["spec"] = curSpec
	upd, err := cli.Update(p.ctx, cur, metav1.UpdateOptions{})
	if err != nil {
		return nil, fmt.Errorf("update %s %s/%s: %w", kind, ns, name, err)
	}
	p.c.logf("updated %s %s/%s", kind, ns, name)
	return upd, nil
}

// connectionData uses the keys of the application Secret that CloudNativePG
// creates for a Cluster, plus ro-host and ro-uri.
func connectionData(ref policy.ClusterRef, user, pw, db string) map[string]string {
	host := ref.Name + "-rw." + ref.Namespace + ".svc"
	ro := ref.Name + "-ro." + ref.Namespace + ".svc"
	uri := func(h string) string {
		u := url.URL{Scheme: "postgresql", User: url.UserPassword(user, pw), Host: h + ":5432", Path: "/" + db}
		return u.String()
	}
	return map[string]string{
		"username": user,
		"user":     user,
		"password": pw,
		"dbname":   db,
		"host":     host,
		"port":     "5432",
		"uri":      uri(host),
		"jdbc-uri": fmt.Sprintf("jdbc:postgresql://%s:5432/%s?password=%s&user=%s", host, db, url.QueryEscape(pw), url.QueryEscape(user)),
		"pgpass":   fmt.Sprintf("%s:5432:%s:%s:%s\n", host, db, user, pw),
		"ro-host":  ro,
		"ro-uri":   uri(ro),
	}
}

func (p *pass) ensureAppSecret(claim, cur *unstructured.Unstructured, name string, data map[string]string) error {
	ns := claim.GetNamespace()
	cli := p.c.Client.Resource(SecretGVR).Namespace(ns)
	ownerRef := map[string]interface{}{
		"apiVersion":         ClaimGVR.GroupVersion().String(),
		"kind":               "DatabaseClaim",
		"name":               claim.GetName(),
		"uid":                string(claim.GetUID()),
		"controller":         true,
		"blockOwnerDeletion": true,
	}
	labels := map[string]string{LabelManagedBy: ManagedBy, LabelClaimNS: ns, LabelClaimName: claim.GetName()}
	if cur == nil {
		o := newObject(SecretGVR, "Secret", ns, name, labels, nil)
		o.Object["type"] = "Opaque"
		setSecretData(o, data)
		unstructured.SetNestedSlice(o.Object, []interface{}{ownerRef}, "metadata", "ownerReferences")
		if _, err := cli.Create(p.ctx, o, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create secret %s/%s: %w", ns, name, err)
		}
		p.c.logf("created secret %s/%s", ns, name)
		return nil
	}
	if sameJSON(secretData(cur), data) {
		return nil
	}
	setSecretData(cur, data)
	unstructured.SetNestedSlice(cur.Object, []interface{}{ownerRef}, "metadata", "ownerReferences")
	if _, err := cli.Update(p.ctx, cur, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update secret %s/%s: %w", ns, name, err)
	}
	p.c.logf("updated secret %s/%s", ns, name)
	return nil
}

func (p *pass) setStatus(o *unstructured.Unstructured, st map[string]interface{}, ready bool, reason, msg string) {
	old, _, _ := unstructured.NestedMap(o.Object, "status")
	cond := condition(ready, reason, msg, o.GetGeneration())
	if old != nil {
		if conds, ok := old["conditions"].([]interface{}); ok && len(conds) == 1 {
			if oc, ok := conds[0].(map[string]interface{}); ok && oc["status"] == cond["status"] {
				cond["lastTransitionTime"] = oc["lastTransitionTime"]
			}
		}
	}
	st["conditions"] = []interface{}{cond}
	if old != nil && sameJSON(old, st) {
		return
	}
	o.Object["status"] = st
	upd, err := p.c.Client.Resource(ClaimGVR).Namespace(o.GetNamespace()).UpdateStatus(p.ctx, o, metav1.UpdateOptions{})
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("update status of %s/%s: %w", o.GetNamespace(), o.GetName(), err))
		return
	}
	*o = *upd
	if old == nil || old["phase"] != st["phase"] {
		p.c.logf("claim %s/%s: %s (%s)", o.GetNamespace(), o.GetName(), st["phase"], msg)
	}
}

// cleanup deletes the Database, then (once it is gone) the DatabaseRole,
// then the password Secret. It returns true when nothing is left.
func (p *pass) cleanup(ns, name string) bool {
	s := p.cns[ns]
	if s == nil {
		return false
	}
	del := func(gvr schema.GroupVersionResource, o *unstructured.Unstructured) {
		if o.GetDeletionTimestamp() != nil {
			return
		}
		err := p.c.Client.Resource(gvr).Namespace(ns).Delete(p.ctx, o.GetName(), metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			p.errs = append(p.errs, fmt.Errorf("delete %s %s/%s: %w", gvr.Resource, ns, o.GetName(), err))
			return
		}
		p.c.logf("deleted %s %s/%s", gvr.Resource, ns, o.GetName())
	}
	if o := s.dbs[name]; o != nil {
		del(DatabaseGVR, o)
		return false
	}
	if o := s.roles[name]; o != nil {
		del(RoleGVR, o)
		return false
	}
	if o := s.secrets[name+"-password"]; o != nil {
		del(SecretGVR, o)
		delete(s.secrets, name+"-password")
	}
	return true
}

func (p *pass) deleteClaim(o *unstructured.Unstructured, cl policy.Claim) {
	ns := cl.Cluster.Namespace
	if p.cns[ns] == nil {
		if !p.c.watched(ns) {
			p.removeFinalizer(o)
		}
		return
	}
	if !p.cleanup(ns, policy.ObjectName(cl.Namespace, cl.Name)) {
		return
	}
	p.removeFinalizer(o)
}

func (p *pass) removeFinalizer(o *unstructured.Unstructured) {
	var keep []string
	for _, f := range o.GetFinalizers() {
		if f != Finalizer {
			keep = append(keep, f)
		}
	}
	o.SetFinalizers(keep)
	if _, err := p.c.Client.Resource(ClaimGVR).Namespace(o.GetNamespace()).Update(p.ctx, o, metav1.UpdateOptions{}); err != nil && !apierrors.IsNotFound(err) {
		p.errs = append(p.errs, fmt.Errorf("remove finalizer from %s/%s: %w", o.GetNamespace(), o.GetName(), err))
		return
	}
	p.c.logf("claim %s/%s cleaned up", o.GetNamespace(), o.GetName())
}

// orphans cleans up managed objects whose claim no longer exists (for
// example after the claim was removed with its finalizer stripped).
func (p *pass) orphans(live map[string]*unstructured.Unstructured) {
	nss := make([]string, 0, len(p.cns))
	for ns := range p.cns {
		nss = append(nss, ns)
	}
	sort.Strings(nss)
	for _, ns := range nss {
		s := p.cns[ns]
		seen := map[string]bool{}
		for _, m := range []map[string]*unstructured.Unstructured{s.dbs, s.roles} {
			for name, o := range m {
				key := o.GetLabels()[LabelClaimNS] + "/" + o.GetLabels()[LabelClaimName]
				if live[key] == nil && !seen[name] {
					seen[name] = true
					p.c.logf("orphaned objects %s/%s (claim %s is gone)", ns, name, key)
					p.cleanup(ns, name)
				}
			}
		}
		for name, o := range s.secrets {
			base := strings.TrimSuffix(name, "-password")
			key := o.GetLabels()[LabelClaimNS] + "/" + o.GetLabels()[LabelClaimName]
			if live[key] == nil && s.dbs[base] == nil && s.roles[base] == nil {
				p.cleanup(ns, base)
			}
		}
	}
}

// Metrics writes the counters in the Prometheus text format.
func (c *Controller) Metrics(w io.Writer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Fprintln(w, "# HELP cnpg_dbclaim_claims DatabaseClaims by phase after the last reconcile pass.")
	fmt.Fprintln(w, "# TYPE cnpg_dbclaim_claims gauge")
	for _, ph := range []string{PhaseReady, PhasePending, PhaseNotGranted, PhaseConflict, PhaseInvalid, PhaseDeleting} {
		fmt.Fprintf(w, "cnpg_dbclaim_claims{phase=%q} %d\n", ph, c.phases[ph])
	}
	fmt.Fprintln(w, "# HELP cnpg_dbclaim_reconcile_passes_total Reconcile passes.")
	fmt.Fprintln(w, "# TYPE cnpg_dbclaim_reconcile_passes_total counter")
	fmt.Fprintf(w, "cnpg_dbclaim_reconcile_passes_total %d\n", c.passes)
	fmt.Fprintln(w, "# HELP cnpg_dbclaim_reconcile_errors_total Errors during reconcile passes.")
	fmt.Fprintln(w, "# TYPE cnpg_dbclaim_reconcile_errors_total counter")
	fmt.Fprintf(w, "cnpg_dbclaim_reconcile_errors_total %d\n", c.errors)
	fmt.Fprintln(w, "# HELP cnpg_dbclaim_reconcile_duration_seconds Duration of the last pass.")
	fmt.Fprintln(w, "# TYPE cnpg_dbclaim_reconcile_duration_seconds gauge")
	fmt.Fprintf(w, "cnpg_dbclaim_reconcile_duration_seconds %g\n", c.lastDur.Seconds())
}
