// Package policy holds the parts of cnpg-dbclaim that need no Kubernetes
// API: deriving PostgreSQL names from a DatabaseClaim, deciding whether a
// DatabaseGrant admits a claim, and naming the objects created for it.
package policy

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"
)

// ClusterRef points at a CloudNativePG Cluster.
type ClusterRef struct {
	Namespace string
	Name      string
}

func (r ClusterRef) String() string { return r.Namespace + "/" + r.Name }

// Claim is the part of a DatabaseClaim the policy needs.
type Claim struct {
	Namespace string
	Name      string
	UID       string
	Created   time.Time

	Cluster         ClusterRef
	Database        string // empty: derived from namespace and name
	Owner           string // empty: same as the database name
	Extensions      []string
	ConnectionLimit *int64
	ReclaimPolicy   string // "retain" (default) or "delete"
	SecretName      string // empty: "<name>-db"
}

// Grant is the part of a DatabaseGrant the policy needs. A grant lives in
// the namespace of the Cluster it refers to.
type Grant struct {
	Namespace string
	Name      string

	Cluster            string
	FromNamespaces     []string
	NamespaceSelector  map[string]string // matchLabels; nil means "not used"
	NamePattern        string            // glob, "{namespace}" is replaced; default "{namespace}_*"
	MaxDatabases       int               // per namespace; 0 means no limit
	AllowReclaimDelete bool
	AllowedExtensions  []string
	MaxConnectionLimit int64 // 0 means no cap
}

// DefaultPattern is used when a grant sets no namePattern.
const DefaultPattern = "{namespace}_*"

var ident = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// reserved are names a claim may never take, independent of any grant.
var reserved = map[string]bool{
	"postgres": true, "template0": true, "template1": true,
	"streaming_replica": true, "cnpg_pooler_pgbouncer": true,
	"public": true,
}

// NamespaceToken turns a namespace into the form used in default names and
// in "{namespace}" of a pattern: lower case, '-' and '.' become '_'.
func NamespaceToken(ns string) string {
	return strings.NewReplacer("-", "_", ".", "_").Replace(strings.ToLower(ns))
}

// Names returns the database and owner role a claim asks for.
func Names(c Claim) (db, owner string, err error) {
	db = c.Database
	if db == "" {
		db = NamespaceToken(c.Namespace) + "_" + NamespaceToken(c.Name)
		if len(db) > 63 {
			return "", "", fmt.Errorf("derived database name %q is longer than 63 characters; set spec.database", db)
		}
	}
	owner = c.Owner
	if owner == "" {
		owner = db
	}
	for _, n := range []struct{ what, v string }{{"database", db}, {"owner", owner}} {
		if !ident.MatchString(n.v) {
			return "", "", fmt.Errorf("%s name %q must match %s", n.what, n.v, ident.String())
		}
		if reserved[n.v] || strings.HasPrefix(n.v, "pg_") {
			return "", "", fmt.Errorf("%s name %q is reserved", n.what, n.v)
		}
	}
	for _, e := range c.Extensions {
		if !ident.MatchString(e) {
			return "", "", fmt.Errorf("extension name %q must match %s", e, ident.String())
		}
	}
	switch c.ReclaimPolicy {
	case "", "retain", "delete":
	default:
		return "", "", fmt.Errorf("reclaimPolicy must be retain or delete, not %q", c.ReclaimPolicy)
	}
	if c.ConnectionLimit != nil && *c.ConnectionLimit < -1 {
		return "", "", fmt.Errorf("connectionLimit must be -1 or larger")
	}
	return db, owner, nil
}

// ReclaimPolicy returns the claim's policy with the default applied.
func ReclaimPolicy(c Claim) string {
	if c.ReclaimPolicy == "" {
		return "retain"
	}
	return c.ReclaimPolicy
}

// SecretName returns the name of the credentials Secret in the claim's
// namespace.
func SecretName(c Claim) string {
	if c.SecretName != "" {
		return c.SecretName
	}
	return c.Name + "-db"
}

// ConnectionLimit returns the limit to put on the role and the database:
// the claim's value, or the grant's cap when the claim sets none, or -1.
func ConnectionLimit(c Claim, g Grant) int64 {
	if c.ConnectionLimit != nil {
		return *c.ConnectionLimit
	}
	if g.MaxConnectionLimit > 0 {
		return g.MaxConnectionLimit
	}
	return -1
}

// CoversNamespace reports whether the grant lets the namespace attach at
// all. nsLabels are the labels of the claim's namespace.
func (g Grant) CoversNamespace(ns string, nsLabels map[string]string) bool {
	for _, n := range g.FromNamespaces {
		if n == ns {
			return true
		}
	}
	if g.NamespaceSelector != nil {
		for k, v := range g.NamespaceSelector {
			if nsLabels[k] != v {
				return false
			}
		}
		return true
	}
	return false
}

// Pattern returns the grant's name pattern with "{namespace}" filled in.
func (g Grant) Pattern(ns string) string {
	p := g.NamePattern
	if p == "" {
		p = DefaultPattern
	}
	return strings.ReplaceAll(p, "{namespace}", NamespaceToken(ns))
}

// Admits checks every limit of the grant against the claim. It does not
// check the per-namespace database count, which needs all claims.
// The returned reason is empty when the grant admits the claim.
func (g Grant) Admits(c Claim, nsLabels map[string]string) string {
	if g.Namespace != c.Cluster.Namespace || g.Cluster != c.Cluster.Name {
		return fmt.Sprintf("grant %s/%s is for cluster %s/%s", g.Namespace, g.Name, g.Namespace, g.Cluster)
	}
	if !g.CoversNamespace(c.Namespace, nsLabels) {
		return fmt.Sprintf("grant %s does not include namespace %s", g.Name, c.Namespace)
	}
	db, owner, err := Names(c)
	if err != nil {
		return err.Error()
	}
	pat := g.Pattern(c.Namespace)
	for _, n := range []string{db, owner} {
		ok, err := path.Match(pat, n)
		if err != nil {
			return fmt.Sprintf("grant %s has an invalid namePattern %q: %v", g.Name, pat, err)
		}
		if !ok {
			return fmt.Sprintf("grant %s allows only names matching %q, not %q", g.Name, pat, n)
		}
	}
	if ReclaimPolicy(c) == "delete" && !g.AllowReclaimDelete {
		return fmt.Sprintf("grant %s does not allow reclaimPolicy delete", g.Name)
	}
	allowed := map[string]bool{}
	for _, e := range g.AllowedExtensions {
		allowed[e] = true
	}
	for _, e := range c.Extensions {
		if !allowed[e] {
			return fmt.Sprintf("grant %s does not allow extension %q", g.Name, e)
		}
	}
	if g.MaxConnectionLimit > 0 && c.ConnectionLimit != nil {
		if l := *c.ConnectionLimit; l == -1 || l > g.MaxConnectionLimit {
			return fmt.Sprintf("grant %s caps connectionLimit at %d", g.Name, g.MaxConnectionLimit)
		}
	}
	return ""
}

// ObjectName is the name of the Database, DatabaseRole and password Secret
// created in the cluster's namespace for a claim. It hashes namespace and
// name with a separator that cannot occur in either, so different claims
// never share a name.
func ObjectName(claimNamespace, claimName string) string {
	sum := sha256.Sum256([]byte(claimNamespace + "/" + claimName))
	return "dbclaim-" + hex.EncodeToString(sum[:])[:16]
}

// SortClaims orders claims oldest first (then by UID), the order in which
// they get their names and count against limits.
func SortClaims(cs []Claim) {
	sort.SliceStable(cs, func(i, j int) bool {
		if !cs[i].Created.Equal(cs[j].Created) {
			return cs[i].Created.Before(cs[j].Created)
		}
		return cs[i].UID < cs[j].UID
	})
}

// SortGrants orders grants by name, the order in which they are tried.
func SortGrants(gs []Grant) {
	sort.SliceStable(gs, func(i, j int) bool { return gs[i].Name < gs[j].Name })
}
