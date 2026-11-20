package controller

import (
	"time"

	"github.com/Jwrede/cnpg-dbclaim/internal/policy"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// API group of the two resources this controller defines.
const (
	Group   = "dbclaim.jonathanwrede.de"
	Version = "v1alpha1"

	Finalizer      = Group + "/cleanup"
	LabelManagedBy = "app.kubernetes.io/managed-by"
	ManagedBy      = "cnpg-dbclaim"
	LabelClaimNS   = Group + "/claim-namespace"
	LabelClaimName = Group + "/claim-name"
	AnnClaimUID    = Group + "/claim-uid"
	AnnRotate      = Group + "/rotate"
	AnnRotatedFor  = Group + "/rotated-for"
)

var (
	ClaimGVR     = schema.GroupVersionResource{Group: Group, Version: Version, Resource: "databaseclaims"}
	GrantGVR     = schema.GroupVersionResource{Group: Group, Version: Version, Resource: "databasegrants"}
	ClusterGVR   = schema.GroupVersionResource{Group: "postgresql.cnpg.io", Version: "v1", Resource: "clusters"}
	DatabaseGVR  = schema.GroupVersionResource{Group: "postgresql.cnpg.io", Version: "v1", Resource: "databases"}
	RoleGVR      = schema.GroupVersionResource{Group: "postgresql.cnpg.io", Version: "v1", Resource: "databaseroles"}
	SecretGVR    = schema.GroupVersionResource{Version: "v1", Resource: "secrets"}
	NamespaceGVR = schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
)

// ListKinds maps the resources above to their list kinds (for the fake
// dynamic client in tests).
var ListKinds = map[schema.GroupVersionResource]string{
	ClaimGVR:     "DatabaseClaimList",
	GrantGVR:     "DatabaseGrantList",
	ClusterGVR:   "ClusterList",
	DatabaseGVR:  "DatabaseList",
	RoleGVR:      "DatabaseRoleList",
	SecretGVR:    "SecretList",
	NamespaceGVR: "NamespaceList",
}

func str(o *unstructured.Unstructured, f ...string) string {
	s, _, _ := unstructured.NestedString(o.Object, f...)
	return s
}

func strs(o *unstructured.Unstructured, f ...string) []string {
	s, _, _ := unstructured.NestedStringSlice(o.Object, f...)
	return s
}

func boolean(o *unstructured.Unstructured, f ...string) bool {
	b, _, _ := unstructured.NestedBool(o.Object, f...)
	return b
}

func integer(o *unstructured.Unstructured, f ...string) (int64, bool) {
	v, ok, _ := unstructured.NestedFieldNoCopy(o.Object, f...)
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	}
	return 0, false
}

func claimFrom(o *unstructured.Unstructured) policy.Claim {
	c := policy.Claim{
		Namespace:     o.GetNamespace(),
		Name:          o.GetName(),
		UID:           string(o.GetUID()),
		Created:       o.GetCreationTimestamp().Time,
		Cluster:       policy.ClusterRef{Namespace: str(o, "spec", "cluster", "namespace"), Name: str(o, "spec", "cluster", "name")},
		Database:      str(o, "spec", "database"),
		Owner:         str(o, "spec", "owner"),
		Extensions:    strs(o, "spec", "extensions"),
		ReclaimPolicy: str(o, "spec", "reclaimPolicy"),
		SecretName:    str(o, "spec", "secretName"),
	}
	if c.Cluster.Namespace == "" {
		c.Cluster.Namespace = c.Namespace
	}
	if l, ok := integer(o, "spec", "connectionLimit"); ok {
		c.ConnectionLimit = &l
	}
	return c
}

func grantFrom(o *unstructured.Unstructured) policy.Grant {
	g := policy.Grant{
		Namespace:          o.GetNamespace(),
		Name:               o.GetName(),
		Cluster:            str(o, "spec", "cluster"),
		NamePattern:        str(o, "spec", "namePattern"),
		AllowReclaimDelete: boolean(o, "spec", "allowReclaimDelete"),
		AllowedExtensions:  strs(o, "spec", "allowedExtensions"),
	}
	if n, ok := integer(o, "spec", "maxDatabases"); ok {
		g.MaxDatabases = int(n)
	}
	if n, ok := integer(o, "spec", "maxConnectionLimit"); ok {
		g.MaxConnectionLimit = n
	}
	from, _, _ := unstructured.NestedSlice(o.Object, "spec", "from")
	for _, f := range from {
		m, ok := f.(map[string]interface{})
		if !ok {
			continue
		}
		if ns, ok := m["namespace"].(string); ok && ns != "" {
			g.FromNamespaces = append(g.FromNamespaces, ns)
		}
		if sel, ok := m["namespaceSelector"].(map[string]interface{}); ok {
			ml, _ := sel["matchLabels"].(map[string]interface{})
			g.NamespaceSelector = map[string]string{}
			for k, v := range ml {
				if s, ok := v.(string); ok {
					g.NamespaceSelector[k] = s
				}
			}
		}
	}
	return g
}

func managedLabels(c policy.Claim) map[string]string {
	return map[string]string{
		LabelManagedBy: ManagedBy,
		LabelClaimNS:   c.Namespace,
		LabelClaimName: c.Name,
	}
}

func newObject(gvr schema.GroupVersionResource, kind, ns, name string, labels, ann map[string]string) *unstructured.Unstructured {
	o := &unstructured.Unstructured{Object: map[string]interface{}{}}
	o.SetAPIVersion(gvr.GroupVersion().String())
	o.SetKind(kind)
	o.SetNamespace(ns)
	o.SetName(name)
	o.SetLabels(labels)
	if len(ann) > 0 {
		o.SetAnnotations(ann)
	}
	return o
}

func condition(status bool, reason, msg string, gen int64) map[string]interface{} {
	s := "False"
	if status {
		s = "True"
	}
	return map[string]interface{}{
		"type":               "Ready",
		"status":             s,
		"reason":             reason,
		"message":            msg,
		"observedGeneration": gen,
		"lastTransitionTime": metav1.Now().UTC().Format(time.RFC3339),
	}
}
