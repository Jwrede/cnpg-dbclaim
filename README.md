# cnpg-dbclaim

Self-service databases on a shared [CloudNativePG](https://cloudnative-pg.io) cluster for
applications in other namespaces.

An application team creates a `DatabaseClaim` in its own namespace. The team that owns the
shared `Cluster` decides, with a `DatabaseGrant` in the cluster's namespace, which namespaces may
claim databases there, which names they may use, how many, with which extensions and whether a
claim may drop its database on deletion. The controller creates CloudNativePG's own `Database`
and `DatabaseRole` objects next to the cluster and writes the credentials into a Secret in the
application's namespace, with the same keys as the `<cluster>-app` Secret that CloudNativePG
creates.

```
namespace team-a                     namespace databases
+--------------------------+         +---------------------------------------+
| DatabaseClaim orders     | ------> | DatabaseGrant app-teams  (consent)    |
|   cluster: databases/    |         | Cluster shared                        |
|            shared        |         | Database      dbclaim-<hash>  <-------+-- created by
| Secret orders-db  <------+-------- | DatabaseRole  dbclaim-<hash>          |   cnpg-dbclaim
|   (uri, host, password)  |         | Secret        dbclaim-<hash>-password |
+--------------------------+         +---------------------------------------+
```

CloudNativePG itself is not changed. Its operator and instance manager keep their RBAC; the
cross-namespace work happens in a separate controller with its own ServiceAccount, and the
PostgreSQL pods never see objects from other namespaces.

## Why

CloudNativePG's `Database` and `DatabaseRole` resources reference their `Cluster` by local name,
so they must live in the cluster's namespace. A team that wants one shared PostgreSQL cluster for
many applications has three options today: give every application team write access to the
database namespace (Kubernetes RBAC cannot restrict that to "your own" databases), run one
cluster per namespace, or write admission policies that pin names per namespace.

- [cloudnative-pg #6043](https://github.com/cloudnative-pg/cloudnative-pg/issues/6043)
  "allow cross-namespace Database and Role configuration": 67 reactions, 35 comments, open since
  11/2024, defended against the stale bot several times. Use cases in the thread: hundreds of
  small databases for dynamically created application instances, homelabs with one shared
  cluster, an organisation moving about 30 projects from a shared Cloud SQL instance (blocked on
  exactly this).
- The upstream PR [#9823](https://github.com/cloudnative-pg/cloudnative-pg/pull/9823) adds a
  per-cluster flag. Two maintainers lean against merging it: it gives the operator cluster-scoped
  rights on ClusterRoles and lets the instance manager of an opted-in cluster change `Database`
  objects in every namespace (mnencia, 10/2026, who also points out a duplicate-name check
  limited to one namespace and a ClusterRole naming collision). gbartolini (08/2026) calls the
  `ReferenceGrant` pattern from Gateway API "the strongest version of this argument so far",
  but not something to take on in the operator now. mnencia: "If someone finds a design that
  adds no cluster-scoped permissions for the operator and keeps the instance manager's reach to
  the namespaces that opted in, I'm happy to look at it."

cnpg-dbclaim implements the `ReferenceGrant` shape outside the operator. General-purpose
operators such as ext-postgres-operator or db-operator can also create databases on a shared
PostgreSQL server, but they connect with their own superuser credentials and do not use
CloudNativePG's `Database` and `DatabaseRole` reconciliation.

## How it works

Each pass (on every watch event and every 30 s) the controller:

1. Reads all `DatabaseClaim`s, oldest first, and the `DatabaseGrant`s, `Cluster`s, `Database`s
   and `DatabaseRole`s in the referenced cluster namespaces.
2. Derives the PostgreSQL names. Default database and owner: `<namespace>_<name>` with `-` and
   `.` replaced by `_`. Names must be plain lower-case identifiers; `postgres`, `template0/1`,
   `streaming_replica`, `pg_*` and the cluster's own application database and owner are refused.
3. Tries the grants for that cluster in name order. A grant admits a claim when the claim's
   namespace is in `from` (by name or label selector), database and owner match `namePattern`
   (default `{namespace}_*`), every extension is in `allowedExtensions`, `reclaimPolicy: delete`
   is allowed, the connection limit is within `maxConnectionLimit`, and the namespace has fewer
   than `maxDatabases` admitted claims under that grant.
   Claims in conflict and claims being deleted do not count against `maxDatabases`; the next
   admitted claim in creation order gets a freed slot.
4. Refuses a name that another claim, an unmanaged `Database`/`DatabaseRole` or the cluster
   already uses, across all namespaces. The older claim keeps the name.
5. Creates or updates, in the cluster's namespace, a basic-auth Secret with a random password,
   a `DatabaseRole` (login, no superuser, createdb, createrole, replication or bypassrls) and a
   `Database` owned by that role. All three are named `dbclaim-` plus 16 hex characters of
   SHA-256 over `namespace/name`, so `a-b/c` and `a/b-c` never collide.
6. Once CloudNativePG reports both objects as applied, writes the Secret `<name>-db` (or
   `spec.secretName`) into the claim's namespace with `username`, `password`, `dbname`, `host`,
   `port`, `uri`, `jdbc-uri`, `pgpass`, `ro-host` and `ro-uri`, and sets the claim to `Ready`.

Deleting a claim deletes the `Database`, waits until CloudNativePG has removed it, then deletes
the `DatabaseRole` and the password Secret, then releases the claim's finalizer. Whether the
database and role are dropped in PostgreSQL follows the claim's `reclaimPolicy` (default
`retain`). Revoking a grant does not delete anything: affected claims turn `NotGranted` and say
that the existing database and role are kept. Setting the annotation
`dbclaim.jonathanwrede.de/rotate` to a new value generates a new password.

Claim phases: `Ready`, `Pending` (waiting for CloudNativePG or the cluster), `NotGranted`,
`Conflict`, `Invalid`, `Deleting`. The reason is in `status.conditions[0].message`.

## Installation

Requires CloudNativePG 1.30 or newer (the `DatabaseRole` resource is new in 1.30).

```sh
git clone https://github.com/Jwrede/cnpg-dbclaim && cd cnpg-dbclaim
docker build -t registry.example.com/cnpg-dbclaim:0.1.0 . && docker push registry.example.com/cnpg-dbclaim:0.1.0
cd deploy && kustomize edit set image cnpg-dbclaim=registry.example.com/cnpg-dbclaim:0.1.0 && cd ..
kubectl apply -k deploy/
```

There is no published image. `deploy/` contains the two CRDs, the controller's ServiceAccount,
ClusterRole and Deployment (one replica, no leader election), and a ClusterRole that aggregates
`databaseclaims` into the built-in `admin` and `edit` roles, so namespace admins can create
claims. `DatabaseGrant`s are not aggregated: only whoever may write in the cluster's namespace
can create them.

The controller's ClusterRole allows Secrets in all namespaces (it writes credentials into the
claim namespaces) and CNPG `Database`/`DatabaseRole` objects. With
`-cluster-namespaces=databases` it only reads grants and writes CNPG objects in the listed
namespaces; the `postgresql.cnpg.io` rules can then move into a Role in those namespaces.
Metrics are on `:8080/metrics` (`cnpg_dbclaim_claims{phase=...}`, pass and error counters).

## Example

```yaml
# namespace databases, owned by the database team
apiVersion: dbclaim.jonathanwrede.de/v1alpha1
kind: DatabaseGrant
metadata: {name: app-teams, namespace: databases}
spec:
  cluster: shared
  from:
    - namespace: team-a
    - namespaceSelector: {matchLabels: {shared-postgres: allowed}}
  namePattern: "{namespace}_*"
  maxDatabases: 3
  allowReclaimDelete: false
  allowedExtensions: [pg_trgm, citext]
  maxConnectionLimit: 20
---
# namespace team-a, owned by the application team
apiVersion: dbclaim.jonathanwrede.de/v1alpha1
kind: DatabaseClaim
metadata: {name: orders, namespace: team-a}
spec:
  cluster: {namespace: databases, name: shared}
  extensions: [pg_trgm]
```

The application then uses `team-a/orders-db` like CloudNativePG's app Secret, for example
`DATABASE_URL` from the key `uri`.

Output of the end-to-end test (`e2e/run.sh`, CloudNativePG 1.30.2 on kind):

```
E2E_OUTPUT
```

## Tests

- Unit tests (`go test ./...`): name derivation and every grant limit; the controller against a
  fake Kubernetes API: created objects and their fields, Secret contents, refused claims create
  nothing, name conflicts with unmanaged objects, with the cluster's app database and between
  claims (also after a conflicting Secret disappears), `maxDatabases`, deletion order
  (Database, then role, then password Secret, then finalizer), password rotation, revoked grants,
  orphaned objects, `-cluster-namespaces`.
- End-to-end test (`e2e/run.sh`, in CI against CloudNativePG 1.30.2 and 1.30.1 on kind, and
  every Monday): a shared cluster in `db`, two grants, eight claims from three namespaces. It
  checks that three claims become `Ready` and the other five are refused with the expected
  reason; that a pod in `team-a` connects with its Secret, creates a table, finds `pg_trgm` and
  sees a role without attributes and with connection limit 10; that `team_b_main` can neither
  read nor create tables in `team_a_orders`; that rotation makes the new password work and the
  old one fail; that deleting a `delete` claim drops database and role and a `retain` claim keeps
  them, after which the claim that was in conflict takes over the retained database with its
  data; that revoking a grant keeps the database; and that metrics are served.

## Limits

- PostgreSQL grants `CONNECT` on new databases to `PUBLIC`. Tenants can connect to each other's
  databases and read the catalog (table names), but not table contents (tested). To prevent the
  connection itself, add a `pg_hba` rule to the cluster, for example `host sameuser all all
  scram-sha-256` before a `reject` rule, which works because a claim's owner defaults to the
  database name. This is not tested here.
- A database and role kept by `reclaimPolicy: retain` stay in PostgreSQL with their data. A
  later claim for the same names takes them over and gets a new password. With the default
  pattern `{namespace}_*` only claims from the same namespace can do that; a broader
  `namePattern` such as `*` lets other admitted namespaces take over retained databases.
- Database and owner names cannot change after creation (CloudNativePG treats the database name
  as immutable); the claim turns `Invalid` instead.
- The app Secret is updated as soon as a rotated password is written, a few seconds before
  CloudNativePG has applied it to the role.
- One replica only, no leader election. No admission webhook: invalid claims are reported in
  their status, not rejected by the API server.
- Only `Database` and `DatabaseRole` are handled, not `Publication` or `Subscription`.
- Tested on kind with a single-instance cluster. Not tested with clusters that have replicas,
  with network policies, or on managed Kubernetes.

## License

MIT
