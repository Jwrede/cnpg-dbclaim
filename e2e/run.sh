#!/usr/bin/env bash
# End-to-end test: kind + CloudNativePG + cnpg-dbclaim.
# Needs docker, kind, kubectl. Runs in CI (.github/workflows/ci.yml).
set -euo pipefail
cd "$(dirname "$0")/.."

CNPG_VERSION=${CNPG_VERSION:-1.30.2}
CLUSTER=${KIND_CLUSTER:-cnpg-dbclaim}

log() { printf '\n=== %s\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "PASS: $*"; }
k() { kubectl "$@"; }

if [[ "${KEEP_CLUSTER:-}" == "" ]]; then
  trap 'kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true' EXIT
fi

# phase NS NAME -> status.phase of a claim
phase() { k -n "$1" get databaseclaim "$2" -o jsonpath='{.status.phase}' 2>/dev/null || true; }
message() { k -n "$1" get databaseclaim "$2" -o jsonpath='{.status.conditions[0].message}' 2>/dev/null || true; }
wait_phase() { # NS NAME PHASE [SECONDS]
  local i
  for ((i = 0; i < ${4:-180}; i += 2)); do
    [[ "$(phase "$1" "$2")" == "$3" ]] && return 0
    sleep 2
  done
  k -n "$1" get databaseclaim "$2" -o yaml || true
  fail "claim $1/$2 did not reach $3 (is $(phase "$1" "$2"): $(message "$1" "$2"))"
}
expect_phase() { # NS NAME PHASE MESSAGE-PART
  wait_phase "$1" "$2" "$3" 60
  local m
  m=$(message "$1" "$2")
  [[ "$m" == *"$4"* ]] || fail "claim $1/$2 message '$m' lacks '$4'"
  pass "$1/$2 is $3: $m"
}
secret() { k -n "$1" get secret "$2" -o jsonpath="{.data.$3}" | base64 -d; }
# su_sql SQL -> run as postgres superuser inside the instance
su_sql() { k -n db exec shared-1 -c postgres -- psql -X -tAq -c "$1"; }
# tenant_sql URI SQL -> run over TCP with a tenant's credentials
tenant_sql() { k -n db exec shared-1 -c postgres -- psql -X -tAq "$1" -c "$2"; }

log "kind cluster"
kind create cluster --name "$CLUSTER" --wait 120s

log "CloudNativePG $CNPG_VERSION"
k apply --server-side -f "https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-${CNPG_VERSION%.*}/releases/cnpg-$CNPG_VERSION.yaml"
k -n cnpg-system rollout status deployment/cnpg-controller-manager --timeout=300s

log "cnpg-dbclaim image and deployment"
docker build -q -t cnpg-dbclaim:dev .
kind load docker-image cnpg-dbclaim:dev --name "$CLUSTER"
k apply -k deploy/
k -n cnpg-dbclaim rollout status deployment/cnpg-dbclaim --timeout=180s

log "shared Cluster in namespace db, tenant namespaces"
k create namespace db
k create namespace team-a
k create namespace team-b
k label namespace team-b tenant=b
k create namespace team-c
cat <<YAML | k apply -f -
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: {name: shared, namespace: db}
spec:
  instances: 1
  storage: {size: 1Gi}
YAML
k -n db wait --for=condition=Ready cluster/shared --timeout=600s

log "grants (in db) and claims (in the tenant namespaces)"
cat <<YAML | k apply -f -
apiVersion: dbclaim.jonathanwrede.de/v1alpha1
kind: DatabaseGrant
metadata: {name: team-a, namespace: db}
spec:
  cluster: shared
  from: [{namespace: team-a}]
  allowedExtensions: [pg_trgm]
  maxDatabases: 2
  maxConnectionLimit: 10
---
apiVersion: dbclaim.jonathanwrede.de/v1alpha1
kind: DatabaseGrant
metadata: {name: tenants, namespace: db}
spec:
  cluster: shared
  from: [{namespaceSelector: {matchLabels: {tenant: b}}}]
  allowReclaimDelete: true
YAML
claim() { # NS NAME EXTRA-SPEC
  cat <<YAML | k apply -f -
apiVersion: dbclaim.jonathanwrede.de/v1alpha1
kind: DatabaseClaim
metadata: {name: $2, namespace: $1}
spec:
  cluster: {namespace: db, name: shared}
  $3
YAML
}
claim team-a orders "extensions: [pg_trgm]"
sleep 2 # creation order decides who gets a name first
claim team-b main "reclaimPolicy: delete"
claim team-c x ""
claim team-a steal "database: team_b_x"
claim team-a gis "extensions: [postgis]"
sleep 2
claim team-a dup "database: team_a_orders"
sleep 2
claim team-a three ""
sleep 2
claim team-a four ""

log "admitted claims become Ready"
wait_phase team-a orders Ready
wait_phase team-b main Ready
wait_phase team-a three Ready
k get databaseclaims -A
k -n db get databases.postgresql.cnpg.io,databaseroles.postgresql.cnpg.io

log "refused claims"
expect_phase team-c x NotGranted "does not include namespace team-c"
expect_phase team-a steal NotGranted 'not "team_b_x"'
expect_phase team-a gis NotGranted 'extension "postgis"'
expect_phase team-a dup Conflict "already used by team-a/orders"
expect_phase team-a four NotGranted "allows 2 database(s) for namespace team-a"
n=$(k -n db get databases.postgresql.cnpg.io -l app.kubernetes.io/managed-by=cnpg-dbclaim -o name | wc -l)
[[ "$n" == 3 ]] || fail "expected 3 managed Database objects, found $n"
pass "only the three admitted claims have Database objects"

log "the Secret in team-a works from a pod in team-a"
k -n team-a get secret orders-db -o jsonpath='{.data}' | grep -q '"jdbc-uri"' || fail "jdbc-uri missing"
IMAGE=$(k -n db get cluster shared -o jsonpath='{.status.image}')
cat <<YAML | k apply -f -
apiVersion: v1
kind: Pod
metadata: {name: client, namespace: team-a}
spec:
  restartPolicy: Never
  containers:
    - name: psql
      image: $IMAGE
      command: [sh, -c]
      args:
        - >-
          psql -X -tAq "\$DATABASE_URL" -v ON_ERROR_STOP=1
          -c "create table t (x int); insert into t values (42);"
          -c "select 'rows=' || count(*) from t;"
          -c "select 'ext=' || extname from pg_extension where extname = 'pg_trgm';"
          -c "select 'attrs=' || rolsuper || rolcreatedb || rolcreaterole || rolreplication || rolbypassrls || ' limit=' || rolconnlimit from pg_roles where rolname = current_user;"
      env:
        - name: DATABASE_URL
          valueFrom: {secretKeyRef: {name: orders-db, key: uri}}
YAML
for ((i = 0; i < 90; i++)); do
  p=$(k -n team-a get pod client -o jsonpath='{.status.phase}')
  [[ "$p" == Succeeded || "$p" == Failed ]] && break
  sleep 2
done
out=$(k -n team-a logs client)
echo "$out"
[[ "$p" == Succeeded ]] || fail "client pod $p"
grep -qx 'rows=1' <<<"$out" || fail "insert/select"
grep -qx 'ext=pg_trgm' <<<"$out" || fail "extension pg_trgm missing"
grep -qx 'attrs=falsefalsefalsefalsefalse limit=10' <<<"$out" || fail "role attributes or connection limit"
pass "team-a pod connected with its Secret; pg_trgm present; role has no attributes; limit 10"

log "tenant isolation inside PostgreSQL"
A_URI=$(secret team-a orders-db uri)
B_URI=$(secret team-b main uri)
B_INTO_A=${B_URI%/team_b_main}/team_a_orders
if err=$(tenant_sql "$B_INTO_A" "select * from t" 2>&1); then
  fail "team_b_main could read team_a_orders.t: $err"
fi
grep -q "permission denied" <<<"$err" || fail "unexpected error: $err"
if err=$(tenant_sql "$B_INTO_A" "create table intruder (x int)" 2>&1); then
  fail "team_b_main could create a table in team_a_orders"
fi
pass "team_b_main cannot read or create tables in team_a_orders ($(head -1 <<<"$err"))"
[[ "$(tenant_sql "$A_URI" 'select count(*) from t')" == 1 ]] || fail "team-a lost access"

log "password rotation"
old=$(secret team-a orders-db password)
k -n team-a annotate databaseclaim orders dbclaim.jonathanwrede.de/rotate=1 --overwrite
for ((i = 0; i < 60; i++)); do
  new=$(secret team-a orders-db password)
  [[ "$new" != "$old" ]] && break
  sleep 2
done
[[ "$new" != "$old" ]] || fail "password not rotated"
NEW_URI=$(secret team-a orders-db uri)
for ((i = 0; i < 60; i++)); do
  tenant_sql "$NEW_URI" "select 1" >/dev/null 2>&1 && break
  sleep 2
done
tenant_sql "$NEW_URI" "select 1" >/dev/null || fail "new password does not work"
if tenant_sql "$A_URI" "select 1" >/dev/null 2>&1; then fail "old password still works"; fi
pass "rotated: new password works, old one is rejected"

log "deleting a claim with reclaimPolicy delete drops database and role"
k -n team-b delete databaseclaim main --timeout=180s
[[ -z "$(su_sql "select 1 from pg_database where datname = 'team_b_main'")" ]] || fail "team_b_main still exists"
[[ -z "$(su_sql "select 1 from pg_roles where rolname = 'team_b_main'")" ]] || fail "role team_b_main still exists"
k -n team-b get secret main-db >/dev/null 2>&1 && fail "secret team-b/main-db left behind"
pass "team_b_main dropped, role dropped, Secret gone"

log "deleting a claim with reclaimPolicy retain keeps the data"
k -n team-a delete databaseclaim orders --timeout=180s
[[ "$(su_sql "select 1 from pg_database where datname = 'team_a_orders'")" == 1 ]] || fail "team_a_orders was dropped"
[[ "$(su_sql "select 1 from pg_roles where rolname = 'team_a_orders'")" == 1 ]] || fail "role team_a_orders was dropped"
n=$(k -n db get databases.postgresql.cnpg.io,databaseroles.postgresql.cnpg.io -l dbclaim.jonathanwrede.de/claim-name=orders -o name | wc -l)
[[ "$n" == 0 ]] || fail "CNPG objects of team-a/orders left: $n"
pass "team_a_orders and its role kept in PostgreSQL, CNPG objects removed"

log "revoking the grant keeps existing databases"
k -n db delete databasegrant team-a
expect_phase team-a three NotGranted "existing database and role are kept"
[[ "$(su_sql "select 1 from pg_database where datname = 'team_a_three'")" == 1 ]] || fail "team_a_three dropped after revocation"
pass "team_a_three still exists"

log "metrics"
m=$(k get --raw /api/v1/namespaces/cnpg-dbclaim/services/cnpg-dbclaim:8080/proxy/metrics)
grep -E '^cnpg_dbclaim_(claims|reconcile_passes_total)' <<<"$m"
grep -q 'cnpg_dbclaim_claims{phase="NotGranted"}' <<<"$m" || fail "metrics missing"
pass "metrics served"

k -n cnpg-dbclaim logs deployment/cnpg-dbclaim --tail=40
echo
echo "ALL E2E CHECKS PASSED"
