package dsql

import (
	"errors"
	"os"
	"testing"
)

// Aurora DSQL has no local, Docker or Testcontainers equivalent, so the dktest harness the
// other SQL drivers use cannot cover it. Running the conformance suite against a postgres
// container would be worse than no test: postgres supports both TRUNCATE and advisory
// locks, so it would pass while proving nothing about DSQL.
//
// These tests therefore run against a real cluster and skip when none is configured, the
// same shape as the Aurora DSQL support in Flyway and Entity Framework Core. CI skips them;
// TestEmitsNoUnsupportedSQL and the rest of dsql_test.go are what gate a PR.
//
//	DSQL_CLUSTER_ENDPOINT=mycluster.dsql.us-east-1.on.aws go test ./database/dsql/...
//
// The caller needs AWS credentials on the default provider chain and dsql:DbConnectAdmin
// on the cluster.
const endpointEnvVar = "DSQL_CLUSTER_ENDPOINT"

// dsn returns a connection URL for the configured cluster, skipping the test when there is
// none.
func dsn(t *testing.T) string {
	t.Helper()

	endpoint := os.Getenv(endpointEnvVar)
	if endpoint == "" {
		t.Skipf("%s is not set; skipping the live Aurora DSQL tests", endpointEnvVar)
	}
	return "dsql://admin@" + endpoint + ":5432/postgres"
}

// TestIntegrationOpen covers URL parsing, the connector's credential resolution and pool
// construction. It does not reach the cluster yet: pgxpool connects lazily, IAM tokens are
// generated per connection in BeforeConnect, and WithInstance returns before any Ping. It
// becomes a real end-to-end test once WithInstance pings and the assertion below is
// replaced.
func TestIntegrationOpen(t *testing.T) {
	_, err := (&DSQL{}).Open(dsn(t))

	// TODO: once the driver is implemented, assert a working driver instead and run
	// dt.Test(t, driver, []byte("SELECT 1")) plus dt.TestMigrate over
	// examples/migrations.
	if !errors.Is(err, errNotImplemented) {
		t.Fatalf("Open = %v, want errNotImplemented", err)
	}
}

func TestIntegrationLockIsExclusive(t *testing.T) {
	// TODO: two drivers race Lock; assert exactly one wins and the loser gets
	// database.ErrLocked, then that x-force-lock lets a third take over the stale row.
	// This is the check the conformance suite cannot make: its TestLockAndUnlock only
	// requires the second Lock to return *some* error.
	//
	// Cover both losing shapes, since DSQL adjudicates at COMMIT: a migrator that starts
	// concurrently commits 40001, and one that starts after the winner committed sees the
	// row and gets 23505. Both have to reach the caller as database.ErrLocked, including
	// with retry off, which is x-occ-max-retries=0 and the default.
	t.Skip("pending: Lock is not implemented yet")
}

func TestIntegrationOCCRetry(t *testing.T) {
	// TODO: with x-occ-max-retries set, drive concurrent writers into a real OC000 and
	// assert the retry wrapper recovers rather than surfacing the conflict, then that the
	// default of 0 surfaces it.
	t.Skip("pending: the retried operations are not implemented yet")
}

func TestIntegrationAsyncDDL(t *testing.T) {
	// TODO: a CREATE INDEX ASYNC migration returns only once the build has finished, and a
	// failed build fails the migration. Cover ALTER TABLE ASYNC ... VALIDATE CONSTRAINT
	// the same way. The wait is unconditional, so there is no opt-out to cover.
	//
	// This is also what measures how long sys.wait_for_job blocks against the 60-minute
	// connection cap, and what a failed job looks like from sys.jobs. See runStatement.
	t.Skip("pending: Run is not implemented yet")
}
