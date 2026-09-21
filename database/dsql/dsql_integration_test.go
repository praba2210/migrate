package dsql

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// Aurora DSQL has no local, Docker or Testcontainers image, so these tests run against a real
// cluster and skip when none is configured, the same shape as the Aurora DSQL support in
// Flyway and Entity Framework Core.
//
// They are the third test tier. TestPostgresStandIn covers the lock protocol, the version
// bookkeeping and Drop against a postgres container, which stands in for the protocol rather
// than for compatibility: postgres accepts SQL that DSQL refuses, and has no sys.jobs or
// CREATE INDEX ASYNC. So asynchronous DDL and real OCC conflicts belong here, alongside
// TestEmitsNoUnsupportedSQL. CI skips these; dsql_test.go and TestPostgresStandIn gate a PR.
//
//	DSQL_CLUSTER_ENDPOINT=mycluster.dsql.us-east-1.on.aws go test ./database/dsql/...
//
// The caller needs AWS credentials on the default provider chain and dsql:DbConnectAdmin on
// the cluster.
//
// Every test runs inside one schema, DSQL_TEST_SCHEMA, and each takes its own migrations and
// lock table inside it so they do not read each other's state. The schema has to exist
// already, and it must be a scratch schema: Drop is part of what is under test, and it
// removes every table in the schema it is pointed at. A cluster allows at most 10 schemas, so
// these tests do not create one.
const (
	endpointEnvVar = "DSQL_CLUSTER_ENDPOINT"
	schemaEnvVar   = "DSQL_TEST_SCHEMA"
)

// dsn returns a connection URL for the configured cluster, skipping the test when there is
// none. Options are appended as given.
func dsn(t *testing.T, options ...string) string {
	t.Helper()

	endpoint := os.Getenv(endpointEnvVar)
	if endpoint == "" {
		t.Skipf("%s is not set; skipping the live Aurora DSQL tests", endpointEnvVar)
	}
	rawURL := "dsql://admin@" + endpoint + ":5432/postgres"
	if len(options) > 0 {
		rawURL += "?" + strings.Join(options, "&")
	}
	return rawURL
}

// scratchSchema returns the schema the live tests work in.
func scratchSchema(t *testing.T) string {
	t.Helper()

	schema := os.Getenv(schemaEnvVar)
	if schema == "" {
		t.Skipf("%s is not set; these tests drop every table in the schema they run in, so it has to be named explicitly", schemaEnvVar)
	}
	return schema
}

// testPrefix derives an identifier prefix from the test's own name, so each test's tables are
// its own and a failed run leaves something identifiable behind.
func testPrefix(t *testing.T) string {
	t.Helper()
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			return r
		}
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return '_'
	}, t.Name())
}

// openScratch opens a driver on the scratch schema with tables of this test's own, and drops
// them when the test ends. The driver's own two tables are named after the test, so a test
// never reads a sibling's version row.
func openScratch(t *testing.T, options ...string) *DSQL {
	t.Helper()

	prefix := testPrefix(t)
	options = append(options,
		"x-migrations-schema="+scratchSchema(t),
		"x-migrations-table="+prefix+"_version",
		"x-lock-table="+prefix+"_lock",
	)

	driver, err := (&DSQL{}).Open(dsn(t, options...))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	d := driver.(*DSQL)
	t.Cleanup(func() {
		dropTestTables(t, d, prefix)
		if err := d.Close(); err != nil {
			t.Errorf("closing the driver: %v", err)
		}
	})
	return d
}

// dropTestTables removes what a test created, by prefix, so the scratch schema does not
// accumulate. It reports rather than fails, since it runs after the test's own assertions.
func dropTestTables(t *testing.T, d *DSQL, prefix string) {
	t.Helper()

	ctx := context.Background()
	query := `SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE' AND table_name LIKE $2`
	rows, err := d.db.QueryContext(ctx, query, d.config.SchemaName, prefix+"%")
	if err != nil {
		t.Logf("listing this test's tables to clean up: %v", err)
		return
	}
	names := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			names = append(names, name)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Logf("listing this test's tables to clean up: %v", err)
		return
	}
	for _, name := range names {
		if _, err := d.db.ExecContext(ctx, `DROP TABLE IF EXISTS `+d.qualifiedTable(name)+` CASCADE`); err != nil {
			t.Logf("cleaning up %s: %v", name, err)
		}
	}
}

// sameTablesDriver opens a second driver on the same schema and the same two tables, which is
// what a competing migrator is. Each brings its own pool, so they contend through the lock
// table rather than through a shared connection.
func sameTablesDriver(t *testing.T, existing *DSQL, options ...string) *DSQL {
	t.Helper()

	driver, err := (&DSQL{}).Open(dsn(t, append(options,
		"x-migrations-schema="+existing.config.SchemaName,
		"x-migrations-table="+existing.config.MigrationsTable,
		"x-lock-table="+existing.config.LockTable,
	)...))
	if err != nil {
		t.Fatalf("Open for a competing migrator: %v", err)
	}
	t.Cleanup(func() {
		if err := driver.Close(); err != nil {
			t.Errorf("closing a competing driver: %v", err)
		}
	})
	return driver.(*DSQL)
}

func mustExec(t *testing.T, d *DSQL, statement string) {
	t.Helper()
	if _, err := d.db.ExecContext(context.Background(), statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

// TestIntegrationOpen runs the conformance suite the other drivers run, which covers
// Version, SetVersion, Run, Drop and the lock round-trip.
func TestIntegrationOpen(t *testing.T) {
	driver := openScratch(t)
	dt.Test(t, driver, []byte("SELECT 1"))
}

// TestIntegrationMigrate runs the example migrations end to end, so the version the
// migrator records is checked against a schema it actually applied.
//
// The examples use unqualified names, so search_path is what places them in the scratch
// schema rather than wherever the connection lands.
func TestIntegrationMigrate(t *testing.T) {
	driver := openScratch(t, "search_path="+scratchSchema(t))
	dt.TestMigrate(t, newMigrator(t, driver))
}

// TestIntegrationLockIsExclusive is the check the conformance suite cannot make: its
// TestLockAndUnlock only requires the second Lock to return *some* error, while what matters
// here is that it returns database.ErrLocked.
//
// Both losing shapes are covered, because DSQL adjudicates at COMMIT: a migrator that starts
// concurrently commits 40001, and one that starts after the winner committed sees the row and
// affects no rows. Retry is left at its default of 0 throughout — the forced lock retry is
// what has to carry the first shape.
func TestIntegrationLockIsExclusive(t *testing.T) {
	winner := openScratch(t)

	// Every competing driver is opened before anyone locks. Opening one takes the lock itself,
	// in ensureVersionTable, so a driver opened while another holds it fails there rather than
	// in the Lock under test. cockroachdb behaves the same way, and the postgres drivers do not
	// only because an advisory lock waits instead of reporting.
	const racers = 4
	drivers := make([]*DSQL, racers)
	for i := range drivers {
		drivers[i] = sameTablesDriver(t, winner)
	}
	late := sameTablesDriver(t, winner)
	forcer := sameTablesDriver(t, winner, "x-force-lock=true")

	// The late arrival: the winner's row is already committed and in its snapshot.
	if err := winner.Lock(); err != nil {
		t.Fatalf("the first Lock failed: %v", err)
	}
	if err := late.Lock(); !errors.Is(err, database.ErrLocked) {
		t.Errorf("a migrator arriving after the lock was taken got %v, want database.ErrLocked", err)
	}
	if err := winner.Unlock(); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	var (
		start   = make(chan struct{})
		mu      sync.Mutex
		holders int
		group   sync.WaitGroup
	)
	for _, racer := range drivers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			err := racer.Lock()
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				holders++
			case errors.Is(err, database.ErrLocked):
			default:
				t.Errorf("a racing Lock got %v, want nil or database.ErrLocked", err)
			}
		}()
	}
	close(start)
	group.Wait()

	if holders != 1 {
		t.Errorf("%d of %d racers believe they hold the lock, want exactly 1", holders, racers)
	}

	// Whoever holds it, x-force-lock takes it over, and the row it deleted is the only one
	// there — so the forcing migrator holds the lock afterwards.
	if err := forcer.Lock(); err != nil {
		t.Errorf("x-force-lock could not take over a held lock: %v", err)
	}
	if err := forcer.Unlock(); err != nil {
		t.Errorf("Unlock after a forced lock: %v", err)
	}
}

// TestIntegrationUnlockToleratesADroppedLockTable pins the interaction Drop relies on:
// Migrate calls Unlock after Drop has removed the lock table.
func TestIntegrationUnlockToleratesADroppedLockTable(t *testing.T) {
	driver := openScratch(t)

	if err := driver.Lock(); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := driver.Drop(); err != nil {
		t.Fatalf("Drop: %v", err)
	}
	if err := driver.Unlock(); err != nil {
		t.Errorf("Unlock after Drop removed the lock table = %v, want nil", err)
	}

	// And the version reads as unset rather than failing on the missing table.
	version, dirty, err := driver.Version()
	if err != nil {
		t.Fatalf("Version after Drop: %v", err)
	}
	if version != database.NilVersion || dirty {
		t.Errorf("Version after Drop = (%d, %v), want (%d, false)", version, dirty, database.NilVersion)
	}
}

// TestIntegrationOCCRetry drives concurrent writers into real OCC conflicts on the version
// table and asserts that opting in absorbs them. Eight writers contending over the single row
// SetVersion rewrites is what produces the OC000 the retry exists for; with the default of 0
// this reports "max retries (0) exceeded".
//
// Only the opted-in side is asserted. What the default does with the same load is the
// contrast, and it belongs in a test that can fail on it: the retry counts each setting
// resolves to are pinned in TestRetryAtLeastFloorsTheRetryCount, without needing a conflict to
// arise on any given run.
func TestIntegrationOCCRetry(t *testing.T) {
	const writers = 8

	driver := openScratch(t, "x-occ-max-retries=10", "x-occ-max-retry-delay=200")

	var (
		start = make(chan struct{})
		group sync.WaitGroup
		mu    sync.Mutex
		first error
	)
	for i := range writers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			if err := driver.SetVersion(i, false); err != nil {
				mu.Lock()
				if first == nil {
					first = err
				}
				mu.Unlock()
			}
		}()
	}
	close(start)
	group.Wait()

	if first != nil {
		t.Errorf("a concurrent SetVersion failed with retry opted in: %v", first)
	}

	// The row the writers were contending over is still the one row SetVersion maintains.
	var rows int
	query := `SELECT count(*) FROM ` + driver.qualifiedTable(driver.config.MigrationsTable)
	if err := driver.db.QueryRowContext(context.Background(), query).Scan(&rows); err != nil {
		t.Fatalf("counting version rows: %v", err)
	}
	if rows != 1 {
		t.Errorf("the version table holds %d rows after %d concurrent writers, want 1", rows, writers)
	}
}

// TestIntegrationAsyncDDL is the reason Run waits: CREATE INDEX ASYNC returns as soon as the
// build is enqueued, so without the wait migrate would clear the dirty flag over an index
// that is still building or has failed.
func TestIntegrationAsyncDDL(t *testing.T) {
	driver := openScratch(t)
	prefix := testPrefix(t)
	table := driver.qualifiedTable(prefix + "_users")

	mustExec(t, driver, `CREATE TABLE `+table+` (id INT PRIMARY KEY, email TEXT)`)
	mustExec(t, driver, `INSERT INTO `+table+` VALUES (1, 'a@example.com'), (2, 'b@example.com')`)

	// A build that succeeds: Run returns only once the index is valid.
	if err := driver.Run(strings.NewReader(
		`CREATE INDEX ASYNC ` + prefix + `_email_idx ON ` + table + ` (email)`)); err != nil {
		t.Fatalf("Run on CREATE INDEX ASYNC: %v", err)
	}
	if !indexIsValid(t, driver, prefix+"_email_idx") {
		t.Error("Run returned while the index was still building, so migrate would mark the version clean over an incomplete schema")
	}

	// A build that fails: the job reports failure with no error of its own, so Run has to
	// turn that into one. Without it the migration would be recorded as applied.
	mustExec(t, driver, `INSERT INTO `+table+` VALUES (3, 'a@example.com')`)
	err := driver.Run(strings.NewReader(
		`CREATE UNIQUE INDEX ASYNC ` + prefix + `_email_uniq ON ` + table + ` (email)`))
	if err == nil {
		t.Fatal("Run succeeded on a unique index that cannot be built over duplicate rows")
	}
	if !strings.Contains(err.Error(), "duplicate key") {
		t.Errorf("Run reported %q, which does not carry the reason the job failed", err)
	}

	// The failed build leaves an INVALID index behind that goes on enforcing uniqueness, so
	// the error has to be loud enough for an operator to go and drop it.
	if indexIsValid(t, driver, prefix+"_email_uniq") {
		t.Error("the failed index reports itself valid")
	}
	mustExec(t, driver, `DROP INDEX IF EXISTS `+driver.qualifiedTable(prefix+"_email_uniq"))
}

// TestIntegrationAsyncConstraintValidation covers the second statement that returns a job_id.
// It is the reason the wait cannot be special-cased to indexes: a validation that fails leaves
// the constraint unvalidated, and without the wait migrate would record the migration as
// applied.
//
// Note which spellings the engine accepts, since the pair is not symmetric: the constraint is
// added with a plain ALTER TABLE ... NOT VALID, and only the validation takes ASYNC. Adding a
// constraint with ALTER TABLE ASYNC, and validating without it, are both rejected.
func TestIntegrationAsyncConstraintValidation(t *testing.T) {
	driver := openScratch(t)
	prefix := testPrefix(t)
	table := driver.qualifiedTable(prefix + "_orders")
	constraint := prefix + "_amount_check"

	mustExec(t, driver, `CREATE TABLE `+table+` (id INT PRIMARY KEY, amount INT)`)
	mustExec(t, driver, `INSERT INTO `+table+` VALUES (1, 5), (2, 10)`)
	mustExec(t, driver, `ALTER TABLE `+table+` ADD CONSTRAINT `+constraint+` CHECK (amount > 0) NOT VALID`)

	// A validation that passes: Run returns only once the job has finished, so the constraint
	// is validated by then.
	if err := driver.Run(strings.NewReader(
		`ALTER TABLE ASYNC ` + table + ` VALIDATE CONSTRAINT ` + constraint)); err != nil {
		t.Fatalf("Run on ALTER TABLE ASYNC ... VALIDATE CONSTRAINT: %v", err)
	}
	if !constraintIsValidated(t, driver, constraint) {
		t.Error("Run returned while the constraint was still being validated, so migrate would mark the version clean over an unvalidated constraint")
	}

	// A validation that fails, on a row the constraint rejects. The job reports failure with
	// no error of its own, so Run has to turn that into one.
	violating := prefix + "_violating"
	violatingTable := driver.qualifiedTable(violating)
	mustExec(t, driver, `CREATE TABLE `+violatingTable+` (id INT PRIMARY KEY, amount INT)`)
	mustExec(t, driver, `INSERT INTO `+violatingTable+` VALUES (1, 5), (2, -3)`)
	mustExec(t, driver, `ALTER TABLE `+violatingTable+` ADD CONSTRAINT `+violating+`_chk CHECK (amount > 0) NOT VALID`)

	err := driver.Run(strings.NewReader(
		`ALTER TABLE ASYNC ` + violatingTable + ` VALIDATE CONSTRAINT ` + violating + `_chk`))
	if err == nil {
		t.Fatal("Run succeeded validating a constraint that a row violates")
	}
	if !strings.Contains(err.Error(), "violated") {
		t.Errorf("Run reported %q, which does not carry the reason the job failed", err)
	}
	if constraintIsValidated(t, driver, violating+"_chk") {
		t.Error("the failed validation reports the constraint as validated")
	}
}

// TestIntegrationSearchPath covers the pool hook: a search_path in the URL reaches every
// connection, and CURRENT_SCHEMA() resolves from it, which is what leaves
// x-migrations-schema an override rather than the only way to place the two tables.
func TestIntegrationSearchPath(t *testing.T) {
	schema := scratchSchema(t)
	prefix := testPrefix(t)

	// No x-migrations-schema: where the two tables land has to come from search_path alone.
	driver, err := (&DSQL{}).Open(dsn(t,
		"search_path="+schema,
		"x-migrations-table="+prefix+"_version",
		"x-lock-table="+prefix+"_lock",
	))
	if err != nil {
		t.Fatalf("Open with search_path: %v", err)
	}
	d := driver.(*DSQL)
	t.Cleanup(func() {
		dropTestTables(t, d, prefix)
		if err := d.Close(); err != nil {
			t.Errorf("closing the driver: %v", err)
		}
	})

	if d.config.SchemaName != schema {
		t.Errorf("CURRENT_SCHEMA() resolved to %q, want %q from search_path", d.config.SchemaName, schema)
	}

	// And the tables landed there rather than in the connection's default schema.
	var count int
	query := `SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name IN ($2, $3)`
	if err := d.db.QueryRowContext(context.Background(), query, schema, prefix+"_version", prefix+"_lock").Scan(&count); err != nil {
		t.Fatalf("counting the driver's tables: %v", err)
	}
	if count != 2 {
		t.Errorf("found %d of the driver's 2 tables in %q", count, schema)
	}
}

// TestIntegrationStatementTimeout covers x-statement-timeout, which is applied per statement
// rather than by the server.
func TestIntegrationStatementTimeout(t *testing.T) {
	driver := openScratch(t, "x-statement-timeout=1")

	err := driver.Run(strings.NewReader(`SELECT pg_sleep(5)`))
	if err == nil {
		t.Fatal("a 5s statement completed under a 1ms statement timeout")
	}
	// Matched on the message rather than with errors.Is: the deadline is inside the
	// database.Error naming the failing migration, and that type has no Unwrap. Same property
	// retry() depends on, from the other side.
	if !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Errorf("Run reported %q, which does not say the statement ran out of time", err)
	}
}

// TestIntegrationPasswordIsRejected keeps the URL contract honest against the connector: it
// discards a password and authenticates with IAM, so accepting one would be a lie.
func TestIntegrationPasswordIsRejected(t *testing.T) {
	endpoint := os.Getenv(endpointEnvVar)
	if endpoint == "" {
		t.Skipf("%s is not set; skipping the live Aurora DSQL tests", endpointEnvVar)
	}

	for _, rawURL := range []string{
		"dsql://admin:hunter2@" + endpoint + ":5432/postgres",
		"dsql://admin@" + endpoint + ":5432/postgres?password=hunter2",
	} {
		if _, err := (&DSQL{}).Open(rawURL); !errors.Is(err, ErrPasswordSet) {
			t.Errorf("Open(%q) = %v, want ErrPasswordSet", rawURL, err)
		}
	}
}

// indexIsValid reports pg_index.indisvalid, which is false while a build is running and stays
// false after one fails.
func indexIsValid(t *testing.T, d *DSQL, indexName string) bool {
	t.Helper()
	var valid sql.NullBool
	query := `SELECT indisvalid FROM pg_index JOIN pg_class ON pg_class.oid = indexrelid WHERE relname = $1`
	if err := d.db.QueryRowContext(context.Background(), query, indexName).Scan(&valid); err != nil {
		t.Fatalf("reading the state of index %s: %v", indexName, err)
	}
	return valid.Bool
}

// constraintIsValidated reports pg_constraint.convalidated, which stays false for a constraint
// added NOT VALID until a validation job succeeds.
func constraintIsValidated(t *testing.T, d *DSQL, constraintName string) bool {
	t.Helper()
	var validated sql.NullBool
	query := `SELECT convalidated FROM pg_constraint WHERE conname = $1`
	if err := d.db.QueryRowContext(context.Background(), query, constraintName).Scan(&validated); err != nil {
		t.Fatalf("reading the state of constraint %s: %v", constraintName, err)
	}
	return validated.Bool
}

// newMigrator wires the driver to the example migrations for dt.TestMigrate.
func newMigrator(t *testing.T, d *DSQL) *migrate.Migrate {
	t.Helper()
	m, err := migrate.NewWithDatabaseInstance("file://examples/migrations", d.config.DatabaseName, d)
	if err != nil {
		t.Fatalf("NewWithDatabaseInstance: %v", err)
	}
	// The examples build an index, so give the lock and the jobs room.
	m.LockTimeout = 30 * time.Second
	return m
}
