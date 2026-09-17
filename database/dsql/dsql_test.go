package dsql

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	nurl "net/url"
	"strings"
	"testing"
	"time"

	awsdsql "github.com/awslabs/aurora-dsql-connectors/go/pgx/dsql"
	"github.com/awslabs/aurora-dsql-connectors/go/pgx/occretry"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	testSchema   = "public"
	testDatabase = "postgres"
)

func mustParseConfig(t *testing.T, rawURL string) *Config {
	t.Helper()
	purl, err := nurl.Parse(rawURL)
	if err != nil {
		t.Fatalf("parsing %q: %v", rawURL, err)
	}
	config, err := parseConfig(purl)
	if err != nil {
		t.Fatalf("parseConfig(%q): %v", rawURL, err)
	}
	return config
}

func TestParseConfigDefaults(t *testing.T) {
	config := mustParseConfig(t, "dsql://admin@cluster.dsql.us-east-1.on.aws:5432/postgres")

	if config.DatabaseName != testDatabase {
		t.Errorf("DatabaseName = %q, want postgres", config.DatabaseName)
	}
	if config.MigrationsTable != "" {
		t.Errorf("MigrationsTable = %q, want empty before setDefaults", config.MigrationsTable)
	}
	// Left empty so WithInstance can fall back to CURRENT_SCHEMA().
	if config.SchemaName != "" {
		t.Errorf("SchemaName = %q, want empty when x-migrations-schema is absent", config.SchemaName)
	}
	if config.ForceLock {
		t.Error("ForceLock = true, want false")
	}
	if config.MultiStatementEnabled {
		t.Error("MultiStatementEnabled = true, want false")
	}
	if config.StatementTimeout != 0 {
		t.Errorf("StatementTimeout = %v, want 0", config.StatementTimeout)
	}
	if config.MultiStatementMaxSize != DefaultMultiStatementMaxSize {
		t.Errorf("MultiStatementMaxSize = %d, want %d", config.MultiStatementMaxSize, DefaultMultiStatementMaxSize)
	}
	// Retry is opt-in: the driver imposes no count of its own.
	if config.OCCMaxRetries != 0 {
		t.Errorf("OCCMaxRetries = %d, want 0 when x-occ-max-retries is absent", config.OCCMaxRetries)
	}
	if config.OCCMaxRetryDelay != DefaultOCCMaxRetryDelay {
		t.Errorf("OCCMaxRetryDelay = %v, want %v", config.OCCMaxRetryDelay, DefaultOCCMaxRetryDelay)
	}
}

func TestParseConfigAllOptions(t *testing.T) {
	config := mustParseConfig(t, "dsql://admin@cluster.dsql.us-east-1.on.aws/mydb?"+
		"x-migrations-schema=app&x-migrations-table=mt&x-lock-table=lt&x-force-lock=true&"+
		"x-statement-timeout=250&"+
		"x-multi-statement=true&x-multi-statement-max-size=4096&"+
		"x-occ-max-retries=7&x-occ-max-retry-delay=1500&search_path=app")

	// search_path has no Config field: Open applies it to the pool, so all this pins is
	// that parseConfig accepts it. It used to be rejected.

	if config.DatabaseName != "mydb" {
		t.Errorf("DatabaseName = %q, want mydb", config.DatabaseName)
	}
	if config.SchemaName != "app" {
		t.Errorf("SchemaName = %q, want app", config.SchemaName)
	}
	if config.MigrationsTable != "mt" {
		t.Errorf("MigrationsTable = %q, want mt", config.MigrationsTable)
	}
	if config.LockTable != "lt" {
		t.Errorf("LockTable = %q, want lt", config.LockTable)
	}
	if !config.ForceLock {
		t.Error("ForceLock = false, want true")
	}
	if config.StatementTimeout != 250*time.Millisecond {
		t.Errorf("StatementTimeout = %v, want 250ms", config.StatementTimeout)
	}
	if !config.MultiStatementEnabled {
		t.Error("MultiStatementEnabled = false, want true")
	}
	if config.MultiStatementMaxSize != 4096 {
		t.Errorf("MultiStatementMaxSize = %d, want 4096", config.MultiStatementMaxSize)
	}
	if config.OCCMaxRetries != 7 {
		t.Errorf("OCCMaxRetries = %d, want 7", config.OCCMaxRetries)
	}
	if config.OCCMaxRetryDelay != 1500*time.Millisecond {
		t.Errorf("OCCMaxRetryDelay = %v, want 1.5s", config.OCCMaxRetryDelay)
	}
}

// A malformed option must fail loudly rather than fall back to a default, per
// database/driver.go: "Don't try to correct user input. Don't assume things."
func TestParseConfigRejectsBadOptions(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantText string
	}{
		{"statement timeout", "x-statement-timeout=soon", "x-statement-timeout"},
		{"multi statement max size", "x-multi-statement-max-size=big", "x-multi-statement-max-size"},
		{"occ max retries", "x-occ-max-retries=lots", "x-occ-max-retries"},
		{"occ max retry delay", "x-occ-max-retry-delay=later", "x-occ-max-retry-delay"},
		{"force lock", "x-force-lock=maybe", "x-force-lock"},
		{"multi statement", "x-multi-statement=maybe", "x-multi-statement"},
		{"negative occ retries", "x-occ-max-retries=-1", "x-occ-max-retries"},
		{"negative occ retry delay", "x-occ-max-retry-delay=-1", "x-occ-max-retry-delay"},

		// Silently ignored is worse than refused: the operator believes the setting took.
		// This spelling carries arbitrary -c flags alongside the schema list, and it is the
		// one the Aurora DSQL ORM integrations tell users to write, so the error names the
		// spelling this driver applies.
		{"options search_path", "options=-c%20search_path%3Dapp", "search_path=app"},
		{"migrations table quoted", "x-migrations-table-quoted=true", "x-migrations-table-quoted"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			purl, err := nurl.Parse("dsql://admin@host/postgres?" + test.query)
			if err != nil {
				t.Fatalf("parsing URL: %v", err)
			}
			_, err = parseConfig(purl)
			if err == nil {
				t.Fatalf("parseConfig(%q) succeeded, want an error", test.query)
			}
			if !strings.Contains(err.Error(), test.wantText) {
				t.Errorf("error %q does not name the offending option %q", err, test.wantText)
			}
		})
	}
}

// A non-positive size means "unset", not "zero-length buffer".
func TestParseConfigNonPositiveMaxSizeFallsBack(t *testing.T) {
	for _, value := range []string{"0", "-1"} {
		config := mustParseConfig(t, "dsql://admin@host/postgres?x-multi-statement-max-size="+value)
		if config.MultiStatementMaxSize != DefaultMultiStatementMaxSize {
			t.Errorf("size %s: got %d, want the default %d", value, config.MultiStatementMaxSize, DefaultMultiStatementMaxSize)
		}
	}
}

func TestConfigSetDefaults(t *testing.T) {
	config := &Config{}
	config.setDefaults()

	if config.MigrationsTable != DefaultMigrationsTable {
		t.Errorf("MigrationsTable = %q, want %q", config.MigrationsTable, DefaultMigrationsTable)
	}
	if config.LockTable != DefaultLockTable {
		t.Errorf("LockTable = %q, want %q", config.LockTable, DefaultLockTable)
	}
	if config.MultiStatementMaxSize != DefaultMultiStatementMaxSize {
		t.Errorf("MultiStatementMaxSize = %d, want %d", config.MultiStatementMaxSize, DefaultMultiStatementMaxSize)
	}
	if config.OCCMaxRetryDelay != DefaultOCCMaxRetryDelay {
		t.Errorf("OCCMaxRetryDelay = %v, want %v", config.OCCMaxRetryDelay, DefaultOCCMaxRetryDelay)
	}

	// OCCMaxRetries is left alone: zero is the connector's "no retry" and defaulting it
	// here would make that value unreachable from a Config literal.
	if config.OCCMaxRetries != 0 {
		t.Errorf("OCCMaxRetries = %d, want 0 left untouched", config.OCCMaxRetries)
	}
}

func TestOCCConfigMapping(t *testing.T) {
	config := &Config{OCCMaxRetries: 9, OCCMaxRetryDelay: 3 * time.Second}
	occ := config.occConfig()

	if occ.MaxRetries != 9 {
		t.Errorf("MaxRetries = %d, want 9", occ.MaxRetries)
	}
	if occ.MaxWait != 3*time.Second {
		t.Errorf("MaxWait = %v, want 3s", occ.MaxWait)
	}
	// Unset fields keep the connector's defaults rather than becoming zero.
	if occ.InitialWait <= 0 {
		t.Errorf("InitialWait = %v, want the connector default", occ.InitialWait)
	}
	if occ.Multiplier <= 1 {
		t.Errorf("Multiplier = %v, want the connector default", occ.Multiplier)
	}
}

// The retry count is the connector's value, not a reinterpretation of it, and zero is both
// the driver's default and occretry's "run once". So retry is opt-in and the two entry
// points cannot disagree, which is the thing a non-zero default could not deliver.
func TestOCCMaxRetriesIsPassedThrough(t *testing.T) {
	tests := []struct {
		query string
		want  int
	}{
		{"", 0},
		{"?x-occ-max-retries=", 0},
		{"?x-occ-max-retries=0", 0},
		{"?x-occ-max-retries=7", 7},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			config := mustParseConfig(t, "dsql://admin@host/postgres"+test.query)
			config.setDefaults()
			if got := config.occConfig().MaxRetries; got != test.want {
				t.Errorf("MaxRetries = %d, want %d", got, test.want)
			}
		})
	}

	// A bare URL and a bare Config literal must resolve identically. They did not when the
	// URL defaulted to 3: a Config literal's zero is indistinguishable from "unset".
	fromURL := mustParseConfig(t, "dsql://admin@host/postgres")
	fromURL.setDefaults()
	literal := &Config{}
	literal.setDefaults()
	if got, want := literal.occConfig().MaxRetries, fromURL.occConfig().MaxRetries; got != want {
		t.Errorf("&Config{} gives MaxRetries = %d, but a bare URL gives %d", got, want)
	}
}

// occretry has no normalization of its own, so occConfig has to seed from DefaultConfig or
// Multiplier and MaxWait arrive as zero. MaxWait additionally needs a floor: the connector
// clamps each wait to it and computes jitter as rand.Int63n(wait/4), which panics below 4ns.
func TestOCCConfigKeepsConnectorSafe(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
	}{
		{"unset", &Config{}},
		{"negative delay", &Config{OCCMaxRetryDelay: -time.Second}},
		{"sub-nanosecond delay", &Config{OCCMaxRetryDelay: 3}},
		{"one nanosecond delay", &Config{OCCMaxRetries: 5, OCCMaxRetryDelay: 1}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.config.setDefaults()
			occ := test.config.occConfig()

			if occ.Multiplier <= 1 {
				t.Errorf("Multiplier = %v, want the connector default", occ.Multiplier)
			}
			if occ.MaxWait < minOCCRetryDelay {
				t.Errorf("MaxWait = %v, want at least %v to keep the connector's jitter safe", occ.MaxWait, minOCCRetryDelay)
			}
			if occ.InitialWait > occ.MaxWait {
				t.Errorf("InitialWait = %v exceeds MaxWait = %v, so the first backoff ignores the bound", occ.InitialWait, occ.MaxWait)
			}
			if occ.InitialWait <= 0 {
				t.Errorf("InitialWait = %v, want a positive seed", occ.InitialWait)
			}
		})
	}
}

// A password is rejected, not ignored. The connector discards one and authenticates with
// IAM, so accepting it would leave an operator believing a credential was in use.
func TestParseConfigRejectsPassword(t *testing.T) {
	rejected := []string{
		"dsql://admin:hunter2@cluster.dsql.us-east-1.on.aws:5432/postgres",
		"dsql://admin:@cluster.dsql.us-east-1.on.aws/postgres", // present but empty
		"dsql://:hunter2@cluster.dsql.us-east-1.on.aws/postgres",
	}
	for _, rawURL := range rejected {
		t.Run(rawURL, func(t *testing.T) {
			purl, err := nurl.Parse(rawURL)
			if err != nil {
				t.Fatalf("parsing %q: %v", rawURL, err)
			}
			if _, err := parseConfig(purl); !errors.Is(err, ErrPasswordSet) {
				t.Errorf("parseConfig(%q) = %v, want ErrPasswordSet", rawURL, err)
			}
		})
	}

	accepted := []string{
		"dsql://admin@cluster.dsql.us-east-1.on.aws:5432/postgres",
		"dsql://cluster.dsql.us-east-1.on.aws/postgres", // no userinfo at all
	}
	for _, rawURL := range accepted {
		t.Run(rawURL, func(t *testing.T) {
			purl, err := nurl.Parse(rawURL)
			if err != nil {
				t.Fatalf("parsing %q: %v", rawURL, err)
			}
			if _, err := parseConfig(purl); err != nil {
				t.Errorf("parseConfig(%q) = %v, want success", rawURL, err)
			}
		})
	}
}

// database.Error has no Unwrap, so anything wrapped in one is invisible to errors.As and
// errors.Is. That is why retry() takes the raw error and migrate's context is added only
// after it returns — see retry(). Left as a test because the trap is silent: wrapping in
// the wrong order degrades retry to a single attempt without any symptom.
func TestDatabaseErrorHidesItsCause(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "OC001", Message: "schema has been updated by another transaction"}

	for _, wrapped := range []error{
		database.Error{OrigErr: pgErr},
		&database.Error{OrigErr: pgErr},
	} {
		var found *pgconn.PgError
		if errors.As(wrapped, &found) {
			t.Errorf("%T now unwraps; retry() may stop taking the raw error", wrapped)
		}
		if occretry.IsOCCError(wrapped) {
			t.Errorf("%T: the connector can now classify a wrapped error", wrapped)
		}
	}

	// Unwrapped, the same error is retryable — the ordering is the whole difference.
	if !occretry.IsOCCError(pgErr) {
		t.Error("a bare OC001 is not classified as an OCC error")
	}
}

// The hook writes the raw value into RuntimeParams, which is where pgx keeps a search_path
// from a URL, so the server parses it as it would through postgres or pgx: APP resolves app,
// "App" keeps its case, a list stays a list, and quoting stays the operator's to decide.
func TestSearchPathHookPreservesRawValue(t *testing.T) {
	for _, searchPath := range []string{"app", "APP", `"App", public`, "app, public", "$user, public"} {
		poolConfig, err := poolConfigFor(searchPath)
		if err != nil {
			t.Fatalf("poolConfigFor(%q): %v", searchPath, err)
		}
		if poolConfig.BeforeConnect == nil {
			t.Fatalf("BeforeConnect is nil for %q, so search_path would never reach the server", searchPath)
		}

		// A copy, as pgxpool passes the hook.
		connConfig := poolConfig.ConnConfig.Copy()
		if err := poolConfig.BeforeConnect(context.Background(), connConfig); err != nil {
			t.Fatalf("BeforeConnect(%q): %v", searchPath, err)
		}
		if got := connConfig.RuntimeParams["search_path"]; got != searchPath {
			t.Errorf("RuntimeParams[search_path] = %q, want %q verbatim", got, searchPath)
		}
	}
}

// Supplying a pool config turns off the connector's own defaults: it fills the two lifetimes
// only when they are zero, and pgxpool.ParseConfig has already set them to 1h and 30m. An
// hour is exactly when DSQL closes a connection server-side, so leaving them would hand out
// connections the server is about to drop. Asserted because nothing else would notice: the
// pool still works, it just stops recycling ahead of the server.
func TestPoolConfigPinsConnectorLifetimes(t *testing.T) {
	poolConfig, err := poolConfigFor("")
	if err != nil {
		t.Fatalf("poolConfigFor: %v", err)
	}

	if poolConfig.MaxConnLifetime != awsdsql.DefaultMaxConnLifetime {
		t.Errorf("MaxConnLifetime = %v, want the connector's %v",
			poolConfig.MaxConnLifetime, awsdsql.DefaultMaxConnLifetime)
	}
	if poolConfig.MaxConnIdleTime != awsdsql.DefaultMaxConnIdleTime {
		t.Errorf("MaxConnIdleTime = %v, want the connector's %v",
			poolConfig.MaxConnIdleTime, awsdsql.DefaultMaxConnIdleTime)
	}

	// The hook appears only for a URL that carries a search_path.
	if poolConfig.BeforeConnect != nil {
		t.Error("BeforeConnect is set with no search_path in the URL")
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"users", `"users"`},
		{"user table", `"user table"`},
		{`we"ird`, `"we""ird"`},
		{"trunc\x00ated", `"trunc"`},
		{"", `""`},
	}

	for _, test := range tests {
		if got := quoteIdentifier(test.in); got != test.want {
			t.Errorf("quoteIdentifier(%q) = %s, want %s", test.in, got, test.want)
		}
	}
}

func TestQualifiedTable(t *testing.T) {
	d := &DSQL{config: &Config{SchemaName: testSchema}}
	if got, want := d.qualifiedTable("schema_migrations"), `"public"."schema_migrations"`; got != want {
		t.Errorf("qualifiedTable = %s, want %s", got, want)
	}

	// Qualifying is what covers WithInstance, whose caller supplies the pool: search_path
	// is applied by Open only. x-migrations-schema overrides whatever the connection
	// resolves to, and places this driver's own two tables.
	nonAdmin := &DSQL{config: mustParseConfig(t, "dsql://app_user@host/postgres?x-migrations-schema=app")}
	if got, want := nonAdmin.qualifiedTable("schema_migrations"), `"app"."schema_migrations"`; got != want {
		t.Errorf("qualifiedTable with x-migrations-schema = %s, want %s", got, want)
	}
}

// Every name that decides which lock a migrator is contending for has to be part of the
// key, or two migrators can hold what they each believe is the only lock.
func TestLockID(t *testing.T) {
	d := &DSQL{config: &Config{
		DatabaseName:    testDatabase,
		SchemaName:      testSchema,
		MigrationsTable: DefaultMigrationsTable,
		LockTable:       DefaultLockTable,
	}}

	first, err := d.lockID()
	if err != nil {
		t.Fatalf("lockID: %v", err)
	}
	if first == "" {
		t.Fatal("lockID is empty")
	}

	second, err := d.lockID()
	if err != nil {
		t.Fatalf("lockID second call: %v", err)
	}
	if first != second {
		t.Errorf("lockID is not stable: %q then %q", first, second)
	}

	// Each name is load-bearing on its own. The lock table especially: two migrators
	// agreeing on schema and migrations table but writing to different lock tables would
	// otherwise share a key and exclude nobody.
	for _, other := range []struct {
		name   string
		config *Config
	}{
		{"schema", &Config{DatabaseName: testDatabase, SchemaName: "elsewhere", MigrationsTable: DefaultMigrationsTable, LockTable: DefaultLockTable}},
		{"migrations table", &Config{DatabaseName: testDatabase, SchemaName: testSchema, MigrationsTable: "versions", LockTable: DefaultLockTable}},
		{"lock table", &Config{DatabaseName: testDatabase, SchemaName: testSchema, MigrationsTable: DefaultMigrationsTable, LockTable: "mutex"}},
		{"database", &Config{DatabaseName: "another", SchemaName: testSchema, MigrationsTable: DefaultMigrationsTable, LockTable: DefaultLockTable}},
	} {
		t.Run(other.name, func(t *testing.T) {
			otherID, err := (&DSQL{config: other.config}).lockID()
			if err != nil {
				t.Fatalf("lockID: %v", err)
			}
			if otherID == first {
				t.Errorf("a different %s produced the same lock ID %q", other.name, otherID)
			}
		})
	}
}

func TestIsLockConflict(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"unique violation", &pgconn.PgError{Code: pgerrcode.UniqueViolation}, true},
		{"wrapped unique violation", errors.Join(errors.New("ctx"), &pgconn.PgError{Code: pgerrcode.UniqueViolation}), true},
		// DSQL's OCC failures are retryable, not lock-held. OC001 fires with no
		// contention at all, so calling one a lock conflict would tell an operator to
		// wait for a lock nobody holds. retry() handles these instead.
		{"serialization failure", &pgconn.PgError{Code: pgerrcode.SerializationFailure}, false},
		{"occ mutation conflict", &pgconn.PgError{Code: "OC000"}, false},
		{"occ schema conflict", &pgconn.PgError{Code: "OC001"}, false},
		{"undefined table", &pgconn.PgError{Code: pgerrcode.UndefinedTable}, false},
		{"plain error", errors.New("nope"), false},
		{"nil", nil, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isLockConflict(test.err); got != test.want {
				t.Errorf("isLockConflict(%v) = %v, want %v", test.err, got, test.want)
			}
		})
	}
}

func TestIsUndefinedTable(t *testing.T) {
	if !isUndefinedTable(&pgconn.PgError{Code: pgerrcode.UndefinedTable}) {
		t.Error("undefined table not recognized")
	}
	if isUndefinedTable(&pgconn.PgError{Code: pgerrcode.UniqueViolation}) {
		t.Error("unique violation misreported as undefined table")
	}
	if isUndefinedTable(errors.New("nope")) {
		t.Error("plain error misreported as undefined table")
	}
}

func TestWithInstanceRejectsNilConfig(t *testing.T) {
	if _, err := WithInstance(nil, nil); !errors.Is(err, ErrNilConfig) {
		t.Errorf("WithInstance(nil, nil) = %v, want ErrNilConfig", err)
	}
}

// Both paths hold OCCMaxRetries to zero or more, the range occretry's attempt <= MaxRetries
// loop runs over: parseConfig checks the URL option, validate checks a Config literal.
func TestWithInstanceRejectsNegativeOCCMaxRetries(t *testing.T) {
	_, err := WithInstance(nil, &Config{OCCMaxRetries: -1})
	if err == nil {
		t.Fatal("WithInstance accepted OCCMaxRetries = -1")
	}
	if !strings.Contains(err.Error(), "OCCMaxRetries") {
		t.Errorf("error %q does not name the offending field", err)
	}
}

// Every method still to be written must fail loudly. This also pins which parts of the
// skeleton are outstanding, so finishing one means deleting a line here.
func TestNotImplemented(t *testing.T) {
	newDriver := func() *DSQL {
		config := &Config{DatabaseName: testDatabase, SchemaName: testSchema}
		config.setDefaults()
		return &DSQL{config: config}
	}
	ctx := context.Background()

	tests := []struct {
		name string
		call func(*DSQL) error
	}{
		{"Lock", func(d *DSQL) error { return d.Lock() }},
		{"Unlock", func(d *DSQL) error { d.isLocked.Store(true); return d.Unlock() }},
		{"Run", func(d *DSQL) error { return d.Run(strings.NewReader("SELECT 1")) }},
		{"runStatement", func(d *DSQL) error { return d.runStatement(ctx, []byte("SELECT 1")) }},
		{"awaitAsyncJob", func(d *DSQL) error { return d.awaitAsyncJob(ctx, "jh2gbtx4mzhgfkbimtgwn5j45y") }},
		{"SetVersion", func(d *DSQL) error { return d.SetVersion(1, false) }},
		{"Version", func(d *DSQL) error { _, _, err := d.Version(); return err }},
		{"Drop", func(d *DSQL) error { return d.Drop() }},
		{"ensureVersionTable", func(d *DSQL) error { return d.ensureVersionTable() }},
		{"ensureLockTable", func(d *DSQL) error { return d.ensureLockTable() }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(newDriver()); !errors.Is(err, errNotImplemented) {
				t.Errorf("%s = %v, want errNotImplemented", test.name, err)
			}
		})
	}
}

// Close must tolerate a driver that never opened anything.
func TestCloseWithoutDB(t *testing.T) {
	if err := (&DSQL{}).Close(); err != nil {
		t.Errorf("Close on a zero driver = %v, want nil", err)
	}
}

// The conformance suite in database/testing cannot catch a regression here: it runs
// against a postgres container, which supports both TRUNCATE and advisory locks, and
// TestLockAndUnlock only requires the second Lock to return *some* error. So assert
// directly that the SQL this driver emits avoids what DSQL does not implement.
//
// Only string literals are inspected, so the prose explaining why these are absent does
// not trip the check.
func TestEmitsNoUnsupportedSQL(t *testing.T) {
	banned := []string{"truncate", "pg_advisory_lock", "pg_advisory_unlock", "pg_try_advisory_lock"}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "dsql.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parsing dsql.go: %v", err)
	}

	ast.Inspect(file, func(node ast.Node) bool {
		lit, ok := node.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		value := strings.ToLower(lit.Value)
		for _, word := range banned {
			if strings.Contains(value, word) {
				t.Errorf("%s: SQL contains %q, which Aurora DSQL does not support: %s",
					fset.Position(lit.Pos()), word, lit.Value)
			}
		}
		return true
	})
}
