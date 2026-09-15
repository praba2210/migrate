// Package dsql implements the database.Driver interface for Amazon Aurora DSQL.
//
// Aurora DSQL speaks PostgreSQL's wire protocol, but two of its behaviors make the
// existing postgres and pgx drivers unusable against it:
//
//   - pg_advisory_lock and pg_advisory_unlock are unsupported (SQLSTATE 0A000), so the
//     migration lock is a row in a table.
//   - TRUNCATE is unsupported, so SetVersion clears the version table with a DELETE that
//     has no WHERE clause. Semantics are identical: the table holds one row.
//
// Separately, DSQL permits at most one DDL statement per transaction and no mixing of DDL
// with DML. Neither postgres nor pgx wraps a migration file in an explicit transaction
// either, so that is a constraint on how migrations are written rather than a difference
// in the driver. See the README.
//
// Every table this driver touches is schema-qualified from x-migrations-schema or
// CURRENT_SCHEMA(). Setting search_path would also work for the Open path, since pgxpool
// runs its per-connection hooks before a connection joins the pool, but it cannot cover
// WithInstance, whose caller supplies a pool this driver never configures. Qualifying
// covers both entry points and matches what the postgres and pgx drivers do.
//
// IAM authentication and optimistic-concurrency retry are delegated to the AWS connector
// (github.com/awslabs/aurora-dsql-connectors/go/pgx) rather than reimplemented here.
// Open builds its pool, so a DSQL URL carries no password: a token is generated per
// connection. WithInstance does not resolve credentials or build connections, so callers
// who bring their own *sql.DB are unaffected by that.
//
// Unlike the postgres drivers this one does not pin a single *sql.Conn. Those pin one
// because an advisory lock is session-scoped; our lock is a table row, so database/sql may
// hand out any connection. That matters on DSQL, which closes every connection at 60
// minutes: a pinned connection would cap a migration run at an hour and would stop the
// connector's 55-minute recycling from ever firing.
//
// STATUS: skeleton. The structure, option surface, connector boundary and retry placement
// are in place for review; every method that talks to the database returns
// errNotImplemented. Each one carries a TODO naming what it will contain and why.
package dsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	nurl "net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	awsdsql "github.com/awslabs/aurora-dsql-connectors/go/pgx/dsql"
	"github.com/awslabs/aurora-dsql-connectors/go/pgx/occretry"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// Defaults for the configurable tables and limits.
var (
	DefaultMigrationsTable       = "schema_migrations"
	DefaultLockTable             = "schema_migrations_lock"
	DefaultMultiStatementMaxSize = 10 * 1 << 20 // 10 MB
)

// OCC retry defaults.
//
// DefaultOCCMaxRetries is 0, so retry is opt-in: this driver imposes no retry count of its
// own and runs a statement once unless asked for more. Both entry points agree on that,
// which a non-zero default cannot manage, since a Config literal's zero is
// indistinguishable from "unset". Worth knowing before leaving it alone: DSQL raises OC001
// non-deterministically even without contention, from a stale catalog cache after a DDL,
// and WithInstance issues two DDLs before migrate's first read.
//
// DefaultOCCMaxRetryDelay does apply when unset, because 0 is not a value the connector can
// use: see occConfig.
const (
	DefaultOCCMaxRetries    = 0
	DefaultOCCMaxRetryDelay = 5 * time.Second

	// minOCCRetryDelay floors the delay handed to the connector. occretry's backoff
	// computes jitter as rand.Int63n(int64(wait/4)), which panics once wait drops below
	// 4ns, and a Config literal takes a time.Duration directly — OCCMaxRetryDelay: 3 is
	// three nanoseconds, not three seconds. One millisecond is also the finest value
	// x-occ-max-retry-delay can express.
	minOCCRetryDelay = time.Millisecond
)

var (
	ErrNilConfig      = errors.New("no config")
	ErrNoDatabaseName = errors.New("no database name")
	ErrNoSchema       = errors.New("no schema")

	// ErrPasswordSet rejects a password in a dsql:// URL. The connector authenticates
	// with an IAM token generated per connection and discards any password it is given,
	// so accepting one would silently ignore the credential the operator supplied.
	ErrPasswordSet = errors.New("dsql: password not supported, authentication uses IAM")
)

// errNotImplemented marks the parts of this skeleton still to be written. It is
// deliberately loud: a half-working migration driver is worse than one that refuses to
// run at all.
var errNotImplemented = errors.New("dsql: driver skeleton, not implemented yet")

func init() {
	database.Register("dsql", &DSQL{})
}

type Config struct {
	MigrationsTable       string
	LockTable             string
	ForceLock             bool
	DatabaseName          string
	SchemaName            string
	StatementTimeout      time.Duration
	MultiStatementEnabled bool
	MultiStatementMaxSize int

	// OCC conflict retry. Classification, backoff and the meaning of these values are the
	// connector's; occConfig hands them over unchanged.
	//
	// OCCMaxRetries counts retries, not attempts, so occretry.Config's zero applies here
	// too: 0 runs a statement exactly once, and 0 is the default. Retry is opt-in, and it is
	// worth opting in, because DSQL raises OC001 with no contention at all.
	// Negative values are a programming error: occretry's loop body never runs, so the
	// statement is skipped and the caller is told retries were exhausted. parseConfig
	// rejects them; a Config literal cannot be checked.
	//
	// OCCMaxRetryDelay is a Duration, so an unsuffixed literal is nanoseconds: write
	// 3 * time.Second, not 3. Unlike the retry count, 0 here cannot be passed through: it
	// would leave the connector no room to back off and panic its jitter, so 0 means
	// "unset" and takes the connector's 5s. See occConfig.
	OCCMaxRetries    int
	OCCMaxRetryDelay time.Duration

	// AwaitAsyncDDL blocks a migration on the asynchronous jobs its DDL enqueues, instead
	// of returning as soon as they are accepted. It covers every statement that returns a
	// job_id, not just index builds: ALTER TABLE ASYNC ... VALIDATE CONSTRAINT is the only
	// way to add a validated foreign key or check constraint on DSQL, and wants the same
	// wait. Off by default.
	AwaitAsyncDDL bool

	// An unexported field forces composite literals outside this package to be keyed, so a
	// field can be added later without breaking callers. Zero width.
	_ struct{}
}

type DSQL struct {
	db       *sql.DB
	isLocked atomic.Bool

	// Open and WithInstance need to guarantee that config is never nil
	config *Config
}

// WithInstance returns a driver for an existing *sql.DB. It neither resolves AWS
// credentials nor opens connections; a caller wanting automatic IAM auth builds the pool
// with awsdsql.NewPool and wraps it via stdlib.OpenDBFromPool.
func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	config.setDefaults()

	// TODO: build &DSQL{db: instance, config: config}, then Ping; fill DatabaseName from
	// CURRENT_DATABASE() and SchemaName from CURRENT_SCHEMA() when unset, returning
	// ErrNoDatabaseName / ErrNoSchema if still empty; then ensureLockTable followed by
	// ensureVersionTable.
	return nil, errNotImplemented
}

// setDefaults fills in the optional fields. Required fields are left alone so a missing
// one surfaces as an error rather than a guess.
func (c *Config) setDefaults() {
	if c.MigrationsTable == "" {
		c.MigrationsTable = DefaultMigrationsTable
	}
	if c.LockTable == "" {
		c.LockTable = DefaultLockTable
	}
	if c.MultiStatementMaxSize <= 0 {
		c.MultiStatementMaxSize = DefaultMultiStatementMaxSize
	}
	// OCCMaxRetries is deliberately not defaulted here. Zero is the connector's "no retry"
	// and defaulting it away would make that value unreachable.
	if c.OCCMaxRetryDelay <= 0 {
		c.OCCMaxRetryDelay = DefaultOCCMaxRetryDelay
	}
}

// occConfig maps the driver's OCC settings onto the connector's retry config. It starts
// from occretry.DefaultConfig, which is how the connector expects to be configured: its
// Config has no normalization of its own, so a zero value would leave Multiplier and
// MaxWait at zero as well.
//
// MaxRetries is handed over unchanged, so the connector's semantics reach the caller
// intact rather than being reinterpreted here.
//
// MaxWait cannot be, and the exception is the connector's doing: its backoff computes
// jitter as rand.Int63n(int64(wait/4)), which panics once wait drops below 4ns, and it
// clamps each subsequent wait to MaxWait. So a MaxWait under 4ns panics on the second
// retry. The floor keeps that unreachable. InitialWait is lowered to match, because the
// connector seeds the first wait from it and only clamps later ones, so a MaxWait below
// InitialWait would otherwise be ignored on the first retry.
func (c *Config) occConfig() occretry.Config {
	cfg := occretry.DefaultConfig()

	cfg.MaxRetries = c.OCCMaxRetries

	cfg.MaxWait = max(c.OCCMaxRetryDelay, minOCCRetryDelay)
	cfg.InitialWait = min(cfg.InitialWait, cfg.MaxWait)

	return cfg
}

// Open connects to Aurora DSQL. The URL carries no password: the connector generates an
// IAM token per connection.
//
//	dsql://admin@cluster.dsql.us-east-1.on.aws:5432/postgres?x-migrations-table=…
//
// The connector resolves the region from ?region=, then the hostname, then the
// environment, and enforces TLS unconditionally — an sslmode in the URL is ignored.
func (d *DSQL) Open(rawURL string) (database.Driver, error) {
	purl, err := nurl.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	config, err := parseConfig(purl)
	if err != nil {
		return nil, err
	}

	// FilterCustomQuery strips only x-*, so region, profile and tokenDurationSecs
	// survive for the connector to parse itself.
	pool, err := awsdsql.NewPool(context.Background(), migrate.FilterCustomQuery(purl).String())
	if err != nil {
		return nil, err
	}

	// TODO: stdlib.OpenDBFromPool does not take ownership of the pool, so Close needs a
	// way to close it too — otherwise a pool Open created outlives the driver.
	db := stdlib.OpenDBFromPool(pool)
	driver, err := WithInstance(db, config)
	if err != nil {
		closeErr := db.Close()
		pool.Close()
		return nil, errors.Join(err, closeErr)
	}
	return driver, nil
}

// parseConfig reads the driver's options from the URL. All configuration comes from the
// URL or from the Config passed to WithInstance; the driver itself never reads the
// environment. (The connector does, for AWS credentials and region — credentials are not
// driver configuration, and an explicit ?region= always wins.)
func parseConfig(purl *nurl.URL) (*Config, error) {
	query := purl.Query()

	// Reject rather than ignore. The connector drops any password it is handed and
	// authenticates with an IAM token, so silently accepting one would leave an operator
	// believing a credential was in use when it never was. An empty password still counts:
	// "admin:@host" names the field, and treating that as absent restores the ambiguity.
	if purl.User != nil {
		if _, set := purl.User.Password(); set {
			return nil, ErrPasswordSet
		}
	}

	if err := rejectIgnoredOptions(query); err != nil {
		return nil, err
	}

	statementTimeout, err := parseInt(query, "x-statement-timeout", 0)
	if err != nil {
		return nil, err
	}

	multiStatementMaxSize, err := parseInt(query, "x-multi-statement-max-size", DefaultMultiStatementMaxSize)
	if err != nil {
		return nil, err
	}
	if multiStatementMaxSize <= 0 {
		multiStatementMaxSize = DefaultMultiStatementMaxSize
	}

	occMaxRetries, err := parseInt(query, "x-occ-max-retries", DefaultOCCMaxRetries)
	if err != nil {
		return nil, err
	}
	// Negatives are rejected because occretry would skip the statement and then report
	// retries as exhausted. Zero needs no special handling: it is both the default and the
	// connector's "run once", so a URL and a Config literal resolve it the same way.
	if occMaxRetries < 0 {
		return nil, fmt.Errorf("x-occ-max-retries must not be negative, got %d", occMaxRetries)
	}

	occMaxRetryDelay, err := parseInt(query, "x-occ-max-retry-delay", int(DefaultOCCMaxRetryDelay/time.Millisecond))
	if err != nil {
		return nil, err
	}
	if occMaxRetryDelay < 0 {
		return nil, fmt.Errorf("x-occ-max-retry-delay must not be negative, got %d", occMaxRetryDelay)
	}

	forceLock, err := parseBool(query, "x-force-lock")
	if err != nil {
		return nil, err
	}

	multiStatementEnabled, err := parseBool(query, "x-multi-statement")
	if err != nil {
		return nil, err
	}

	awaitAsyncDDL, err := parseBool(query, "x-await-async-ddl")
	if err != nil {
		return nil, err
	}

	return &Config{
		DatabaseName:          strings.TrimPrefix(purl.Path, "/"),
		SchemaName:            query.Get("x-migrations-schema"),
		MigrationsTable:       query.Get("x-migrations-table"),
		LockTable:             query.Get("x-lock-table"),
		ForceLock:             forceLock,
		StatementTimeout:      time.Duration(statementTimeout) * time.Millisecond,
		MultiStatementEnabled: multiStatementEnabled,
		MultiStatementMaxSize: multiStatementMaxSize,
		OCCMaxRetries:         occMaxRetries,
		OCCMaxRetryDelay:      time.Duration(occMaxRetryDelay) * time.Millisecond,
		AwaitAsyncDDL:         awaitAsyncDDL,
	}, nil
}

// rejectIgnoredOptions fails on options this driver cannot honor, rather than dropping
// them. Same reasoning as ErrPasswordSet: an operator who sets one of these has an
// expectation, and silently discarding it is worse than refusing to start.
func rejectIgnoredOptions(query nurl.Values) error {
	// The connector replaces ConnConfig.RuntimeParams wholesale, so a search_path in the
	// URL never reaches the server. Schema qualification is the supported route.
	if query.Has("search_path") {
		return errors.New("search_path is not supported, the connector discards it; use x-migrations-schema")
	}
	// postgres can place the migrations table in a schema other than the working one, which
	// this driver cannot express: x-migrations-schema moves both of its tables together.
	if query.Has("x-migrations-table-quoted") {
		return errors.New("x-migrations-table-quoted is not supported, identifiers are always quoted")
	}
	return nil
}

func parseInt(query nurl.Values, key string, fallback int) (int, error) {
	value := query.Get(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("unable to parse %s: %w", key, err)
	}
	return parsed, nil
}

func parseBool(query nurl.Values, key string) (bool, error) {
	value := query.Get(key)
	if value == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("unable to parse %s: %w", key, err)
	}
	return parsed, nil
}

// retry runs fn, re-running it on a DSQL optimistic-concurrency conflict. Classification
// (OC000, OC001, 40001) and the backoff schedule are the connector's; this is the single
// place the driver applies them.
//
// occretry.IsOCCError matches with errors.As on *pgconn.PgError, which pgx's stdlib
// wrapper preserves through database/sql, so the pgx-native helpers are not needed.
//
// fn must return the error it received, unwrapped. database.Error has no Unwrap method, so
// wrapping inside fn hides the *pgconn.PgError: IsOCCError returns false and retry quietly
// degrades to a single attempt. Nothing reports this — the wrapped error still describes a
// real failure, so the lost retry is invisible. The same holds for database.ErrLocked and
// every other sentinel. Add migrate's error context to what this function returns instead;
// occretry wraps its own exhaustion error with %w, so the PgError survives.
func (d *DSQL) retry(ctx context.Context, fn func() error) error {
	return occretry.Retry(ctx, d.config.occConfig(), fn)
}

func (d *DSQL) Close() error {
	if d.db == nil {
		return nil
	}
	return d.db.Close()
}

// Lock takes the migration lock by inserting a row, because Aurora DSQL has no advisory
// locks. The row's primary key is the arbiter, but unlike PostgreSQL the loser is not
// always a unique violation: DSQL adjudicates conflicts at COMMIT under snapshot
// isolation, so two migrators inserting the same key concurrently both succeed at
// statement time and the loser commits with SQLSTATE 40001. A migrator that starts after
// the winner has committed sees the row in its snapshot and gets 23505 instead.
//
// So "already held" is treated as a value rather than an error: INSERT ... ON CONFLICT DO
// NOTHING, and no rows affected means someone else holds it. 40001 stays with retry(),
// where it belongs, and ErrLocked does not depend on which of the two codes came back.
//
// A process killed while holding the lock leaves the row behind; x-force-lock lets an
// operator delete it deliberately.
func (d *DSQL) Lock() error {
	return database.CasRestoreOnErr(&d.isLocked, false, true, database.ErrLocked, func() error {
		// TODO: when config.ForceLock, DELETE the row here, outside the retried unit.
		// Inside it, a retried attempt would re-run the DELETE and could remove a row a
		// second migrator committed in the meantime, so both would believe they hold the
		// lock. Force-lock clears a stale row, not a live one.
		return d.retry(context.Background(), func() error {
			// TODO: INSERT lockID() ... ON CONFLICT (lock_id) DO NOTHING, and return
			// database.ErrLocked bare when RowsAffected is 0, so callers can match it
			// with errors.Is. Also map a 23505 from the INSERT or the COMMIT through
			// isLockConflict, which retry() passes through untouched because a unique
			// violation is not one of the OCC codes. An OCC loser (40001) is retried
			// instead, and on the next attempt the winner's row is visible, so the
			// no-rows-affected arm reports it.
			return errNotImplemented
		})
	})
}

func (d *DSQL) Unlock() error {
	return database.CasRestoreOnErr(&d.isLocked, true, false, database.ErrNotLocked, func() error {
		return d.retry(context.Background(), func() error {
			// TODO: DELETE the lock row by lockID(). Treat isUndefinedTable as success,
			// because Drop removes the lock table and Migrate still calls Unlock after.
			return errNotImplemented
		})
	})
}

// lockID derives the lock row's key from everything that identifies the lock, including
// the lock table itself: two migrators pointed at the same schema and migrations table but
// different x-lock-table values would otherwise compute the same key and store it in
// different tables, excluding nobody. PostgreSQL cannot express that, since its advisory
// lock has no table to vary.
func (d *DSQL) lockID() (string, error) {
	return database.GenerateAdvisoryLockId(
		d.config.DatabaseName,
		d.config.SchemaName,
		d.config.MigrationsTable,
		d.config.LockTable,
	)
}

// Run applies a migration.
//
// By default the whole file is sent as one Exec. That relies on pgx dropping to the simple
// protocol, which it does only when the Exec carries no bound arguments; the server then
// treats the whole string as one implicit transaction. So a multi-statement file fails
// atomically and nothing is half-applied, and an OCC conflict committed nothing, which is
// why this path is safe to retry. Passing even one parameter would send the file through
// the extended protocol, where it is a syntax error.
//
// x-multi-statement splits the file at semicolons so each DDL gets its own transaction,
// which DSQL requires for a file holding more than one DDL. Each piece is retried on its
// own; what must never be replayed is a piece that already committed.
func (d *DSQL) Run(migration io.Reader) error {
	// TODO: read the reader and hand it to runStatement. When !MultiStatementEnabled that
	// is one call under retry(). When enabled, split with multistmt.Parse on ";" and wrap
	// each piece in its own retry() — the piece is its own transaction, so an OCC failure
	// committed nothing and re-running just that piece is safe. Do not hold one retry()
	// around the whole loop: that would replay pieces that already succeeded.
	//
	// Note the splitter is a plain byte search for ";" with no SQL awareness: it breaks
	// dollar-quoted bodies, string literals and comments. Hence off by default.
	return errNotImplemented
}

// runStatement executes one statement.
//
// Callers wrap this in retry(), so it must return the driver's error unwrapped — see
// retry() for why. The database.Error carrying the line number belongs to whoever calls
// retry(), applied to what retry() returns.
func (d *DSQL) runStatement(ctx context.Context, statement []byte) error {
	// TODO: skip blank statements; apply config.StatementTimeout via context.WithTimeout
	// only when it is non-zero, as postgres and pgx both do. Zero is "no limit", and
	// context.WithTimeout(ctx, 0) yields a context that has already expired.
	//
	// Then branch before executing, because a statement that enqueues an asynchronous job
	// needs a different pgx call from one that does not:
	//
	//   - config.AwaitAsyncDDL, and the statement returns a job_id (CREATE [UNIQUE] INDEX
	//     ... ASYNC today, and ALTER TABLE ASYNC ... VALIDATE CONSTRAINT, which is the only
	//     way to add a validated FK or CHECK on DSQL): QueryContext and scan the job_id row,
	//     then awaitAsyncJob. ExecContext cannot be used, because stdlib returns
	//     driver.RowsAffected, an int64, so the row is discarded before a sql.Result
	//     exists. Match with comments stripped first, so a comment cannot hide the
	//     keywords, and fail loudly if a statement matched but no job_id came back rather
	//     than reporting a job that was never waited on.
	//   - otherwise: ExecContext.
	//
	// Two things about the option are still open, and a live run should settle them before
	// it is relied on: whether a bool is enough, given sys.wait_for_job blocks against a
	// hard 60-minute connection cap and a duration would bound that, and whether off is the
	// right default, given a failed build leaves an INVALID index that still enforces
	// uniqueness until someone drops it.
	//
	// Return the *pgconn.PgError as-is. The caller turns it into a database.Error with the
	// failing line, porting computeLineFromPos from database/pgx/v5/pgx.go:304-316.
	// Reporting the position matters more here than in the postgres drivers, because the
	// default path sends the whole file as one statement so pgErr.Position is
	// file-relative.
	return errNotImplemented
}

// awaitAsyncJob blocks until an enqueued asynchronous DDL job finishes.
//
// DSQL has no synchronous form of these: CREATE INDEX ASYNC and ALTER TABLE ASYNC ...
// VALIDATE CONSTRAINT both return a job_id as soon as the work is enqueued, so a later
// migration step can run against an index that is not ready or a constraint that is not
// validated, and a failed build leaves an INVALID index behind.
func (d *DSQL) awaitAsyncJob(ctx context.Context, jobID string) error {
	// TODO: CALL sys.wait_for_job(jobID) outside any transaction block; fail the migration
	// unless it returns true, since a false means the job failed or the wait timed out and
	// neither raises. A false is ambiguous between those two, so re-read sys.jobs.status to
	// say which, and note that terminal jobs are deleted after 30 minutes.
	//
	// Do not put this inside the retry() around the statement that enqueued the job:
	// finishing the job changes the catalog, so a concurrent OC001 here would re-run the
	// CREATE INDEX ASYNC and enqueue a second build. sys.jobs is cluster-wide, so the
	// job_id is still usable from another pooled connection.
	return errNotImplemented
}

// SetVersion records the migration version. Migrate calls this before and after every
// migration, and Force routes through it too, so it sits on the recovery path as well as
// the happy one.
func (d *DSQL) SetVersion(version int, dirty bool) error {
	// TODO: inside a single retry(), one transaction holding a DELETE of the version table
	// with no WHERE clause followed by the INSERT of (version, dirty) — two DML statements
	// and no DDL, which DSQL permits. TRUNCATE, which the postgres drivers use here, is
	// unsupported. Schema-qualify both statements via qualifiedTable.
	//
	// The retry has to cover begin/delete/insert/commit as one unit: a pooled session can
	// take OC001 at any of them after a schema change, and the pair is only meaningful
	// applied together. Follow the postgres drivers in also writing the row when
	// version == database.NilVersion && dirty, so a down migration that fails on the
	// first migration does not leave the table empty (golang-migrate/migrate#330).
	return errNotImplemented
}

func (d *DSQL) Version() (version int, dirty bool, err error) {
	// TODO: SELECT version, dirty ... LIMIT 1; report database.NilVersion for both
	// sql.ErrNoRows and isUndefinedTable.
	return 0, false, errNotImplemented
}

// Drop deletes everything in the schema.
func (d *DSQL) Drop() error {
	// TODO: list BASE TABLEs from information_schema for config.SchemaName, close the
	// rows before issuing DDL, then DROP TABLE ... CASCADE each, ordering the lock table
	// last. That keeps the lock held for the whole loop and leaves the table in place if
	// the loop aborts part-way. On the success path the lock table is gone whichever order
	// is used, and the Unlock that Migrate issues afterwards relies on isUndefinedTable
	// being treated as success.
	return errNotImplemented
}

// ensureVersionTable creates the version table. Like cockroachdb's, it takes the lock
// itself, which deviates from the usual "caller locks" convention in this type.
func (d *DSQL) ensureVersionTable() error {
	// TODO: Lock(), then a deferred Unlock() whose error is joined onto the return with
	// errors.Join. Releasing it is not optional: the lock is a durable row, so a leaked one
	// outlives the process and every later run fails to acquire it, including the force
	// that would clear it.
	//
	// Then a retried CREATE TABLE IF NOT EXISTS
	// (version BIGINT NOT NULL PRIMARY KEY, dirty BOOLEAN NOT NULL), with no existence
	// check, for the same reason as ensureLockTable.
	return errNotImplemented
}

// ensureLockTable creates the lock table. It runs before the lock exists, so it cannot be
// lock-protected, and is written to be safe under concurrency instead.
func (d *DSQL) ensureLockTable() error {
	// TODO: a retried CREATE TABLE IF NOT EXISTS (lock_id TEXT NOT NULL PRIMARY KEY), with
	// no existence check, which would reintroduce a race this does not have.
	//
	// That closes the race here, unlike in PostgreSQL where IF NOT EXISTS has a genuine
	// non-transactional window. DSQL's catalog is transactional under OCC, so a losing
	// concurrent creation surfaces as a conflict at commit rather than a duplicate-table
	// error, and retry() handles it. Measured against a live cluster: 144 concurrent
	// CREATE TABLE IF NOT EXISTS at up to 24-way concurrency produced only SQLSTATE 40001
	// and never 42P07.
	return errNotImplemented
}

// qualifiedTable renders a schema-qualified, quoted table name.
func (d *DSQL) qualifiedTable(table string) string {
	return quoteIdentifier(d.config.SchemaName) + "." + quoteIdentifier(table)
}

// isLockConflict reports whether err means another migrator already holds the lock, i.e. a
// unique violation on the lock row.
//
// DSQL's optimistic-concurrency failures are deliberately not treated as lock conflicts.
// They are retryable, and OC001 fires with no contention at all, so reporting one as
// ErrLocked would tell an operator to wait for a lock nobody holds. They are handled by
// retry() instead, via the connector's occretry.IsOCCError.
func isLockConflict(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.SQLState() == pgerrcode.UniqueViolation
}

func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.SQLState() == pgerrcode.UndefinedTable
}

// Copied from lib/pq implementation: https://github.com/lib/pq/blob/v1.9.0/conn.go#L1611
func quoteIdentifier(name string) string {
	if end := strings.IndexRune(name, 0); end >= 0 {
		name = name[:end]
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
