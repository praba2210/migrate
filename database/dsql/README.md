# Amazon Aurora DSQL

> **Status: skeleton — not usable yet.** The option surface, connector boundary and retry
> placement below are in place for review, but every operation that touches the database
> returns `dsql: driver skeleton, not implemented yet`. The driver is excluded from the
> default CLI build; add `-tags dsql` to compile it in.

Aurora DSQL is PostgreSQL wire-compatible but a distinct distributed engine. It has no
advisory locks and no `TRUNCATE`, which the `postgres` and `pgx` drivers both rely on, so
it needs its own driver rather than a DSN change.

`dsql://` URLs must not carry a password — one is rejected rather than ignored, because the
connector discards it and authenticates with IAM regardless. IAM authentication, TLS and
optimistic-concurrency retry are handled by the
[AWS Aurora DSQL connector for pgx](https://github.com/awslabs/aurora-dsql-connectors/tree/main/go/pgx),
which generates a token per connection from the default AWS credential chain.

## URL

```
dsql://admin@mycluster.dsql.us-east-1.on.aws:5432/postgres?query
```

The host may be a full cluster endpoint or a 26-character cluster ID. The region is taken
from `region`, then the hostname, then `AWS_REGION` or `AWS_DEFAULT_REGION`. TLS is always
enforced, so `sslmode` is ignored.

Connector options, passed through untouched:

| Param | Default | Description |
|---|---|---|
| `region` | parsed from host | AWS region of the cluster |
| `profile` | default chain | AWS shared-config profile |
| `tokenDurationSecs` | `900` | IAM token validity |

Driver options:

| URL query | `WithInstance` `Config` | Default | Description |
|---|---|---|---|
| `x-migrations-schema` | `SchemaName` | `CURRENT_SCHEMA()` | Schema holding the migrations and lock tables |
| `x-migrations-table` | `MigrationsTable` | `schema_migrations` | Name of the migrations table |
| `x-lock-table` | `LockTable` | `schema_migrations_lock` | Name of the lock table |
| `x-force-lock` | `ForceLock` | `false` | Take the lock even if a row is already there. Break glass, to recover from a migration that died holding it |
| `x-statement-timeout` | `StatementTimeout` | none | Abort a statement after N milliseconds |
| `x-multi-statement` | `MultiStatementEnabled` | `false` | Split a migration file at semicolons |
| `x-multi-statement-max-size` | `MultiStatementMaxSize` | 10 MB | Parser buffer limit for the above |
| `x-occ-max-retries` | `OCCMaxRetries` | `0` | Retries of a statement that hits an OCC conflict. Retry is opt-in; see below |
| `x-occ-max-retry-delay` | `OCCMaxRetryDelay` | `5000` | Bound on the backoff between retries, in milliseconds. Jitter can add up to a further 25% |
| `x-await-async-ddl` | `AwaitAsyncDDL` | `false` | Block until the asynchronous jobs a migration enqueues finish |

## Permissions

The connecting principal needs `dsql:DbConnectAdmin` for the `admin` user, or
`dsql:DbConnect` for any other, plus credentials resolvable from the default chain.

Migrating as a custom role takes three steps, none of which happen automatically — a new
role has no schema of its own and no access to anyone else's. Connect as `admin` and run:

```sql
CREATE ROLE migrator WITH LOGIN;
AWS IAM GRANT migrator TO 'arn:aws:iam::123456789012:role/my-migration-role';
GRANT USAGE, CREATE ON SCHEMA app TO migrator;
```

Then migrate with `x-migrations-schema=app`. `sys.iam_pg_role_mappings` lists the
IAM-to-role mappings if a connection is refused. Because the role's schema is not implied by
its name, `x-migrations-schema` is how this driver learns where to put the migrations and
lock tables.

It places those two tables and nothing else. Where your own objects land is decided by your
SQL, so qualify it:

```sql
CREATE TABLE app.users (id UUID PRIMARY KEY);   -- lands in app
CREATE TABLE users (id UUID PRIMARY KEY);       -- lands wherever search_path points
```

The migrations under `examples/` use bare names, as the other drivers' examples do, so add
the prefix when migrating as a custom role.

`search_path` cannot do this job here. Both `?search_path=app` and
`?options=-c search_path=app` are stored in pgx's `RuntimeParams`, which the connector
replaces wholesale, so neither reaches the server; the driver rejects them rather than let
them look effective. A library caller who wants `search_path` can set it in their own pool's
`AfterConnect`, which the connector leaves alone, and pass that pool to `WithInstance`.

That is also why the driver qualifies its own two tables rather than relying on a session
setting: a pool hook covers the `dsql://` path, but only qualifying covers `WithInstance`,
where the caller supplies a pool this driver never configures.

## Writing migrations for DSQL

**One DDL statement per file.** A transaction may hold at most one DDL statement and may
not mix DDL with DML. By default this driver sends the whole file as a single statement,
which becomes one implicit transaction, so a file with two `CREATE TABLE`s fails.

`x-multi-statement=true` splits the file so each statement gets its own transaction, but
the splitter is a plain search for `;` with no SQL awareness — it breaks dollar-quoted
bodies, string literals and comments — and a failure part-way leaves earlier statements
committed. One DDL per file is the better answer.

Other DSQL constraints worth knowing:

- `CREATE INDEX` must be `CREATE INDEX ASYNC`, which returns once the build is *enqueued*.
- No `SERIAL`; use `BIGINT GENERATED BY DEFAULT AS IDENTITY (CACHE 1)` or a UUID.
- `ALTER COLUMN ... TYPE`, `ALTER COLUMN ... SET NOT NULL` and
  `ADD COLUMN ... NOT NULL DEFAULT` are not supported at all.
- No `TRUNCATE`, materialized views or triggers.
- A transaction is capped at 3,000 rows and 10 MiB.
- DSQL closes every connection at 60 minutes, so a single migration run must finish inside
  an hour.

## When a migration fails

An OCC conflict (`OC000`, `OC001`, `40001`) is retryable, and this driver retries one for
you. If a migration fails anyway, migrate has already marked the version dirty and will not
run again until that is cleared: a second `migrate up` stops with
`Dirty database version N. Fix and force version.` Re-running is not the recovery, because
migrate cannot know how much of the failed file was applied.

Recovery is manual. Find the dirty version with `migrate ... version`, then inspect the
schema to see what actually landed — DSQL puts each DDL statement in its own transaction,
so a file with several statements can be partly applied. Finish or undo the remainder by
hand, then tell migrate where you ended up:

```sh
migrate ... force N   # set version N and clear the dirty flag, running nothing
migrate ... up        # resume
```

If the failed run died without releasing the lock, both of those stop with `can't acquire
lock` — `force` takes the lock too. Add `x-force-lock=true` to the URL to delete the stale
row, and check no other migration is running first, because it does not ask.

Point `force` at the state the database is genuinely in, not the one you wanted. One DDL
per file keeps that judgement trivial, which is the main reason to follow the rule.

## Checking migrations before you run them

[dsql-lint](https://github.com/awslabs/aurora-dsql-tools/tree/main/dsql-lint) reports the
incompatibilities above against your migration files, including the down-migrations. It
catches far more than this driver can, because most DSQL friction is in the content of a
migration rather than in the tool applying it. Run it in CI:

```sh
uvx dsql-lint migrations/*.sql
# or, after `npm install -g @aws/dsql-lint`: dsql-lint migrations/*.sql
```

`--fix` makes dsql-lint rewrite the SQL, which is how the Aurora DSQL ORM integrations use
it. This driver never rewrites anything: your migration files are applied as written.

It will not flag a file with several DDL statements unless the file has an explicit
`BEGIN`/`COMMIT`, so keep the one-DDL-per-file rule in review as well.

## Asynchronous DDL

`CREATE INDEX ASYNC` returns as soon as the build is enqueued, so a later migration can run
against an index that is not ready, and a failed build leaves an `INVALID` index behind that
has to be dropped by hand. `ALTER TABLE ASYNC ... VALIDATE CONSTRAINT` behaves the same way.

`x-await-async-ddl=true` blocks the migration on those jobs and fails it if one did not
succeed. It is off by default: fire-and-forget is fine for a pure performance index, and
the wait only earns its cost when a later step depends on the result.

Two things about the option are still open until it has run against a real cluster: whether
a bool is enough, since `sys.wait_for_job` blocks against the 60-minute connection cap and
a duration would bound that, and whether off is the right default, since a failed unique
index keeps enforcing uniqueness on writes until someone drops it.

## Usage as a library

```go
import (
    "github.com/golang-migrate/migrate/v4"
    "github.com/golang-migrate/migrate/v4/database/dsql"
    _ "github.com/golang-migrate/migrate/v4/source/file"
    awsdsql "github.com/awslabs/aurora-dsql-connectors/go/pgx/dsql"
    "github.com/jackc/pgx/v5/stdlib"
)

pool, err := awsdsql.NewPool(ctx, awsdsql.Config{Host: "mycluster.dsql.us-east-1.on.aws"})
if err != nil {
    return err
}
defer pool.Close()

driver, err := dsql.WithInstance(stdlib.OpenDBFromPool(pool), &dsql.Config{
    SchemaName: "app",
})
if err != nil {
    return err
}

m, err := migrate.NewWithDatabaseInstance("file://migrations", "dsql", driver)
```

`WithInstance` does not resolve credentials or open connections, so a caller wanting a
different auth or pooling setup builds the `*sql.DB` themselves.

`OCCMaxRetries` is handed to the connector unchanged and counts retries rather than
attempts, so the default of `0` runs each statement once. A URL and a `Config` literal
resolve it identically. Mind the units on `OCCMaxRetryDelay`: it is a `time.Duration`, so
`OCCMaxRetryDelay: 3` means three nanoseconds, not three seconds.
