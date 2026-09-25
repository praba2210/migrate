# Amazon Aurora DSQL

## Introduction

This driver applies golang-migrate migrations to Amazon Aurora DSQL. It keeps the
migrations and lock tables, holds the migration lock as a table row, and waits for
asynchronous DDL to finish before reporting a migration as applied.

## Features and Limitations

Aurora DSQL's distributed architecture differs from single-node PostgreSQL in a few
ways that shape how the driver behaves:

- **IAM authentication** — TLS and a per-connection token come from the
  [Aurora DSQL connector for pgx](https://github.com/awslabs/aurora-dsql-connectors/tree/main/go/pgx),
  which resolves credentials through the default AWS chain. The token is what
  authenticates every connection, so a URL carries no password.
- **Table-row lock** — the migration lock is a row in its own table, which suits DSQL's
  distributed design. It outlives the session that took it, and `x-force-lock` releases
  one a previous run left behind. See [Lock](#lock).
- **Optimistic concurrency** — `x-occ-max-retries` has the connector retry a conflict
  DSQL reports at COMMIT. See [Concurrency control](#concurrency-control).
- **Asynchronous DDL** — the driver waits for the job an `ASYNC` statement enqueues, so
  the version migrate records describes a finished schema. See
  [Asynchronous DDL](#asynchronous-ddl).
- **One DDL per transaction** — one DDL statement per migration file applies
  atomically. See [Writing migrations](#writing-migrations).

## Prerequisites

- Go 1.25+
- An Amazon Aurora DSQL cluster
- `dsql:DbConnectAdmin` for the `admin` user, or `dsql:DbConnect` for any other
- Credentials resolvable from the default AWS chain

## Setup

The `dsql` tag compiles the driver in, keeping the connector and `aws-sdk-go-v2` out of
the default CLI:

```sh
go build -tags dsql ./cmd/migrate
migrate -source file://migrations \
  -database 'dsql://admin@mycluster.dsql.us-east-1.on.aws/postgres' up
```

### URL

```
dsql://admin@mycluster.dsql.us-east-1.on.aws:5432/postgres?query
```

The host accepts a full cluster endpoint or a 26-character cluster ID. The region
resolves from `region`, then the hostname, then `AWS_REGION` or `AWS_DEFAULT_REGION`.
TLS is always enforced, so `sslmode` needs no setting.

Connector options, passed through untouched:

| Param | Default | Description |
|---|---|---|
| `region` | parsed from host | AWS region of the cluster |
| `profile` | default chain | AWS shared-config profile |
| `tokenDurationSecs` | `900` | IAM token validity |

Driver options:

| URL query | `Config` field | Default | Description |
|---|---|---|---|
| `x-migrations-schema` | `SchemaName` | `CURRENT_SCHEMA()` | Schema holding the two tables |
| `x-migrations-table` | `MigrationsTable` | `schema_migrations` | Migrations table name |
| `x-lock-table` | `LockTable` | `schema_migrations_lock` | Lock table name |
| `x-force-lock` | `ForceLock` | `false` | Release a lock row left behind |
| `x-statement-timeout` | `StatementTimeout` | none | Abort a statement after N ms |
| `x-multi-statement` | `MultiStatementEnabled` | `false` | Split the file at semicolons |
| `x-multi-statement-max-size` | `MultiStatementMaxSize` | 10 MB | Parser buffer for the above |
| `x-occ-max-retries` | `OCCMaxRetries` | `0` | Retries after an OCC conflict |
| `x-occ-max-retry-delay` | `OCCMaxRetryDelay` | `5000` | Backoff ceiling in ms; jitter adds 25% |

`search_path` reaches the connection through a pool hook. Set any other libpq
parameter, `sslmode` and `connect_timeout` included, on your own pool and use
[`WithInstance`](#usage-as-a-library).

### search_path

`?search_path=app` places unqualified SQL as it does with the `postgres` and `pgx`
drivers, and resolves `CURRENT_SCHEMA()` to `app`, leaving `x-migrations-schema` an
override. The value travels verbatim, so the server parses it the same way.

Write the plain spelling, `search_path=app`. The driver names that form for you when a URL
carries `options=-c search_path=app` instead.

`WithInstance` callers own their pool, so they set `search_path` in its config. The
driver qualifies its own two tables either way.

### Migrating as a custom role

A new DSQL role owns no schema, so three grants make one ready. Connect as `admin`:

```sql
CREATE ROLE migrator WITH LOGIN;
AWS IAM GRANT migrator TO 'arn:aws:iam::123456789012:role/my-migration-role';
GRANT USAGE, CREATE ON SCHEMA app TO migrator;
```

Then migrate with `x-migrations-schema=app`, which tells the driver where to put the
migrations and lock tables. `sys.iam_pg_role_mappings` lists the IAM-to-role mappings.

The driver places those two tables; your own SQL decides where your objects land:

```sql
CREATE TABLE app.users (id UUID PRIMARY KEY);   -- lands in app
CREATE TABLE users (id UUID PRIMARY KEY);       -- lands wherever search_path points
```

The migrations under `examples/` use bare names, as the other drivers' examples do, so
add the prefix or set `search_path`.

## Writing migrations

**One DDL statement per file** applies atomically, because a DSQL transaction holds one DDL
statement and keeps DDL separate from DML. The driver sends the whole file as a single
statement, which becomes one implicit transaction.

`x-multi-statement=true` gives each statement its own transaction. The splitter searches
for `;` literally, so one DDL per file remains the better answer.

DSQL specifics worth knowing:

- `CREATE INDEX ASYNC` is how an index is built, and the driver waits for it. See
  [Asynchronous DDL](#asynchronous-ddl).
- `BIGINT GENERATED BY DEFAULT AS IDENTITY` takes an explicit cache size, `CACHE 1` for
  close ordering or `CACHE 65536` and above for throughput. A UUID is the other surrogate
  key, in place of `SERIAL`.
- A constraint is added with `ALTER TABLE ... ADD CONSTRAINT ... NOT VALID` and
  validated with `ALTER TABLE ASYNC ... VALIDATE CONSTRAINT`. Only the validation takes
  `ASYNC`, and the two are two DDLs, so they belong in two files.
- `ALTER COLUMN ... TYPE`, `ALTER COLUMN ... SET NOT NULL`, `ADD COLUMN ... NOT NULL
  DEFAULT`, `TRUNCATE`, materialized views and triggers are unsupported.
- A transaction carries up to 3,000 rows and 10 MiB.
- A connection lasts 60 minutes. That bounds a single statement, not a run: the driver holds no
  session and the pool retires a connection at 55 minutes, so a run spans as many connections
  as it needs and can take longer than an hour.

[dsql-lint](https://github.com/awslabs/aurora-dsql-tools/tree/main/dsql-lint) reports
these against your files, down-migrations included, and catches far more than a driver
can. Run it in CI:

```sh
uvx dsql-lint migrations/*.sql
```

`--fix` has dsql-lint rewrite the SQL. This driver applies your files as written.

## Lock

`Lock` inserts a row keyed by the schema and both table names, so two migrators sharing
those exclude each other, and a different `x-lock-table` gives a migrator a lock of its own.

The row outlives the process that took it, which keeps the lock durable across a
restart. `x-force-lock=true` releases one a previous run left behind:

```sh
migrate -database 'dsql://...?x-force-lock=true' ... up
```

It releases whichever row is there, so confirm no other migration is running first.

Creating the version table takes the lock, so a second driver opened against the same
tables reports `can't acquire lock` while the first holds it. `cockroachdb` behaves the
same way.

## Concurrency control

DSQL adjudicates conflicts at COMMIT and reports `OC000`, `OC001` or `40001`. Taking and
releasing the lock retry on their own, which is what lets a concurrent loser report
"already locked" and a finished run release the lock.

`x-occ-max-retries` extends retry to your migration statements, and
`x-occ-max-retry-delay` bounds the backoff:

```sh
migrate -database 'dsql://...?x-occ-max-retries=5' ... up
```

It reaches your statements, `SetVersion`, `Version` and `Drop`. Releasing the lock already
retries ten times and takes your setting only above that, since a `DELETE` that gives up
leaves a lock row nobody holds.

Taking the lock keeps its own count whatever you set, because migrate abandons a `Lock` that
overruns its 15-second lock timeout while the call keeps running, and a retry landing
afterwards writes a lock row nothing releases.

## Asynchronous DDL

`CREATE [UNIQUE] INDEX ASYNC` and `ALTER TABLE ASYNC ... VALIDATE CONSTRAINT` hand back
a `job_id` as soon as their work is enqueued. The driver recognizes either spelling with
comments anywhere among its keywords, so `CREATE /* later */ INDEX ASYNC` is waited on like
the plain form.

The driver waits for the job and reports what `sys.jobs` says when one fails, so a
migration counts as applied once its index is built and its constraint is validated.
Migrate clears the dirty flag as soon as `Run` returns, so the wait is what keeps the
version history describing the schema. AWS recommends it for schema migrations for the
same reason.

The wait catches a unique index over duplicate rows, which reports a duplicate key and
leaves the index `INVALID` and still enforcing uniqueness until it is dropped; and a
`CHECK` some row violates, which reports `is violated by some row` and leaves the
constraint unvalidated. Both reach you as `migration failed`.

A large index build takes real time, and the 60-minute connection limit applies to it.

## When a migration fails

Migrate marks the version dirty, and `migrate up` reports
`Dirty database version N. Fix and force version.` until that is cleared. Recovery is
deliberate, because only you know how much of the file landed.

Find the version, inspect what the schema has, finish or undo the remainder by hand,
then record where you ended up:

```sh
migrate ... version   # report the dirty version
migrate ... force N   # set version N and clear the dirty flag, running nothing
migrate ... up        # resume
```

Point `force` at the state the database is genuinely in. One DDL per file keeps that
judgement trivial, which is the main reason to follow the rule.

A run that stopped mid-build leaves that object in the state
[Asynchronous DDL](#asynchronous-ddl) describes, so include it:

```sql
SELECT relname, indisvalid FROM pg_index JOIN pg_class ON pg_class.oid = indexrelid;
SELECT conname, convalidated FROM pg_constraint;
```

`force` takes the lock like any other command, so add `x-force-lock=true` when a
previous run left a lock row behind. See [Lock](#lock).

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

`WithInstance` takes the `*sql.DB` you built, so you own credentials and pooling. `Open`
builds its own pool and closes it in `Close`.

`OCCMaxRetries` reaches the connector unchanged and counts retries rather than attempts,
so `0` runs each statement once and a URL and a `Config` literal resolve alike.
`OCCMaxRetryDelay` is a `time.Duration`, so write `3 * time.Second`.

## Development

Tests come in three tiers, and the whole suite is green with nothing installed — each
tier that needs something skips without it:

```sh
go test ./database/dsql/
```

The `dsql` tag is for the CLI, which imports the driver behind it; the package itself carries
no build constraint, so CI exercises these tests with no tag of its own.

The unit tests need nothing, and cover option parsing, async DDL classification and error
mapping. Adding `-skip TestPostgresStandIn` runs them alone.

`TestPostgresStandIn` drives the driver against real PostgreSQL through `WithInstance`,
the stand-in `redshift` uses for Redshift. Every statement the driver issues is plain
PostgreSQL, so this covers the lock protocol, the version bookkeeping and `Drop`. It runs
when a Docker daemon is reachable and skips when one is not:

```sh
go test -run TestPostgresStandIn ./database/dsql/
```

The `x-multi-statement` split is verified there too: the splitter searches for `;`
literally, so PostgreSQL exercises it fully. AWS documents the one DDL per transaction that
shapes the files themselves — see [Writing migrations](#writing-migrations).

PostgreSQL stands in for the protocol, not for compatibility — it accepts SQL that DSQL
refuses. `sys.jobs` and `CREATE INDEX ASYNC` have no PostgreSQL equivalent, so the
asynchronous DDL wait belongs to the third tier: a real cluster, which skips without one:

```sh
DSQL_CLUSTER_ENDPOINT=mycluster.dsql.us-east-1.on.aws \
DSQL_TEST_SCHEMA=migrate_scratch \
  go test -run TestIntegration ./database/dsql/
```

`DSQL_TEST_SCHEMA` names an existing scratch schema, and it has to be one you are willing to
lose: `Drop` is under test and empties the schema it runs in, taking every table, view,
sequence, domain and function with it, whether a test created it or not. A cluster carries up
to 10 schemas, so the tests share one and each takes its own prefixed objects inside it.

## Troubleshooting

**`can't acquire lock`** — a previous run holds the lock row. Confirm nothing else is
migrating, then add `x-force-lock=true`. See [Lock](#lock).

**`Dirty database version N`** — see
[When a migration fails](#when-a-migration-fails).

**Connection refused for a custom role** — check the mapping with
`SELECT * FROM sys.iam_pg_role_mappings`, and confirm the role has `USAGE, CREATE` on
its schema.

**`password not supported`** — the driver authenticates with an IAM token, so a URL
carries a user and no password.

**A migration appears to hang** — a large `CREATE INDEX ASYNC` is still building.
`SELECT * FROM sys.jobs` reports its progress.

## Resources

- [Aurora DSQL docs](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/what-is-aurora-dsql.html)
- [Concurrency control](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-concurrency-control.html)
- [Asynchronous indexes](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-create-index-async.html)
- [Unsupported PostgreSQL features](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-unsupported-features.html)
- [Aurora DSQL connector for pgx](https://github.com/awslabs/aurora-dsql-connectors/tree/main/go/pgx)
- [dsql-lint](https://github.com/awslabs/aurora-dsql-tools/tree/main/dsql-lint)
