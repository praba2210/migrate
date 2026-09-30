# Aurora DSQL README Reduction Plan

## Goal

Reduce `database/dsql/README.md` from 313 lines to approximately 60–80 lines.
Keep only the information required to start using the driver and replace detailed
Aurora DSQL explanations with links to maintained documentation.

## Proposed README Structure

### 1. Overview

- Two or three sentences describing the driver.
- Link to the Amazon Aurora DSQL documentation.

### 2. Usage

- Build the CLI with the `dsql` build tag.
- Show one `migrate up` command.
- Mention the required IAM permission and link to the official documentation.

### 3. Connection URL

- Show the URL format.
- Keep compact connector and driver option tables.
- Remove extended explanations already represented by the tables or external
  documentation.

### 4. Example

- Include one short migration using `CREATE INDEX ASYNC`.
- Link to `database/dsql/examples/migrations`.
- Link to the repository's `MIGRATIONS.md`.

### 5. Important Behavior

Limit each item to one sentence and link to supporting documentation:

- Use one DDL statement per migration.
- Wait for asynchronous DDL jobs before recording a migration as complete.
- Optionally retry optimistic concurrency conflicts.
- Use `x-force-lock` to recover from a stale migration lock.

### 6. Development

- Keep compact commands for unit tests, PostgreSQL compatibility tests, and
  Aurora DSQL integration tests.
- Preserve the integration-test schema safety warning.

### 7. References

Link to:

- Aurora DSQL migration guidance.
- PostgreSQL features not supported by Aurora DSQL.
- Aurora DSQL quotas and database limits.
- Database roles and IAM authentication.
- Concurrency control.
- Asynchronous DDL.
- `dsql-lint`.

## Content to Remove

- The entire "Features and Limitations" overview.
- The custom-role walkthrough and SQL grant examples.
- Detailed migration-lock implementation.
- Detailed optimistic concurrency retry algorithm.
- Detailed asynchronous job polling explanation.
- The failure-recovery walkthrough.
- The library usage example.
- Copied quotas and PostgreSQL compatibility lists.
- Repeated explanations already covered by linked documentation.

## Example Migration Changes

Add:

- `database/dsql/examples/migrations/3_index_users_email.up.sql` using
  `CREATE INDEX ASYNC`.
- `database/dsql/examples/migrations/3_index_users_email.down.sql` using
  `DROP INDEX`.

## Expected Files Changed

- `database/dsql/README.md`
- `database/dsql/examples/migrations/3_index_users_email.up.sql`
- `database/dsql/examples/migrations/3_index_users_email.down.sql`

The implementation should make the DSQL README resemble the repository's other
database-driver READMEs while retaining DSQL-specific guidance through concise
examples and maintained references.
