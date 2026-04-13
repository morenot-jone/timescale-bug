# TimescaleDB Compressed-Chunk Deduplication Bug

`INSERT ... ON CONFLICT DO NOTHING` silently creates duplicate rows when the
target data lives in a **compressed chunk** and the unique constraint includes a
**nullable column whose value is NULL**.

## The bug

TimescaleDB hypertables support compression, which rewrites chunk data into a
columnar format. When a new row is inserted with `ON CONFLICT DO NOTHING`,
TimescaleDB must check the compressed data for conflicts. That check fails when
any column in the unique constraint is `NULL`, because SQL defines
`NULL != NULL` — the compressed-side comparison never finds a match, so the
"conflicting" row is inserted as a duplicate.

When the same nullable column contains a non-NULL value, deduplication works
correctly.

## Schema

```sql
CREATE TABLE measurements (
    time            TIMESTAMPTZ      NOT NULL,
    col_a           TEXT             NOT NULL,
    col_b           TEXT             NOT NULL,
    col_c           INTEGER          NOT NULL,
    col_d           DOUBLE PRECISION,
    col_e           TEXT,
    col_f           TEXT,
    nullable_col    TEXT,                        -- the problem column
    UNIQUE (time, col_a, col_b, col_c, col_e, nullable_col)
);

SELECT create_hypertable('measurements', by_range('time'));

ALTER TABLE measurements
    SET (timescaledb.compress,
         timescaledb.compress_segmentby = 'col_a, col_b',
         timescaledb.compress_orderby   = 'time DESC');
```

## How to reproduce

1. Insert rows into the hypertable (some with `nullable_col = NULL`).
2. Compress the chunk.
3. Re-insert the exact same rows with `ON CONFLICT DO NOTHING`.
4. Count the rows — duplicates now exist for every row where
   `nullable_col IS NULL`.

## Test suite

The repo contains four NUnit integration tests that run against a real
TimescaleDB instance (`timescale/timescaledb:2.24.0-pg18`) via
[Testcontainers](https://dotnet.testcontainers.org/).

All four tests assert the **correct** behaviour (`row count == 3` after
re-insert). Two of them fail, proving the bug:

| Test | nullable_col | Result |
|------|-------------|--------|
| `...WithNullableColNull` (multi-row insert) | `NULL` | **Fails** — duplicates created |
| `...WithNullableColNull_SingleRowInserts` | `NULL` | **Fails** — duplicates created |
| `...WithNullableColNotNull` (multi-row insert) | non-NULL | Passes |
| `...WithNullableColNotNull_SingleRowInserts` | non-NULL | Passes |

### Prerequisites

- [.NET 9 SDK](https://dotnet.microsoft.com/download)
- [Docker](https://www.docker.com/) (for Testcontainers)

### Running the tests

```bash
dotnet test
```

## Tested version

- **TimescaleDB** 2.24.0
- **PostgreSQL** 18
