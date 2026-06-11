# Querying the S4 Logs archive with Amazon Athena

S4 Logs data objects are **standard RFC 8878 zstd frames** containing JSONL,
laid out under Hive-style partitions:

```
{prefix}data/account={acct}/loggroup={g}/dt=YYYY-MM-DD/{name}.jsonl.zst
```

Athena reads zstd transparently (it keys off the `.zst` extension) and reads
JSON via the OpenX JSON SerDe. The only wrinkle is **partitions**: `loggroup`
values are **percent-encoded** (e.g. `/aws/lambda/foo` →
`%2Faws%2Flambda%2Ffoo`), which trips up `MSCK REPAIR TABLE` on some Athena
versions/tooling.

This doc gives two table definitions:

1. **Partition projection** (recommended) — Athena *computes* partitions from
   config; **no crawler, no `MSCK REPAIR`, no `ADD PARTITION`**.
2. **Explicit `ADD PARTITION`** — what we actually ran end-to-end against real
   Athena (see *Verification* below).

---

## 1. Partition projection (no MSCK, recommended)

With [partition projection](https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html)
Athena derives partition values from `TBLPROPERTIES` instead of a metastore
listing — so the percent-encoding problem disappears (you never run a repair
or crawler) and queries don't pay a `GetPartitions` round-trip.

```sql
CREATE EXTERNAL TABLE s4logs_archive (
  `timestamp` bigint,
  stream string,
  message string,
  ingestion_time bigint,
  event_id string
)
PARTITIONED BY (account string, loggroup string, dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://YOUR_BUCKET/s4logs/data/'
TBLPROPERTIES (
  'projection.enabled' = 'true',

  -- account: the 12-digit AWS account id(s) under data/. `injected` means
  -- you MUST constrain it in the WHERE clause (Athena injects the literal you
  -- supply); this avoids enumerating accounts you don't query.
  'projection.account.type' = 'injected',

  -- loggroup: values are PERCENT-ENCODED on S3 (e.g. %2Faws%2Flambda%2Ffoo).
  -- `injected` is the cleanest fit: you query by the exact encoded value, and
  -- Athena uses your literal directly as the partition path segment. (An
  -- `enum` would force you to hard-code the full encoded list in DDL and
  -- re-ALTER it whenever a new log group appears.)
  'projection.loggroup.type' = 'injected',

  -- dt: a real date partition. Athena enumerates the range itself, so range
  -- predicates (BETWEEN, >=) prune to just the days they touch.
  'projection.dt.type' = 'date',
  'projection.dt.format' = 'yyyy-MM-dd',
  'projection.dt.range' = '2024-01-01,NOW',
  'projection.dt.interval' = '1',
  'projection.dt.interval.unit' = 'DAYS',

  -- Tell Athena how to build the S3 path from the projected values. Must
  -- match the on-disk layout exactly (the literal `loggroup=` etc.).
  'storage.location.template' =
    's3://YOUR_BUCKET/s4logs/data/account=${account}/loggroup=${loggroup}/dt=${dt}/'
);
```

Query it — note `injected` columns (`account`, `loggroup`) **must** appear in
`WHERE` with the **percent-encoded** loggroup value:

```sql
SELECT from_unixtime("timestamp" / 1000) AS t, stream, message
FROM s4logs_archive
WHERE account = '123456789012'
  AND loggroup = '%2Faws%2Flambda%2Fpayments'   -- percent-encoded
  AND dt BETWEEN '2026-06-01' AND '2026-06-09'
  AND message LIKE '%ERROR%'
LIMIT 100;
```

### Why `injected` for account/loggroup, `date` for dt

- **`dt` = `date`**: dates are a dense, ordered range. The `date` projection
  lets Athena prune `dt BETWEEN ...` to exactly the days you ask for without
  you listing them. `range = '2024-01-01,NOW'` is a sane open-ended window;
  widen the lower bound if you have older data (projection only *describes*
  the keyspace — non-existent days just return no rows).
- **`account`/`loggroup` = `injected`**: their value sets are sparse and
  user-specific, and `loggroup` is percent-encoded. `injected` means "I'll
  give you the exact literal in the query", which (a) requires no DDL changes
  when a new account/log group appears and (b) sidesteps the encoding issue —
  you pass the same encoded string that's on S3. The trade-off: you **must**
  filter on them (a query without `account`/`loggroup` in `WHERE` errors).
  That's usually what you want anyway — you query one log group at a time.

### Alternative: a decoded, queryable loggroup column

If typing percent-encoded values is unpleasant, project `loggroup` as the raw
(encoded) partition for path-building but expose a human-readable column via a
view:

```sql
CREATE OR REPLACE VIEW s4logs_archive_v AS
SELECT *, url_decode(loggroup) AS loggroup_name
FROM s4logs_archive;
```

You still **filter** on the encoded `loggroup` in the WHERE (that's what
prunes partitions), but `loggroup_name` is readable in the SELECT output.
Athena's `url_decode` handles the `%2F` → `/` mapping.

---

## 2. Explicit `ADD PARTITION` (what we ran end-to-end)

This is the form the 2026-06-10 real-AWS experiment used. The table `LOCATION`
points at a single `loggroup=` directory and you add each `dt=` partition
explicitly — no `MSCK`, no projection, no encoding ambiguity:

```sql
CREATE EXTERNAL TABLE s4logs_one_group (
  `timestamp` bigint,
  stream string,
  message string,
  ingestion_time bigint,
  event_id string
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://YOUR_BUCKET/s4logs/data/account=123456789012/loggroup=%2Faws%2Flambda%2Fpayments/';

ALTER TABLE s4logs_one_group
  ADD IF NOT EXISTS PARTITION (dt = '2026-06-09')
  LOCATION 's3://YOUR_BUCKET/s4logs/data/account=123456789012/loggroup=%2Faws%2Flambda%2Fpayments/dt=2026-06-09/';

SELECT count(*) FROM s4logs_one_group WHERE dt = '2026-06-09';
```

---

## Verification status (honesty bar)

- **Explicit `ADD PARTITION` form (§2): verified end-to-end on real Athena**
  (2026-06-10). Against a real 41-object / 1.6 GiB archive, `count(*)`
  returned exactly the drained record count, and a `LIKE` query with
  partition pruning scanned 1.68 GB / 78.6 MB (full vs pruned). Cost Explorer
  later confirmed the Athena scan line ($0.008 at $5/TB).
- **Partition-projection form (§1): NOT yet run on real Athena.** The
  projection config is standard, documented Athena syntax and we validated it
  for **parse-correctness only** (DDL shape, property names, the
  `storage.location.template` against the real on-disk layout). It has not
  been executed against a live Athena endpoint. If you adopt it, sanity-check
  `count(*)` against a known window before relying on it.
- `MSCK REPAIR TABLE` is **not recommended** for the multi-loggroup table: the
  percent-encoded `loggroup=` values trip it on some Athena versions. Use
  projection (§1) or explicit `ADD PARTITION` (§2) instead.
