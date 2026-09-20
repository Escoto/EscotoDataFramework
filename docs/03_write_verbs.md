# 03 — Write Verbs

The Output layer writes with one of five verbs. Verbs are **layer-agnostic**: any origin can pair with any verb whose declared requirements are satisfied (matrix in [02_config_schema.md](02_config_schema.md) §3). All Delta writes preserve the metadata contract below.

## 0. Metadata contract (all history-tracking verbs)

| Column | Set by | Meaning |
|---|---|---|
| `__EXPORT_DATE` | Pipeline | Source export timestamp (from file name) |
| `__FILEPATH` | Pipeline | Source file the record arrived from |
| `__BRONZE_LAST_MODIFIED_DT` | Pipeline | Bronze ingest time (dropped on promotion) |
| `__SILVER_LAST_MODIFIED_DT` | Output | Last framework write touching the record |
| `__START_DATE` | Output | Validity start of the history row |
| `__END_DATE` | Output | Validity end (NULL = open) |
| `__CURRENT_FLAG` | Output | `'Y'` current version / `'N'` historical |
| `__DELETED_FLAG` | Output | `'Y'` soft-deleted entity |

---

## 1. APPEND

> *Get the latest from a layer and load it into the next.* Typical: Inbound→Bronze. Also valid Bronze→Silver or Gold→Export.

- **Requires**: target only.
- **Semantics**: write incoming records to the target with Delta `append` (with `mergeSchema` when schema evolution is enabled). No keys, no history columns beyond what the Pipeline added.
- **Increments**: file origins via Auto Loader checkpoint; delta origin via streaming checkpoint.
- KPI: `bronze_new_records` / `silver_new_records` depending on target layer.

## 2. FULL

> *Snapshot and overwrite.* Typical: Gold→Export, reference tables, full-refresh feeds.

- **Requires**: target only.
- **Semantics**: Delta `overwrite` of the target with the current dataset (with `mergeSchema`). Empty input → **skip** with an audit log entry. A source that produced nothing is a run with no news, not an instruction to empty the table.

## 3. UPSERT — *new in the rewrite* (SCD Type 1)

> *Latest state per key, no history.* The GOLD-layer builder.

- **Requires**: `output.keys`. Optional: `output.dedup.*` (recommended when the source may carry several versions per key in one batch), `output.event_time.*` (when present, "newer wins" uses it; otherwise last write wins).
- **Semantics**: Delta `MERGE` on the keys —
  - matched → update all columns (when `event_time` configured: only if source is newer);
  - not matched → insert.
- No `__START_DATE/__END_DATE/__CURRENT_FLAG/__DELETED_FLAG` columns; `__SILVER_LAST_MODIFIED_DT` is still maintained.
- Target absent → create via append.

## 4. SCD2

> *Latest from one layer to the next, history-tracked by keys and date.*

- **Requires**: `output.keys`, `output.event_time.column` (+ `.format` if it is a string column).
- **Optional**: `output.dedup.*`, `output.snapshot_scope` (§6).
- **No deletes feed**: SCD2 runs per batch, and a stream has no way to cut a second source at the same point as its updates. A source that sends deletes separately belongs on COMPLETE_DELTA; a full-snapshot source expresses deletion by omission (§6).
- **Increments**: delta origin with streaming checkpoint (each micro-batch flows through the algorithm below); file origins supported the same way.

**Algorithm** (per batch):

1. Optional rename patterns; event-time/dedup-column normalization to timestamp. These normalized columns are held internally and never persisted, so the target schema stays the one the source defines.
2. Optional dedup: keep the latest row per `dedup.columns` ordered by `dedup.order_by` desc.
3. Add `__SILVER_LAST_MODIFIED_DT`; drop `__BRONZE_LAST_MODIFIED_DT`.
4. Target absent → create with metadata init (`__START_DATE` = event_time or now, `__END_DATE` = NULL, flags Y/N).
5. Target present:
   a. **Anti-filter**: drop source rows whose event_time ≤ the target's current row's event_time for the same keys (idempotent re-runs, late/duplicate files are no-ops).
   b. **Close**: Delta merge — matched current, not-deleted rows with older event_time get `__END_DATE` = source event_time, `__CURRENT_FLAG` = 'N'.
   c. **Insert**: surviving source rows appended as new current rows.

Note: SCD2 collapses to *latest per key within the processed increment* (step 2). If the increment contains v1→v2→v3 of the same key, Silver records the transition current-state → v3. When **every** intermediate version must appear in history, use COMPLETE_DELTA.

## 5. COMPLETE_DELTA

> *Everything not yet promoted, replayed step by step — every evolution of the data is represented in Silver.*

- **Requires**: delta origin (`source.table` = updates table), `output.keys`, `output.event_time.column`, `source.snapshot_time_pattern` (`datetime` | `timestamp`).
- **Optional**: deletes feed (§7), `output.dedup.*` (applied per snapshot), `output.snapshot_scope` (§6).
- **Increments**: watermark — only source rows with `__EXPORT_DATE > max(target.__EXPORT_DATE)` (a typed timestamp comparison, not a string one; the watermark defaults to 1900-01-01 when the target is empty or absent).

**Algorithm**:

1. Read updates (and deletes, if configured) newer than the watermark.
2. Derive a **snapshot id** per source file from the timestamp embedded in `__FILEPATH` (pattern per `snapshot_time_pattern`; files sharing a timestamp are disambiguated by rank).
3. Order snapshots chronologically; **for each snapshot, in order**: run the SCD2 merge (§4 steps 1–5) for its updates, then apply its deletes (§7).
4. No snapshots + missing target → create the empty target (schema from source).

### Worked example — why replay matters

Source exports three files for table `SUBJECTS` (key `ID`, event time `UPDATEDTIME`):

| File (snapshot) | Content |
|---|---|
| `subjects_20260101120000.csv` | `A` v1 (`UPDATEDTIME` 2026-01-01), `B` v1 |
| `subjects_20260102120000.csv` | `A` v2 (2026-01-02) — and deletes file: `B` (deleted 2026-01-02) |
| `subjects_20260103120000.csv` | `A` v3 (2026-01-03) |

All three land in Bronze before the Silver task runs (a backlog — weekend, reprocessing, new table). COMPLETE_DELTA replays snapshot 1, then 2, then 3. Silver afterwards:

| ID | payload | __START_DATE | __END_DATE | __CURRENT_FLAG | __DELETED_FLAG |
|----|---------|--------------|------------|----------------|----------------|
| A | v1 | 2026-01-01 | 2026-01-02 | N | N |
| A | v2 | 2026-01-02 | 2026-01-03 | N | N |
| A | v3 | 2026-01-03 | NULL | Y | N |
| B | v1 | 2026-01-01 | 2026-01-02 | N | **Y** |

Every evolution is represented: A's full version chain with correct validity windows, and B's life-and-deletion. Plain SCD2 over the same backlog would dedup to the latest per key and produce only `A v3 (current)` — A's v1→v2 transitions and B's existence would never reach Silver. **This is the "everything from one layer to the next, not only the very latest" requirement.**

## 6. Modifier: `snapshot_scope` (SCD2, COMPLETE_DELTA)

- **`delta`** (default): the source sends only changes. Records absent from an increment are simply untouched.
- **`full`**: the source sends the complete dataset every time. Before merging a snapshot, **all** current rows in the target are expired (`__CURRENT_FLAG`='N', `__END_DATE`=now). The merge then re-inserts what the snapshot contains — anything absent stays expired. This is how implicit deletes work for full-snapshot sources.
- Re-run with no new data: nothing is expired and nothing merged — the watermark and the anti-filter together make the whole run a no-op. Re-running a job must never change the table.

### Worked example

Target has current rows `A, B, C`. A `snapshot_scope: full` load arrives containing `A (unchanged), B (changed), D (new)`:

| ID | Outcome |
|----|---------|
| A | old row expired, identical new current row inserted (full-snapshot sources re-assert every record) |
| B | old row expired; new current row with changed payload |
| C | expired, **no** new row — implicitly deleted (stays `__DELETED_FLAG='N'`, it simply has no current version) |
| D | new current row |

## 7. Modifier: deletes feed (COMPLETE_DELTA)

A second Delta table (`source.deletes_table`) carrying delete records, with its own `output.deletes.keys` and `output.deletes.event_time.*` — all three are required together. Applied after the updates of the same snapshot. Soft-delete semantics, via a double merge:

1. every key match in the target → `__DELETED_FLAG` = 'Y' (all history rows of the entity are flagged);
2. the open current row (`__END_DATE IS NULL AND __CURRENT_FLAG='Y'`) → `__END_DATE` = delete event time, `__CURRENT_FLAG` = 'N', `__SILVER_LAST_MODIFIED_DT` = now.

No physical deletes, ever — history is preserved.

## 8. Verb selection guide

| Scenario | Verb |
|---|---|
| Land raw files into Bronze | `append` (or `full` for full-refresh drops) |
| Bronze→Silver, source sends change feeds, all history must be visible | `complete_delta` |
| Bronze→Silver, current-state tracking with history, latest per batch is enough | `scd2` |
| Source sends complete snapshots and absent = deleted | `scd2`/`complete_delta` + `snapshot_scope: full` |
| Silver→Gold business tables (latest state only) | `upsert` |
| Gold→Export, reference full refresh | `full` |
