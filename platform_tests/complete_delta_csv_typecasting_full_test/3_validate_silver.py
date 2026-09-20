"""Step 3 — the same week of snapshots under snapshot_scope=full.

Identical fixtures to the `delta` variant, one parameter different, and a very
different silver. Under `full` the source is treated as re-asserting its entire
dataset each time, so before every snapshot merges, *every* current row is expired.
A record the snapshot no longer contains therefore stays closed — that is how
deletion-by-omission works — but a record it re-sends unchanged is closed and
re-inserted anyway.

The cost of that shows up in TST_ST_111: unchanged all week, yet it lands 21 rows
here against 3 under `delta`. Both are correct; they answer different questions about
what an absent record means. What they agree on is the current state — the nine open
rows are identical either way.

Note that expired rows here close at the moment the job ran, not at an event time.
Nothing in the data says when a record stopped being true; only that a later
snapshot superseded it.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    ENROLLMENT_HOUR,
    FULL_UPDATE_HOUR,
    MILESTONES,
    ROWS_PER_SNAPSHOT,
    SILVER_COLUMNS,
    SILVER_TABLE,
    SNAPSHOTS,
    STATIC_START,
    STUDIES,
    expect_columns,
    expect_rows,
    expect_types,
    milestone,
    qualified,
    started_at,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

assert spark.catalog.tableExists(qualified(SILVER_TABLE)), "silver table was not created"
df = spark.table(qualified(SILVER_TABLE))

expect_columns(df, SILVER_COLUMNS)
expect_types(df)

# Every record re-asserted by every snapshot: 9 x 7, of which the last 9 are open.
expect_rows(df, ROWS_PER_SNAPSHOT * SNAPSHOTS)
assert df.filter(df["__CURRENT_FLAG"] == "Y").count() == 9, "expected 9 current records"
assert df.filter(df["__CURRENT_FLAG"] == "N").count() == 54, "expected 54 expired records"
assert df.filter(df["__DELETED_FLAG"] == "Y").count() == 0, "omission is not a soft delete"
assert df.filter(df["__START_DATE"].isNull()).count() == 0, "UPDATEDATE failed to parse"

# Every milestone of every study carries one version per snapshot, exactly one open.
for study in STUDIES:
    for code in MILESTONES:
        versions = milestone(df, study, code)
        assert len(versions) == SNAPSHOTS, f"{study}/{code}: {len(versions)} rows, expected 7"
        open_rows = [row for row in versions if row["__CURRENT_FLAG"] == "Y"]
        assert len(open_rows) == 1, f"{study}/{code}: {len(open_rows)} open rows, expected 1"

# --- The churn `full` buys, stated plainly ---------------------------------
# Unchanged all week, and still re-versioned seven times. Under `delta` this same
# study holds 3 rows.
assert df.filter(df["STUDYID"] == "TST_ST_111").count() == 21, "expected full-scope churn"

# Those seven versions are genuinely identical: the record never moved, so every one
# of them opens at the same instant its only real edit did.
for code in MILESTONES:
    starts = {str(row["__START_DATE"]) for row in milestone(df, "TST_ST_111", code)}
    assert starts == {STATIC_START}, f"TST_ST_111/{code} start dates drifted: {starts}"

# Expiry here is a write-time event, not a data event: nothing in the source says
# when these stopped being true, so they close at the clock rather than at an
# UPDATEDATE. Every close therefore lands after every event time in the fixture.
latest_event = df.agg({"__START_DATE": "max"}).collect()[0][0]
stale = df.filter((df["__CURRENT_FLAG"] == "N") & (df["__END_DATE"] <= latest_event)).count()
assert stale == 0, f"{stale} expired rows closed at an event time rather than the write time"

# --- The current state matches the `delta` variant exactly -----------------
current = df.filter(df["__CURRENT_FLAG"] == "Y")

# All nine open rows were written by the final snapshot.
exports = current.select("__EXPORT_DATE").distinct().collect()
assert len(exports) == 1, f"open rows span {len(exports)} exports, expected only the last"

# Open versions still open at the event time the record itself carries.
for study, code, hour in (
    ("TST_ST_246", "ENR", ENROLLMENT_HOUR),
    ("TST_ST_782", "FPI", FULL_UPDATE_HOUR),
):
    row = [r for r in milestone(df, study, code) if r["__CURRENT_FLAG"] == "Y"][0]
    expected = started_at(SNAPSHOTS - 1, hour)
    assert str(row["__START_DATE"]) == expected, f"{study}/{code}: {row['__START_DATE']}"


# Same final values as the delta variant: the two scopes disagree about history,
# never about the present.
def current_value(study: str, code: str):
    row = [r for r in milestone(df, study, code) if r["__CURRENT_FLAG"] == "Y"][0]
    return row


assert str(current_value("TST_ST_246", "ENR")["MILESTONEVALUE"]) == "230.00"
assert str(current_value("TST_ST_782", "ENR")["MILESTONEVALUE"]) == "68.00"
assert str(current_value("TST_ST_782", "FPI")["MILESTONEDATE"]) == "2026-03-07"
assert str(current_value("TST_ST_111", "ENR")["MILESTONEVALUE"]) == "120.00"

print("silver full-scope replay validated")
