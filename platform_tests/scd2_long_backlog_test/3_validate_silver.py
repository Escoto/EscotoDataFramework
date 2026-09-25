"""Step 3 — the current set in silver must match export 3, the latest full truth.

This is expected to FAIL until CODE_REVIEW.md #6 ("SCD2 full scope merges a backlog
as one snapshot") is fixed. Keys 15 and 20 are in exports 1 and 2 but export 3 — the
newest, most authoritative full snapshot — no longer sends them, so they should not
be current. deduplicate() only collapses a key that repeats *within* the combined
batch; a key present in an earlier export but entirely absent from the latest one has
nothing to dedup against, so it survives as current instead of disappearing with the
export that dropped it.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    DROPPED_KEYS,
    SILVER_COLUMNS,
    SILVER_EXPECTED,
    SILVER_TABLE,
    current,
    expect_columns,
    expect_rows,
    qualified,
)
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

spark = SparkSession.builder.getOrCreate()

assert spark.catalog.tableExists(qualified(SILVER_TABLE)), "silver table was not created"
df = spark.table(qualified(SILVER_TABLE))

expect_columns(df, SILVER_COLUMNS)

live = current(df)

# The headline assertion: only export 3's 23 keys should be current.
expect_rows(live, SILVER_EXPECTED)

# Keys the latest full export dropped must not still be current.
for key in DROPPED_KEYS:
    survivors = live.filter(F.col("KEYSEQ") == key).count()
    assert survivors == 0, f"KEYSEQ {key} was dropped by export 3 but is still current"

# A key export 3 actually carries and updated must reflect its value, not an
# earlier export's.
completed = live.filter((F.col("KEYSEQ") == "11") & (F.col("MILESTONEVALUE") == "Completed"))
assert completed.count() == 1, "KEYSEQ 11 should be Completed per export 3"

# The new inserts from export 3 must be present.
assert live.filter(F.col("KEYSEQ") == "24").count() == 1, "new record from export 3 is missing"

print("silver validated")
