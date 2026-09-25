"""Constants and assertions shared by the scd2_long_backlog_test steps.

Deliberately dependency-free: these run as spark_python_task on a cluster, not
under pytest, so a failed assert is what fails the task.

Demonstrates CODE_REVIEW.md #6: SCD2 treats each micro-batch as one snapshot. All
three exports here land in inbound before the pipeline ever runs, so Auto Loader's
availableNow trigger (no maxFilesPerTrigger is set anywhere) hands SCD2's first-ever
run all three as one batch. deduplicate() only collapses a key that repeats *within*
that batch, so a key present in an earlier export but absent from the latest one —
KEYSEQ 15 and 20, dropped in export 3 — has nothing to dedup against and survives as
current, even though the newest full export no longer contains it.

The three exports are committed under sample_data/ rather than generated at run time
— fully synthetic (a fictional study, invented milestones), same shape as
full_csv_long_backlog_test's fixture, but with two keys the third export
deliberately drops.
"""

CATALOG = "testing_dev_01"
SCHEMA = "functional_testing"

BRONZE_TABLE = "STUDY_MILESTONE_SCD2_BACKLOG_BRONZE_1"
SILVER_TABLE = "STUDY_MILESTONE_SCD2_BACKLOG_SILVER_1"

VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/source_data"
INBOUND = f"{VOLUME}/inbound"
METADATA = f"{VOLUME}/metadata"
SOURCE_DIRECTORY = "STUDY_MILESTONE_SCD2_BACKLOG_SOURCE_1"

BUSINESS_COLUMNS = {
    "STUDYID",
    "SUBSTUDYID",
    "PART",
    "COHORT",
    "ARM",
    "MILESTONESEQ",
    "KEYSEQ",
    "KEYFLAG",
    "MILESTONENAME",
    "DISPLAYNAME",
    "MILESTONETYPE",
    "DISPLAYTYPE",
    "MILESTONEDATE",
    "MILESTONEVALUE",
    "MILESTONECODE",
    "DATASOURCE",
    "CREATEDBY",
    "REFRESHDATE",
    "DC_CONFIG_KEY",
}

BRONZE_COLUMNS = BUSINESS_COLUMNS | {
    "__BRONZE_LAST_MODIFIED_DT",
    "__FILEPATH",
    "__EXPORT_DATE",
}

SILVER_COLUMNS = BUSINESS_COLUMNS | {
    "__FILEPATH",
    "__EXPORT_DATE",
    "__SILVER_LAST_MODIFIED_DT",
    "__START_DATE",
    "__END_DATE",
    "__CURRENT_FLAG",
    "__DELETED_FLAG",
}

# The three exports as committed under sample_data/. The 14-digit stamp in each name
# is what __EXPORT_DATE is cut from — a week apart, so ordering is unambiguous.
EXPORT_1 = "STUDY_MILESTONE_20260901050000Z.csv"  # baseline: 20 keys, all pending
EXPORT_2 = "STUDY_MILESTONE_20260908050000Z.csv"  # resend of 20: keys 1-10 completed
EXPORT_3 = "STUDY_MILESTONE_20260915050000Z.csv"  # resend of 23: drops keys 15 & 20, 5 new
EXPORTS = (EXPORT_1, EXPORT_2, EXPORT_3)

# Keys 15 and 20 are in exports 1-2 but the source stopped sending them in export 3 —
# a real full-scope deletion. The correct current set is exactly export 3's 23 keys.
DROPPED_KEYS = ("15", "20")
SILVER_EXPECTED = 23


def qualified(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def expect_columns(df, expected: set) -> None:
    actual = set(df.columns)
    assert (
        actual == expected
    ), f"columns differ: missing={expected - actual} extra={actual - expected}"


def expect_rows(df, count: int) -> None:
    actual = df.count()
    assert actual == count, f"expected {count} rows, found {actual}"


def current(df):
    """Only the live version of each key."""
    return df.filter(df["__CURRENT_FLAG"] == "Y")
