"""Constants and assertions shared by the upsert_csv_test steps.

Deliberately dependency-free: these run as spark_python_task on a cluster, not
under pytest, so a failed assert is what fails the task.
"""

CATALOG = "testing_dev_01"
SCHEMA = "functional_testing"

BRONZE_TABLE = "UPSERT_CSV_BRONZE_1"
SILVER_TABLE = "UPSERT_CSV_SILVER_1"

VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/source_data"
INBOUND = f"{VOLUME}/inbound"
METADATA = f"{VOLUME}/metadata"
SOURCE_DIRECTORY = "UPSERT_CSV_SOURCE_1"

BRONZE_COLUMNS = {
    "ID",
    "NAME",
    "SOURCE_SYSTEM",
    "CREATED_DATE",
    "__BRONZE_LAST_MODIFIED_DT",
    "__FILEPATH",
    "__EXPORT_DATE",
}

SILVER_COLUMNS = {
    "ID",
    "NAME",
    "SOURCE_SYSTEM",
    "CREATED_DATE",
    "__FILEPATH",
    "__EXPORT_DATE",
    "__SILVER_LAST_MODIFIED_DT",
}


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


def expect_value_count(df, column: str, value: str, count: int) -> None:
    actual = df.filter(df[column] == value).count()
    assert actual == count, f"expected {count} rows with {column}={value}, found {actual}"
