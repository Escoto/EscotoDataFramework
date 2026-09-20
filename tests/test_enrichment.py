"""Tests for pipelines.enrichment — provenance, sanitization, rename patterns."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from data_framework.pipelines.enrichment import (
    add_provenance,
    apply_rename_patterns,
    sanitize_column_names,
)


@pytest.fixture
def ctx() -> MagicMock:
    """Only ctx.logger is touched by enrichment."""
    return MagicMock()


def _read_csv(spark, tmp_path, filename: str):
    path = tmp_path / filename
    path.write_text("id,name\n1,alice\n", encoding="utf-8")
    return spark.read.option("header", "true").csv(str(path))


def test_provenance_columns_added(spark, tmp_path):
    df = _read_csv(spark, tmp_path, "AGENTS_20240115103000.csv")

    row = add_provenance(df).collect()[0]

    assert row["__filePath"].endswith("AGENTS_20240115103000.csv")
    assert row["__EXPORT_DATE"] == datetime(2024, 1, 15, 10, 30, 0)
    assert row["__bronze_last_modified_dt"] is not None


def test_export_date_is_null_when_the_name_has_no_stamp(spark, tmp_path):
    """A stampless file still loads; the missing export date is a NULL, not a failure."""
    df = _read_csv(spark, tmp_path, "AGENTS.csv")

    assert add_provenance(df).collect()[0]["__EXPORT_DATE"] is None


def test_provenance_keeps_the_source_columns(spark, tmp_path):
    df = _read_csv(spark, tmp_path, "AGENTS_20240115103000.csv")

    result = add_provenance(df)

    assert result.columns[:2] == ["id", "name"]
    assert result.collect()[0]["name"] == "alice"


def test_sanitize_replaces_unsafe_chars_and_uppercases(spark, ctx):
    df = spark.createDataFrame([(1, 2, 3, 4, 5)]).toDF("order id", "a,b", "c;d", "e.f", "g/h")

    result = sanitize_column_names(df, ctx)

    assert result.columns == ["ORDER_ID", "A_B", "C_D", "E_F", "G_H"]


def test_sanitize_preserves_values(spark, ctx):
    df = spark.createDataFrame([("1", "alice")]).toDF("agent id", "name")

    row = sanitize_column_names(df, ctx).collect()[0]

    assert row["AGENT_ID"] == "1"
    assert row["NAME"] == "alice"


def test_sanitize_drops_rescued_data(spark, ctx):
    df = spark.createDataFrame([(1, "x")]).toDF("id", "_rescued_data")

    assert sanitize_column_names(df, ctx).columns == ["ID"]


def test_sanitize_logs_only_the_columns_it_changed(spark, ctx):
    df = spark.createDataFrame([(1, 2)]).toDF("order id", "NAME")

    sanitize_column_names(df, ctx)

    logged = ctx.logger.info.call_args
    assert logged.kwargs["name"] == "columns_sanitized"
    assert logged.kwargs["total"] == 1
    assert json.loads(logged.kwargs["metadata"]) == {"order id": "ORDER_ID"}


def test_sanitize_logs_nothing_when_names_are_already_clean(spark, ctx):
    """Silence in the rename log must mean 'no renames', not 'not logged'."""
    df = spark.createDataFrame([(1, 2)]).toDF("ID", "NAME")

    result = sanitize_column_names(df, ctx)

    assert result.columns == ["ID", "NAME"]
    ctx.logger.info.assert_not_called()


def test_provenance_survives_sanitization_as_the_metadata_contract(spark, tmp_path, ctx):
    df = _read_csv(spark, tmp_path, "AGENTS_20240115103000.csv")

    result = sanitize_column_names(add_provenance(df), ctx)

    assert "__FILEPATH" in result.columns
    assert "__BRONZE_LAST_MODIFIED_DT" in result.columns
    assert "__EXPORT_DATE" in result.columns


def test_rename_patterns_strip_a_suffix(spark):
    df = spark.createDataFrame([(1, 2)]).toDF("AGENT__V", "NAME")

    assert apply_rename_patterns(df, ["__[Vv]$="]).columns == ["AGENT", "NAME"]


def test_rename_patterns_are_applied_in_order(spark):
    df = spark.createDataFrame([(1,)]).toDF("PREFIX_AGENT__V")

    result = apply_rename_patterns(df, ["__[Vv]$=", "^PREFIX_="])

    assert result.columns == ["AGENT"]


def test_rename_patterns_can_substitute_not_only_delete(spark):
    df = spark.createDataFrame([(1,)]).toDF("AGENT_OLD")

    assert apply_rename_patterns(df, ["_OLD$=_CURRENT"]).columns == ["AGENT_CURRENT"]


def test_no_patterns_returns_the_input_untouched(spark):
    df = spark.createDataFrame([(1,)]).toDF("AGENT")

    assert apply_rename_patterns(df, []) is df
