"""Tests for pipelines.delta_source — checkpoint and watermark increments."""

from __future__ import annotations

import uuid
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from data_framework.context.config import (
    EventTimeConfig,
    IncrementStrategy,
    Origin,
    OutputConfig,
    SourceConfig,
    TaskConfig,
    Verb,
)
from data_framework.context.context import Context, RunIdentity
from data_framework.pipelines.delta_source import DEFAULT_WATERMARK, DeltaSource

RUN = RunIdentity(
    workflow_id="wf-1",
    workflow_run_id="wfrun-1",
    task_key="bronze_to_silver",
    task_run_id="taskrun-1",
)

SCHEMA = "ID string, __EXPORT_DATE timestamp"


@pytest.fixture
def database(spark):
    name = f"test_delta_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{name}`")
    yield name
    spark.sql(f"DROP DATABASE IF EXISTS `{name}` CASCADE")


def _write(spark, database, table, rows):
    spark.createDataFrame(rows, SCHEMA).write.format("delta").mode("overwrite").saveAsTable(
        f"`{database}`.`{table}`"
    )


def _context(spark, database, strategy, *, deletes=None, **source_overrides):
    """A Context built by hand: build_context resolves Unity Catalog three-part names."""
    source = dict(origin=Origin.DELTA, schema_name="bronze_cro", table="UPDATES")
    source.update(source_overrides)
    config = TaskConfig(
        catalog="cro",
        env="dev_01",
        metadata_path="/Volumes/meta/",
        source=SourceConfig(**source),
        output=OutputConfig(verb=Verb.APPEND, schema_name="silver_cro", table="SUBJECTS"),
    )
    return Context(
        config=config,
        spark=spark,
        run=RUN,
        logger=MagicMock(),
        catalog="cro_dev_01",
        source_table=f"`{database}`.`UPDATES`",
        deletes_table=f"`{database}`.`{deletes}`" if deletes else None,
        target_table=f"`{database}`.`SUBJECTS`",
        inbound_glob=None,
        checkpoint_location="/tmp/checkpoint/",
        schema_hints_location="/tmp/hints/",
        increment_strategy=IncrementStrategy(strategy),
    )


def test_checkpoint_strategy_returns_a_stream(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.CHECKPOINT)

    result = DeltaSource().read(ctx)

    assert result.isStreaming


def test_watermark_strategy_returns_a_batch(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    result = DeltaSource().read(ctx)

    assert not result.isStreaming


def test_an_absent_target_takes_the_whole_source(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2)), ("2", datetime(1999, 5, 5))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    assert DeltaSource().read(ctx).count() == 2


def test_the_default_watermark_predates_any_export(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)
    source = DeltaSource()

    source.read(ctx)

    assert source._watermark == DEFAULT_WATERMARK


def test_only_rows_newer_than_the_target_are_read(spark, database):
    _write(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 3))])
    _write(
        spark,
        database,
        "UPDATES",
        [("1", datetime(2024, 1, 2)), ("2", datetime(2024, 1, 4))],
    )
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    rows = DeltaSource().read(ctx).collect()

    assert [row["ID"] for row in rows] == ["2"]


def test_a_row_exactly_on_the_watermark_is_excluded(spark, database):
    """The comparison is a typed, strictly-greater-than timestamp test."""
    boundary = datetime(2024, 1, 3, 12, 30, 45, 123000)
    _write(spark, database, "SUBJECTS", [("1", boundary)])
    _write(
        spark,
        database,
        "UPDATES",
        [("1", boundary), ("2", datetime(2024, 1, 3, 12, 30, 45, 124000))],
    )
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    rows = DeltaSource().read(ctx).collect()

    assert [row["ID"] for row in rows] == ["2"]


def test_an_empty_target_falls_back_to_the_default_watermark(spark, database):
    spark.createDataFrame([], SCHEMA).write.format("delta").saveAsTable(f"`{database}`.`SUBJECTS`")
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    assert DeltaSource().read(ctx).count() == 1


def test_no_deletes_feed_returns_none(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    assert DeltaSource().read_deletes(ctx) is None


def test_the_deletes_feed_is_filtered_like_the_updates(spark, database):
    _write(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 3))])
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 4))])
    _write(
        spark,
        database,
        "DELETES",
        [("9", datetime(2024, 1, 1)), ("8", datetime(2024, 1, 5))],
    )
    ctx = _context(spark, database, IncrementStrategy.WATERMARK, deletes="DELETES")

    rows = DeltaSource().read_deletes(ctx).collect()

    assert [row["ID"] for row in rows] == ["8"]


def test_the_deletes_feed_streams_under_the_checkpoint_strategy(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    _write(spark, database, "DELETES", [("9", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.CHECKPOINT, deletes="DELETES")

    assert DeltaSource().read_deletes(ctx).isStreaming


def test_updates_and_deletes_are_cut_at_the_same_watermark(spark, database):
    """Both feeds belong to one run; a target that moves mid-run must not split them."""
    _write(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 1))])
    _write(spark, database, "UPDATES", [("2", datetime(2024, 1, 2))])
    _write(spark, database, "DELETES", [("3", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK, deletes="DELETES")
    source = DeltaSource()

    assert source.read(ctx).count() == 1

    _write(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 5))])

    assert source.read_deletes(ctx).count() == 1


def test_the_resolved_watermark_is_logged(spark, database):
    _write(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 3))])
    _write(spark, database, "UPDATES", [("2", datetime(2024, 1, 4))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    DeltaSource().read(ctx)

    logged = ctx.logger.info.call_args
    assert logged.kwargs["name"] == "watermark_resolved"
    assert "2024-01-03" in logged.kwargs["description"]


def _drain(ctx, sink: str, checkpoint: str) -> None:
    query = (
        DeltaSource()
        .read(ctx)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start(sink)
    )
    query.awaitTermination()


def test_a_delete_on_the_source_does_not_break_the_stream(spark, database, tmp_path):
    """Without ignoreDeletes, a single DELETE kills the stream for good."""
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2)), ("2", datetime(2024, 1, 3))])
    ctx = _context(spark, database, IncrementStrategy.CHECKPOINT)
    sink = str(tmp_path / "sink")
    checkpoint = str(tmp_path / "checkpoint")

    _drain(ctx, sink, checkpoint)
    spark.sql(f"DELETE FROM `{database}`.`UPDATES` WHERE ID = '1'")
    _drain(ctx, sink, checkpoint)  # raises "Detected deleted data" without the option

    assert spark.read.format("delta").load(sink).count() == 2


SNAPSHOT_SCHEMA = "ID string, UPDATED_DATE string, __EXPORT_DATE timestamp"


def _snapshot(spark, database, table, rows, mode="append"):
    spark.createDataFrame(rows, SNAPSHOT_SCHEMA).write.format("delta").mode(mode).saveAsTable(
        f"`{database}`.`{table}`"
    )


def test_a_record_anchor_reads_nothing_when_no_record_changed(spark, database):
    """The other snapshot case: the source carries a per-record date.

    The anchor is then a property of the record, so an unchanged snapshot falls wholly
    below the watermark and never enters the pipeline at all.
    """
    _snapshot(spark, database, "SUBJECTS", [("1", "2024-01-01 00:00:00", datetime(2024, 1, 1))])
    for day in range(1, 6):
        _snapshot(
            spark, database, "UPDATES", [("1", "2024-01-01 00:00:00", datetime(2024, 1, day))]
        )
    ctx = _context(
        spark,
        database,
        IncrementStrategy.WATERMARK,
        increment_anchor=EventTimeConfig(column="UPDATED_DATE", format="yyyy-MM-dd HH:mm:ss"),
    )

    assert DeltaSource().read(ctx).count() == 0


def test_a_record_anchor_still_picks_up_a_changed_record(spark, database):
    _snapshot(spark, database, "SUBJECTS", [("1", "2024-01-01 00:00:00", datetime(2024, 1, 1))])
    _snapshot(spark, database, "UPDATES", [("1", "2024-01-01 00:00:00", datetime(2024, 1, 2))])
    _snapshot(spark, database, "UPDATES", [("2", "2024-03-01 00:00:00", datetime(2024, 1, 2))])
    ctx = _context(
        spark,
        database,
        IncrementStrategy.WATERMARK,
        increment_anchor=EventTimeConfig(column="UPDATED_DATE", format="yyyy-MM-dd HH:mm:ss"),
    )

    rows = DeltaSource().read(ctx).collect()

    assert [row["ID"] for row in rows] == ["2"]


def test_the_anchor_format_makes_the_comparison_chronological(spark, database):
    """'15/01/2024' sorts after '01/03/2024' as text, but before it in time."""
    _snapshot(spark, database, "SUBJECTS", [("1", "01/03/2024", datetime(2024, 3, 1))])
    _snapshot(spark, database, "UPDATES", [("2", "15/01/2024", datetime(2024, 3, 2))])
    ctx = _context(
        spark,
        database,
        IncrementStrategy.WATERMARK,
        increment_anchor=EventTimeConfig(column="UPDATED_DATE", format="dd/MM/yyyy"),
    )

    assert DeltaSource().read(ctx).count() == 0


def test_the_resolved_anchor_is_named_in_the_log(spark, database):
    _snapshot(spark, database, "SUBJECTS", [("1", "2024-01-01 00:00:00", datetime(2024, 1, 1))])
    _snapshot(spark, database, "UPDATES", [("1", "2024-01-02 00:00:00", datetime(2024, 1, 2))])
    ctx = _context(
        spark,
        database,
        IncrementStrategy.WATERMARK,
        increment_anchor=EventTimeConfig(column="UPDATED_DATE", format="yyyy-MM-dd HH:mm:ss"),
    )

    DeltaSource().read(ctx)

    assert "UPDATED_DATE" in ctx.logger.info.call_args.kwargs["description"]
