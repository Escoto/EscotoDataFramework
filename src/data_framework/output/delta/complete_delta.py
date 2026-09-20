"""COMPLETE_DELTA verb — replay every pending snapshot, in order, through SCD2.

Plain SCD2 keeps the latest version per key within whatever it happens to process, so
a backlog of three exports collapses to the newest one and the intermediate states never
reach Silver. This verb splits the increment back into the snapshots it arrived as and
merges them one at a time, which is what makes every evolution of a record visible.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from data_framework.context.config import (
    IncrementStrategy,
    Origin,
    SnapshotTimePattern,
    Verb,
)
from data_framework.output.base import Requirements
from data_framework.output.delta.scd2 import merge_history
from data_framework.output.mechanics import (
    CURRENT,
    CURRENT_FLAG,
    DELETED,
    DELETED_FLAG,
    END_DATE,
    EXPIRED,
    FILE_PATH,
    SILVER_TIMESTAMP,
    as_timestamp,
    key_condition,
)
from data_framework.pipelines.delta_source import DeltaSource

if TYPE_CHECKING:
    from datetime import datetime

    from pyspark.sql import Column, DataFrame

    from data_framework.context.context import Context

# The timestamp a source stamps into its file names, in the two shapes the interfaces
# produce. Group 1 is the date, group 2 the time, kept apart so one format string can
# parse both halves after they are rejoined with a separator.
_PATTERNS: dict[SnapshotTimePattern, tuple[str, str]] = {
    SnapshotTimePattern.DATETIME: (r"(\d{8})(\d{6})", "yyyyMMdd HHmmss"),
    SnapshotTimePattern.TIMESTAMP: (
        r"(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})",
        "yyyy-MM-dd HH:mm:ss",
    ),
}

# Internal only: used to split the increment, dropped before anything is written.
_SNAPSHOT = "__snapshot"

_SOURCE = "CompleteDeltaWriter"


class SnapshotPatternError(Exception):
    """Raised when a source file name carries no timestamp the configured pattern matches."""


class CompleteDeltaWriter:
    verb: ClassVar[Verb] = Verb.COMPLETE_DELTA
    requires: ClassVar[Requirements] = Requirements(
        keys=True,
        event_time=True,
        snapshot_time_pattern=True,
        supports_deletes=True,
        supports_snapshot_scope=True,
        # snapshot replay needs a Delta updates table; file origins cannot feed it
        origins=frozenset({Origin.DELTA}),
        # Watermark only: replay has to see every snapshot, so a strategy that
        # keeps just the newest one would defeat the purpose of the verb.
        increment_strategies=(IncrementStrategy.WATERMARK,),
    )

    def write(self, df: DataFrame, ctx: Context) -> None:
        updates = _with_snapshot(df, ctx)
        # The deletes feed is read here rather than by the pipeline spine because only
        # this verb has one. Both reads cut at the same point: the watermark comes from
        # the target, and nothing has been written to it yet.
        deletes = _read_deletes(ctx)

        snapshots = _ordered_snapshots(updates, deletes, ctx)
        if not snapshots:
            # Nothing pending. Hand the empty frame to the engine anyway: on a first run
            # that is what creates the target, so downstream readers find an empty table
            # rather than a missing one.
            merge_history(df, ctx)
            return

        ctx.logger.info(
            name="snapshots_to_replay",
            source=_SOURCE,
            total=len(snapshots),
            description=f"Replaying {len(snapshots)} snapshot(s) into {ctx.target_table}",
        )

        for snapshot in snapshots:
            merge_history(_at(updates, snapshot), ctx)
            if deletes is not None:
                _apply_deletes(_at(deletes, snapshot), ctx)


def _snapshot_time(ctx: Context) -> Column:
    """The export timestamp embedded in the source file's name."""
    pattern = ctx.config.source.snapshot_time_pattern
    assert pattern  # guaranteed by Requirements(snapshot_time_pattern=True)
    regex, fmt = _PATTERNS[pattern]

    path = F.col(f"`{FILE_PATH}`")
    date = F.regexp_extract(path, regex, 1)
    time = F.regexp_extract(path, regex, 2)
    return F.to_timestamp(F.concat_ws(" ", date, time), fmt)


def _with_snapshot(df: DataFrame, ctx: Context) -> DataFrame:
    """Tag each record with the snapshot it arrived in.

    The timestamp alone identifies the snapshot: two exports cannot share one, because
    the file names would collide and the second would overwrite the first.
    """
    return df.withColumn(_SNAPSHOT, _snapshot_time(ctx))


def _read_deletes(ctx: Context) -> Optional[DataFrame]:
    deletes = DeltaSource().read_deletes(ctx)
    return None if deletes is None else _with_snapshot(deletes, ctx)


def _ordered_snapshots(
    updates: DataFrame, deletes: Optional[DataFrame], ctx: Context
) -> list[datetime]:
    """Every pending snapshot, oldest first — the order history has to be rebuilt in."""
    stamps = updates.select(_SNAPSHOT)
    if deletes is not None:
        stamps = stamps.union(deletes.select(_SNAPSHOT))

    found = [row[0] for row in stamps.distinct().orderBy(_SNAPSHOT).collect()]
    if any(stamp is None for stamp in found):
        # Silently dropping these would lose a whole export. The pattern is configuration,
        # so a mismatch is a setup error worth failing on.
        pattern = ctx.config.source.snapshot_time_pattern
        assert pattern  # guaranteed by Requirements(snapshot_time_pattern=True)
        raise SnapshotPatternError(
            f"Some files under {ctx.source_table} carry no timestamp matching "
            f"source.snapshot_time_pattern={pattern.value} "
            f"(expected {_PATTERNS[pattern][0]} in {FILE_PATH})"
        )
    return found


def _at(df: DataFrame, snapshot: datetime) -> DataFrame:
    return df.filter(F.col(_SNAPSHOT) == F.lit(snapshot)).drop(_SNAPSHOT)


def _delete_time(ctx: Context, alias: str | None = None) -> Column:
    configured = ctx.config.output.deletes
    assert configured and configured.event_time  # guaranteed by config validation
    return as_timestamp(configured.event_time.column, configured.event_time.format, alias)


def _apply_deletes(df: DataFrame, ctx: Context) -> None:
    """Soft-delete the entities this snapshot retired. History is never removed.

    Two merges, because they touch different row sets: the flag marks the entity across
    all of its versions, while the window only closes on the one still open.
    """
    if not ctx.spark.catalog.tableExists(ctx.target_table):
        return
    if df.isEmpty():
        return

    configured = ctx.config.output.deletes
    assert configured  # guaranteed by config validation
    matched = key_condition(configured.keys, "s", "t")

    target = DeltaTable.forName(ctx.spark, ctx.target_table)
    target.alias("t").merge(df.alias("s"), matched).whenMatchedUpdate(
        set={f"`{DELETED_FLAG}`": F.lit(DELETED)}
    ).execute()

    still_open = (F.col(f"t.`{END_DATE}`").isNull()) & (F.col(f"t.`{CURRENT_FLAG}`") == CURRENT)
    target.alias("t").merge(df.alias("s"), matched).whenMatchedUpdate(
        condition=still_open,
        set={
            f"`{END_DATE}`": _delete_time(ctx, "s"),
            f"`{CURRENT_FLAG}`": F.lit(EXPIRED),
            f"`{SILVER_TIMESTAMP}`": F.current_timestamp(),
        },
    ).execute()

    ctx.logger.kpi(
        name="data_retirement",
        total=df.count(),
        description=f"{_SOURCE} soft-deleted records in {ctx.target_table}",
    )
