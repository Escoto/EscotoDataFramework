"""Delta table origin — checkpoint, watermark, or latest-snapshot increments."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Optional

from pyspark.sql import functions as F

from data_framework.context.config import IncrementStrategy

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from data_framework.context.context import Context

# Earlier than any real export, so a first run takes the whole source.
DEFAULT_WATERMARK = datetime(1900, 1, 1)

_EXPORT_DATE = "__EXPORT_DATE"
_SOURCE = "DeltaSource"


def _anchor_column(ctx: Context) -> str:
    """The configured anchor's name, or the arrival timestamp the framework adds."""
    anchor = ctx.config.source.increment_anchor
    return anchor.column if anchor else _EXPORT_DATE


def _anchor(ctx: Context) -> Column:
    """The anchor as a timestamp, built identically for the source and the target.

    Bronze keeps unconfigured columns as strings, so a per-record business date needs
    its format to compare as a timestamp rather than lexically.
    """
    anchor = ctx.config.source.increment_anchor
    if anchor is None:
        return F.col(_EXPORT_DATE)
    column = F.col(f"`{anchor.column}`")
    return F.to_timestamp(column, anchor.format) if anchor.format else column


class DeltaSource:
    """Read from a Delta table using the run's configured increment strategy."""

    def __init__(self) -> None:
        # Updates and deletes must be cut at the same point, so the target's
        # watermark is read once and reused.
        self._watermark: Optional[datetime] = None

    def read(self, ctx: Context) -> DataFrame:
        table = ctx.source_table
        assert table  # guaranteed by SourceConfig for delta origins
        return self._read_table(ctx, table)

    def read_deletes(self, ctx: Context) -> Optional[DataFrame]:
        """Read the optional deletes feed table. Returns None if not configured."""
        table = ctx.deletes_table
        if not table:
            return None
        return self._read_table(ctx, table)

    def _read_table(self, ctx: Context, table: str) -> DataFrame:
        if ctx.increment_strategy == IncrementStrategy.CHECKPOINT:
            # Without this a single DELETE on the source breaks the stream for good.
            # It covers deletes only: a source that is updated or overwritten has no
            # incremental semantics to offer, and should fail loudly rather than be
            # silenced with skipChangeCommits, which would skip the changed data.
            return ctx.spark.readStream.option("ignoreDeletes", "true").table(table)
        if ctx.increment_strategy == IncrementStrategy.LATEST_SNAPSHOT:
            return self._latest_snapshot(ctx, table)
        return self._since_watermark(ctx, table)

    def _latest_snapshot(self, ctx: Context, table: str) -> DataFrame:
        """Only the newest snapshot, whatever the target holds.

        A full snapshot already carries the complete state, so the newest file is the
        whole truth and older ones are redundant. That keeps the read constant even
        when the target has not moved for weeks — which is the case the watermark
        handles badly, because its cut only advances when rows are actually written.
        """
        source = ctx.spark.table(table)
        latest = source.agg(F.max(_EXPORT_DATE)).collect()[0][0]

        ctx.logger.info(
            name="latest_snapshot_resolved",
            source=_SOURCE,
            description=f"Reading only the snapshot at {_EXPORT_DATE} = {latest}",
        )
        if latest is None:
            return source.limit(0)
        return source.filter(F.col(_EXPORT_DATE) == F.lit(latest))

    def _since_watermark(self, ctx: Context, table: str) -> DataFrame:
        """Compare as timestamps, not as strings.

        A string comparison gets the right answer only while the format happens to
        sort lexicographically, which "M/d/yyyy" does not.
        """
        watermark = self._watermark_for(ctx)
        return ctx.spark.table(table).filter(_anchor(ctx) > F.lit(watermark))

    def _watermark_for(self, ctx: Context) -> datetime:
        if self._watermark is None:
            self._watermark = self._read_watermark(ctx)
        return self._watermark

    def _read_watermark(self, ctx: Context) -> datetime:
        """The anchor's high-water mark in the target; the default when there is none yet."""
        column = _anchor_column(ctx)
        watermark = DEFAULT_WATERMARK
        if ctx.spark.catalog.tableExists(ctx.target_table):
            target = ctx.spark.table(ctx.target_table)
            highest = target.agg(F.max(_anchor(ctx))).collect()[0][0]
            watermark = highest or DEFAULT_WATERMARK

        ctx.logger.info(
            name="watermark_resolved",
            source=_SOURCE,
            description=f"Reading rows with {column} > {watermark}",
        )
        return watermark
