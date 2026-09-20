"""Composition root: read a source, prepare each batch, hand it to the verb's writer.

The layers themselves know nothing about each other; this is the only place that
knows the order they run in.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from data_framework.context.config import FILE_ORIGINS
from data_framework.output.registry import WRITER_BY_VERB
from data_framework.pipelines.enrichment import (
    add_provenance,
    apply_rename_patterns,
    sanitize_column_names,
)
from data_framework.pipelines.preprocessors import apply_preprocessors
from data_framework.pipelines.registry import SOURCES
from data_framework.typecast.service import CastService

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from data_framework.context.context import Context
    from data_framework.output.base import Writer


def prepare(df: DataFrame, ctx: Context) -> DataFrame:
    """Everything between reading a batch and writing it.

    A pure DataFrame transformation: no I/O and no streaming assumptions, so the
    identical chain runs inside foreachBatch and on a plain batch DataFrame. Keeping
    it that way is what lets a verb change increment strategy without rewriting its
    control flow.
    """
    df = apply_preprocessors(df, ctx)
    df = sanitize_column_names(df, ctx)
    df = apply_rename_patterns(df, ctx.config.source.rename_patterns)

    # P5 inserts the policy runner here, between typing and the write.
    return CastService().apply(df, ctx.config.typing, ctx)


def run_pipeline(ctx: Context) -> None:
    """Drive the configured origin into the configured verb."""
    source = SOURCES[ctx.config.source.origin]()
    writer = WRITER_BY_VERB[ctx.config.output.verb]()

    df = source.read(ctx)

    if ctx.config.source.origin in FILE_ORIGINS:
        # Provenance is attached here, to the source DataFrame, and deliberately not
        # inside prepare(): _metadata.file_path resolves only against the file source.
        # A micro-batch arriving in foreachBatch is a plain RDD that has already lost
        # it, so adding it there fails at run time on Auto Loader.
        # A delta source needs none of this — it carries the columns its ingest wrote.
        df = add_provenance(df)

    if df.isStreaming:
        _drive_stream(df, ctx, writer)
    else:
        writer.write(prepare(df, ctx), ctx)


def _drive_stream(df: DataFrame, ctx: Context, writer: Writer) -> None:
    """Run the available data through the writer, one micro-batch at a time.

    awaitTermination is not optional: starting the query and returning would let
    so tasks reported success while the work was still running.
    """
    query = (
        df.writeStream.foreachBatch(
            lambda batch, _epoch_id: writer.write(prepare(batch, ctx), ctx)
        )
        .option("checkpointLocation", ctx.checkpoint_location)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
