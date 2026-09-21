"""CSV/TXT origin — Auto Loader with cloudFiles format."""

from __future__ import annotations

from typing import TYPE_CHECKING

from data_framework.context.config import SchemaEvolution

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from data_framework.context.context import Context

# A txt source is still read by the csv reader; only the glob's extension differs.
_FORMAT = "csv"

_EVOLUTION_MODES = {
    SchemaEvolution.ADD_NEW_COLUMNS: "addNewColumns",
    SchemaEvolution.ADD_NEW_COLUMNS_WITH_TYPE_WIDENING: "addNewColumnsWithTypeWidening",
    SchemaEvolution.RESCUE: "rescue",
    SchemaEvolution.FAIL_ON_NEW_COLUMNS: "failOnNewColumns",
    SchemaEvolution.NONE: "none",
}

_SOURCE = "CsvSource"


def _flag(value: bool) -> str:
    return "true" if value else "false"


def reader_options(ctx: Context) -> dict[str, str]:
    """Every Auto Loader option for this read, as the strings Spark expects.

    Separate from the read itself on purpose: cloudFiles only exists on Databricks,
    so this is the half that can be verified locally.
    """
    options = ctx.config.source.options
    return {
        "cloudFiles.format": _FORMAT,
        "cloudFiles.schemaLocation": ctx.schema_hints_location,
        "cloudFiles.schemaEvolutionMode": _EVOLUTION_MODES[ctx.config.schema_evolution],
        # A file that vanishes between listing and read must not fail the batch.
        "ignoreMissingFiles": "true",
        "header": _flag(options.header),
        "delimiter": options.delimiter,
        "multiLine": _flag(options.multiline),
        "quote": options.quote,
        "escape": options.escape,
    }


class CsvSource:
    """Read CSV (or TXT) files via Auto Loader streaming."""

    def read(self, ctx: Context) -> DataFrame:
        glob = ctx.inbound_glob
        assert glob  # guaranteed by SourceConfig for every file origin

        ctx.logger.info(
            name="source_read",
            source=_SOURCE,
            description=f"Auto Loader stream over {glob}",
        )
        return ctx.spark.readStream.format("cloudFiles").options(**reader_options(ctx)).load(glob)
