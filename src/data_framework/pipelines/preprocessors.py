"""Pre-processor registry — named DataFrame transforms applied after reading."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Protocol, runtime_checkable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from data_framework.context.context import Context


@runtime_checkable
class PreProcessor(Protocol):
    """A named, config-selected DataFrame transform."""

    name: ClassVar[str]

    def apply(self, df: DataFrame, ctx: Context) -> DataFrame: ...


class RecordEnvelope:
    """Common vendor JSON envelope: metadata:export_date + data[] → URI/DATA/EXPORT_DATE."""

    name: ClassVar[str] = "record_envelope"

    def apply(self, df: DataFrame, ctx: Context) -> DataFrame:
        raise NotImplementedError("P6")


class FlattenNested:
    """Generic recursive flattener: explode arrays, expand structs until flat."""

    name: ClassVar[str] = "flatten_nested"

    def apply(self, df: DataFrame, ctx: Context) -> DataFrame:
        raise NotImplementedError("P6")


PREPROCESSORS: dict[str, type[PreProcessor]] = {
    "record_envelope": RecordEnvelope,
    "flatten_nested": FlattenNested,
}


def resolve(names: list[str]) -> list[PreProcessor]:
    """Instantiate the named pre-processors, in configured order.

    Unknown names are rejected at Start (loader.validate_requirements), so by the
    time the pipeline runs every name here is registered.
    """
    return [PREPROCESSORS[name]() for name in names]


def apply_preprocessors(df: DataFrame, ctx: Context) -> DataFrame:
    """Run the configured pre-processors over the batch, in order."""
    for preprocessor in resolve(ctx.config.source.preprocessors):
        df = preprocessor.apply(df, ctx)
    return df
