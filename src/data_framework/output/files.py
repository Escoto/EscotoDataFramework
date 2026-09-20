"""FileWriter — interface only; implementation deferred."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from data_framework.context.config import Verb
from data_framework.output.base import Requirements

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from data_framework.context.context import Context


class FileWriter:
    """Write to Volume paths (export targets). Interface only — not implemented."""

    verb: ClassVar[Verb]
    requires: ClassVar[Requirements] = Requirements()

    def write(self, df: DataFrame, ctx: Context) -> None:
        raise NotImplementedError("FileWriter is an interface stub; implementation deferred.")
