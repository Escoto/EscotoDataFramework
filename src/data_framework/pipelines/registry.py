"""Origin → source reader registry.

Adding an origin means adding a reader and one entry here; nothing in the Start
layer or the entrypoint needs a matching edit.
"""

from __future__ import annotations

from data_framework.context.config import Origin
from data_framework.pipelines.base import SourcePipeline
from data_framework.pipelines.csv_source import CsvSource
from data_framework.pipelines.delta_source import DeltaSource
from data_framework.pipelines.json_source import JsonSource
from data_framework.pipelines.sas_source import SasSource

# json and sas are registered but still raise on read (P6): an origin the config
# accepts should fail where it is unimplemented, not look unknown at dispatch.
SOURCES: dict[Origin, type[SourcePipeline]] = {
    Origin.CSV: CsvSource,
    Origin.JSON: JsonSource,
    Origin.SAS: SasSource,
    Origin.DELTA: DeltaSource,
}
