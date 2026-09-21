"""Placeholder test — validates the package imports correctly."""

# TaskConfig and Requirements are unused on purpose: here the import *is* the
# assertion, so removing them would drop coverage rather than dead code.
import data_framework
from data_framework.context.config import (  # noqa: F401
    Origin,
    TaskConfig,
    Verb,
)
from data_framework.output.base import Requirements, Writer  # noqa: F401
from data_framework.output.registry import VERB_REQUIREMENTS
from data_framework.pipelines.base import SourcePipeline
from data_framework.policies.base import PolicyResult, Severity
from data_framework.typecast.models import CastConfiguration


def test_version():
    assert data_framework.__version__ == "0.1.0"


def test_enums():
    assert Origin.CSV == "csv"
    assert Verb.COMPLETE_DELTA == "complete_delta"
    assert Severity.FAIL == "fail"


def test_verb_requirements_complete():
    for verb in Verb:
        assert verb in VERB_REQUIREMENTS, f"Missing requirements for verb: {verb}"


def test_cast_configuration_defaults():
    # validation is switched by typing.validate_casts, not by the detached YAML
    cfg = CastConfiguration()
    assert cfg.columns == []


def test_policy_result():
    result = PolicyResult(policy="id_is_null", passed=True, severity=Severity.WARN)
    assert result.passed
    assert result.failed_count == 0


def test_protocols_are_runtime_checkable():
    assert hasattr(SourcePipeline, "__protocol_attrs__") or hasattr(
        SourcePipeline, "__abstractmethods__"
    )
    assert hasattr(Writer, "__protocol_attrs__") or hasattr(Writer, "__abstractmethods__")
