"""Policy protocol, PolicyResult, and Severity enum."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, runtime_checkable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from data_framework.context.context import Context


class Severity(StrEnum):
    WARN = "warn"
    FAIL = "fail"


@dataclass
class PolicyResult:
    policy: str
    passed: bool
    failed_count: int = 0
    samples: list[Any] = field(default_factory=list)
    details: str = ""


class PolicyViolation(Exception):
    """Raised when one or more fail-severity policies are violated."""

    def __init__(self, results: list[PolicyResult]):
        self.results = results
        names = [r.policy for r in results]
        super().__init__(f"Policy violations: {', '.join(names)}")


@runtime_checkable
class Policy(Protocol):
    """A data quality rule that evaluates a DataFrame and returns a result."""

    name: ClassVar[str]

    def evaluate(self, df: DataFrame, ctx: Context) -> PolicyResult: ...
