# 04 — Policies (Data Quality)

## 1. Scope and philosophy

The Policies layer evaluates data quality rules against the dataset produced by Pipeline + Typing, **before** the Output layer writes it. Design decisions (agreed):

- **Native-first**: a small internal rule framework covering the checks that are actually used, with no heavyweight external dependency.
- **Pluggable**: rules implement a `Policy` protocol; external engines — specifically **Databricks DQX** — can be integrated later as adapters without touching the runner or writers.
- **Severities**: every rule carries `warn` (log and continue — the default) or `fail` (log, finish evaluating all rules, then fail the task).

## 2. Protocol

```python
class Severity(StrEnum):
    WARN = "warn"
    FAIL = "fail"

@dataclass
class PolicyResult:
    policy: str            # rule name
    passed: bool
    failed_count: int      # offending rows (or columns, rule-defined)
    samples: list[Row]     # up to 5 offending rows for diagnostics
    details: str           # human-readable summary

class Policy(Protocol):
    name: ClassVar[str]

    def evaluate(self, df: DataFrame, ctx: Context) -> PolicyResult: ...
```

Runner behavior:

1. Instantiate the rules configured under `policies.*` (registry lookup by name).
2. Evaluate **all** of them, in config order, even after a failure.
3. Log every result to the audit table — `warn` failures as `WARNING`, `fail` failures as `ERROR`, passes as `INFO` with counts.
4. If any `fail`-severity rule failed → raise `PolicyViolation` listing all failed rules.

For streaming flows the runner executes inside the same `foreachBatch` as Typing and Output, per micro-batch.

## 3. Native rules shipped in the rewrite

A deliberately small set, plus the machinery to grow:

| Rule | Config | What it checks | Default severity |
|---|---|---|---|
| `not_null` | `policies.not_null.columns` (list), `.severity` | No NULLs in the named columns | `warn` |
| `schema_drift` | `policies.schema_drift.severity` | Columns added or missing versus the target table | `warn` |

The cast silent-NULL validation remains in the **Typing** layer (it is a property of casting, not a dataset rule) but reports through the same audit conventions.

Anticipated (interface-ready, not shipped): `unique_keys`, `row_count_min`, `accepted_values`, `custom_sql`. Each is one registered class.

## 4. Configuration

```yaml
policies.not_null.columns: "ID, SUBJECT_ID"
policies.not_null.severity: warn          # warn | fail
policies.schema_drift.severity: warn
```

Absent config → no rules run except `schema_drift` for Delta targets, which is always reported because an unnoticed schema change is the expensive kind.

## 5. Audit log shape

Policy results reuse the audit contract (`` `monitoring_{env}`.`audit`.`logs` ``):

| Field | Value |
|---|---|
| `type` | `INFO` / `WARNING` / `ERROR` per outcome |
| `source` | `Policies` |
| `name` | rule name, e.g. `not_null` |
| `total` | `failed_count` |
| `description` | details + up to 5 sample rows |

KPI-style dashboards can keep querying by `name`/`source` as today.

## 6. Databricks DQX extension point (future)

When DQX is adopted, it plugs in as an adapter — nothing else changes:

```python
class DqxPolicy(Policy):
    name = "dqx"
    def __init__(self, checks_file: str): ...
    def evaluate(self, df, ctx) -> PolicyResult:
        # run DQX checks, map its result → PolicyResult
```

Config would select it like any rule (`policies.dqx.checks_file: ...`). The runner, severities, audit logging, and writers are unaffected. This is a documented extension point only — **not** part of the rewrite scope.

## 7. Table tagging

Unity Catalog tagging (`SET TAGS` upsert, applied when new tag values appear, warning when the table is missing) lives in `observability/tagging.py`. It is governance metadata, not a dataset rule, so it sits outside the Policies layer.
