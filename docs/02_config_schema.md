# 02 — Configuration Schema

## 1. How parameters travel

Workflow YAMLs keep passing **flat `key: value` task parameters** (now as `python_wheel_task.named_parameters`). YAML anchors and shared blocks keep working exactly as today. Dotted keys express nesting; the Start layer splits them into a nested dict and validates the result with pydantic.

```
flat params ──split on "."──▶ nested dict ──pydantic──▶ TaskConfig ──▶ Context
```

Rules applied by the loader:

- **Coercion**: booleans accept `true`/`false` in any casing, quoted or not; lists accept comma-separated strings (`"ID, NAME"` → `["ID", "NAME"]`); enums are case-insensitive.
- **Unknown keys are rejected**, so a typo in a parameter name fails the task instead of being silently ignored.
- **All errors aggregate** into one `ConfigValidationError` listing every problem, so a misconfigured workflow is fixed in one iteration, not one error at a time.

## 2. Schema (pydantic model tree)

### Run identity (reserved `__` parameters)

Four parameters carry Databricks traceability into every audit row. They are consumed by
the entry point before validation, so they never appear in `TaskConfig`, and they are the
only parameters allowed to start with `__`:

| Parameter | Dynamic value | Audit column | Stable across runs |
|---|---|---|---|
| `__workflow_id` | `{{job.id}}` | `__workflow_id` | yes |
| `__workflow_run_id` | `{{job.run_id}}` | `__workflow_run_id` | no |
| `__task_key` | `{{task.name}}` | `__task_key` | yes |
| `__task_run_id` | `{{task.run_id}}` | `__task_run_id` | no |

Any that are absent default to the literal string `local`, and the run logs a
`run_identity_missing` WARNING naming them — so ad-hoc local runs stay frictionless while a
workflow that forgets them is visible in the audit table rather than silently untraceable.

```yaml
# ── identity ─────────────────────────────────────────────
catalog: clinical                 # str, required
env: dev_01                       # str, required   (always ${bundle.target})
metadata_path: /Volumes/.../metadata/   # str, required

# ── source (Layer 2: Pipeline) ───────────────────────────
source.origin: csv                # enum: csv | json | sas | delta   (required)

# file origins (csv/json/sas):
source.path: /Volumes/.../inbound/      # base volume path
source.directory: Subjects                # subdirectory
source.file_extension: txt              # optional; defaults to origin (csv reads *.csv)
source.schema_evolution: fail_on_new_columns  # Auto Loader's schemaEvolutionMode:
                                        #   add_new_columns | add_new_columns_with_type_widening
                                        #   | rescue | fail_on_new_columns (default) | none
                                        # the last three are file origins only
source.snapshot_time_pattern: datetime  # enum: datetime | timestamp (file-name timestamp shape)
source.preprocessors: ""                # ordered list of registered names, e.g. "record_envelope"
source.rename_patterns: ""              # list of regex=replacement pairs, e.g. "__[Vv]$="
source.options.header: true             # csv only (default true)
source.options.delimiter: ","           # csv only (default ",")
source.options.quote: '"'               # csv only (default "")
source.options.escape: "\\"             # csv/json (default "\")
source.options.multiline: true          # csv/json (default true)

# delta origin:
source.schema_name: bronze_main          # schema of the source table
source.table: SUBJECTS_UPDATES          # source table
source.deletes_table: SUBJECTS_DELETES  # optional deletes feed

# ── typing (Layer 3) ─────────────────────────────────────
typing.cast_config: /Workspace/.../subjects_cast.yml   # optional; absent → types pass through
typing.validate_casts: true             # bool, default true (silent-NULL detection)

# ── policies (Layer 4) ───────────────────────────────────
policies.checks_file: /Volumes/.../subjects_checks.yml  # optional; DQX ruleset

# ── output (Layer 5) ─────────────────────────────────────
output.verb: scd2                 # enum: append | full | upsert | scd2 | complete_delta (required)
output.schema_name: silver_main    # str, required
output.table: SUBJECTS            # str, required, UPPERCASE
output.keys: "ID"                 # list; required by upsert/scd2/complete_delta
output.event_time.column: MODIFIEDDATE      # required by scd2/complete_delta
output.event_time.format: "M/d/yyyy h:mm:ss a"  # optional; only when the column is a string (…_fmt)
output.snapshot_scope: delta      # enum: delta | full (default delta)
output.dedup.enabled: true        # bool, default false
output.dedup.columns: "ID"        # list; empty → all non-metadata columns
output.dedup.order_by: MODIFIEDDATE     # required when dedup enabled
output.dedup.order_by_format: "M/d/yyyy h:mm:ss a"  # optional
output.deletes.keys: "ID"               # deletes feed join keys
output.deletes.event_time.column: DATE_DELETED
output.deletes.event_time.format: yyyyMMddHHmmss
```

Notes:

- The increment strategy is **not a parameter**: each verb declares it (`checkpoint` for
  append/full/upsert/scd2, `watermark` for complete_delta) and the Start layer resolves it
  onto the `Context`. Setting `source.increment_strategy` is rejected as an unknown key.
- `output.dedup.order_by` is required **only when dedup is enabled**.
- There is no single overloaded "mode" parameter: Bronze ingestion uses `output.verb: append|full`, Silver promotion uses `output.verb: scd2|complete_delta|upsert`. The verb alone determines how the write behaves.
- The physical catalog is `{catalog}_{env}`, so one config serves every environment. Table names are UPPERCASE by convention, and that convention is validated.

## 3. Origin × verb requirements matrix

Verbs are layer-agnostic; the Start layer enforces this matrix (each writer *declares* its requirements — the matrix is derived, not hardcoded):

| | `append` | `full` | `upsert` | `scd2` | `complete_delta` |
|---|---|---|---|---|---|
| **any origin** | `output.*` target | `output.*` target | + `output.keys` | + `output.keys`, `output.event_time.column` | — |
| **file origins** (csv/json/sas) | typical Inbound→Bronze | supported | supported | supported | not supported (needs a Delta updates table) |
| **delta origin** | supported | supported | typical Silver→Gold | typical Bronze→Silver | + `source.snapshot_time_pattern`; optional `source.deletes_table` (with `output.deletes.*`) |
| **deletes feed** | — | — | — | — | optional |
| **`snapshot_scope: full`** | — | — | — | allowed | allowed |

Validation failures name the missing/conflicting keys, e.g.:
`output.verb=scd2 requires: output.keys, output.event_time.column — missing: output.event_time.column`.

## 4. Cast config file (Layer 3)

Column types live in their own YAML, referenced by `typing.cast_config`. A type list can be
long, and keeping it out of the workflow config stops it from muddying the task parameters:

```yaml
columns:
  - name: ID
    cast: { target_type: bigint }
  - name: VISIT_DATE
    cast: { target_type: date, format: "yyyy-MM-dd" }
  - name: MODIFIED
    cast: { target_type: timestamp, format: "M/d/yyyy h:mm:ss a" }
```

The file carries types and nothing else. Whether casts are validated is the workflow-level
`typing.validate_casts`, not a property of the type list, and unknown keys are rejected here
for the same reason they are in the task config.

**A column the config does not name keeps the type it arrived with.** Out of Auto
Loader that is string, so declaring types on the way into Bronze is enough to define
the table. It matters on promotion: a Bronze→Silver task that declares nothing is
saying *no changes*, not *flatten back to string*, so Bronze's types survive. Note
that re-declaring the same config at Silver is not a way to achieve this — a
`date`/`timestamp` entry would re-parse an already-typed column with its source
format and fail the cast validation.

Cast validation is single-pass, and framework metadata columns are exempt — a `__*` column named here is left alone, with a warning in the audit table.

## 5. Checks file (Layer 4)

Data quality rules live in their own YAML too, referenced by `policies.checks_file`, in
[Databricks DQX](https://databrickslabs.github.io/dqx/) format:

```yaml
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: ID
```

The framework reads the path and hands the contents to DQX; the check vocabulary is DQX's,
not this framework's. See [04_policies.md](04_policies.md) for how the file is applied and
what `criticality` means to the gate.

## 6. Worked example

A Bronze→Silver promotion task, carrying history with COMPLETE_DELTA and a deletes feed:

```yaml
- task_key: bronze_to_silver
  python_wheel_task:
    package_name: data_framework
    entry_point: run
    named_parameters:
      <<: [*basic_config_params]           # catalog, env, metadata_path anchors
      source.origin: delta
      source.schema_name: functional_testing
      source.table: *updates_table
      source.deletes_table: *deletes_table
      source.snapshot_time_pattern: datetime
      output.verb: complete_delta
      output.schema_name: functional_testing
      output.table: *silver_table
      output.keys: "ID, NAME"
      output.event_time.column: UPDATEDTIME
      output.event_time.format: yyyyMMddHHmmss
      output.dedup.enabled: true
      output.dedup.columns: "ID, NAME"
      output.dedup.order_by: UPDATEDTIME
      output.dedup.order_by_format: yyyyMMddHHmmss
      output.deletes.keys: "ID, NAME"
      output.deletes.event_time.column: DATE_DELETED
      output.deletes.event_time.format: yyyyMMddHHmmss
```

Every parameter is a flat `key: value` string, so YAML anchors and Asset Bundle
substitutions keep working; the Start layer is what turns the dotted keys into the
nested, validated `TaskConfig` above.

Note `output.event_time.format`. It is needed only while `UPDATEDTIME` is still a
string. Once a cast config has typed that column, the format must be **omitted** here —
re-parsing an already-typed timestamp with a source format yields NULL and fails cast
validation. See §4.
