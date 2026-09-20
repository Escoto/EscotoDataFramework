# Escoto Databricks Data Framework

A configuration-driven data engineering framework for Databricks. Datasets are onboarded by writing a workflow YAML, not by writing Python.

Five layers, each talking to the next only through a typed `Context` and a DataFrame:

| Layer | Responsibility |
|---|---|
| **Start** | Flat task parameters → one validated, typed `Context`. Unknown keys are rejected. |
| **Pipeline** | Read from the configured origin (CSV / JSON / SAS / Delta), apply pre-processors, add provenance. |
| **Typing** | Apply the casts the config declares. Columns it does not name keep the type they arrived with. |
| **Policies** | Evaluate data quality rules with `warn` or `fail` severity. |
| **Output** | Write with a verb: `append`, `full`, `upsert`, `scd2`, `complete_delta`. |

## Write verbs

| Verb | Semantics |
|---|---|
| `append` | Add rows. No matching, no history. |
| `full` | Overwrite the target. An empty input is skipped, never written. |
| `upsert` | SCD Type 1 — one row per key, newer wins, no history. |
| `scd2` | SCD Type 2 — close the superseded version, insert the new one. |
| `complete_delta` | Split a backlog of exports into snapshots and replay them through the SCD2 engine, in order. |

`scd2` and `complete_delta` maintain `__START_DATE`, `__END_DATE`, __CURRENT_FLAG` and `__DELETED_FLAG`.
A validity window opens at the record's own event time (replaying a week-old backlog reconstructs the real history instead of collapsing it onto the moment the job happened to run).

## Documentation

1. [00_overview.md](docs/00_overview.md) — goals, principles, glossary, layer diagram
2. [01_architecture.md](docs/01_architecture.md) — layers, protocols, execution flow, extension points
3. [02_config_schema.md](docs/02_config_schema.md) — the typed config schema, parameter by parameter
4. [03_write_verbs.md](docs/03_write_verbs.md) — verb semantics with worked examples
5. [04_policies.md](docs/04_policies.md) — data quality rules and the DQX extension point
6. [05_testing.md](docs/05_testing.md) — unit and platform testing
7. [06_roadmap.md](docs/06_roadmap.md) — phased implementation plan and current status

## Development

Dependencies and the virtualenv are managed with [Poetry](https://python-poetry.org/) (2.x).
Python must be **3.11** — it is what Databricks Runtime 15.4 LTS ships.

On Windows, run everything below from WSL (Ubuntu 24.04); Spark does not run natively on
Windows.

```bash
poetry env use python3.11   # once, to pin the interpreter
make install                # runtime + dev dependencies
make test                   # unit tests with coverage
```

Poetry keeps the virtualenv outside the project (`~/.cache/pypoetry/virtualenvs`), so there is
no `.venv/` directory to get out of sync with the interpreter actually running the tests.

### Make targets

| Target | What it does |
|---|---|
| `make install` | `poetry install` — create the venv and install everything |
| `make test` | `poetry run pytest` — unit tests with coverage (70% gate) |
| `make qa` | `black .`, then `flake8`, then `yamllint .` |
| `make format` | import sort (`ruff --select I --fix`) + `black .` |
| `make build` | `poetry build` — wheel + sdist into `dist/` |
| `make clean` | remove `dist/`, caches, `__pycache__` |

`make` with no target prints this list.

Note that `make qa` **rewrites files** — `black .` formats in place rather than checking.
Use `poetry run black --check .` for a read-only pass.

### Tool configuration

All tool config lives in `pyproject.toml` except flake8, which cannot read it — flake8's
settings are in `.flake8`.

`mypy` and `ruff` are installed and configured but are not part of `make qa` yet.

## Deploying to Databricks

`make build` produces `dist/data_framework-0.1.0-py3-none-any.whl` with the console-script
entry point `run`, which is what a `python_wheel_task` expects:

```yaml
python_wheel_task:
  package_name: data_framework
  entry_point: run
  named_parameters: { ... }
```

The bundle in [databricks.yml](databricks.yml) builds and uploads that wheel and deploys the
platform-test jobs. No workspace URL is committed — authenticate with a CLI profile or with
`DATABRICKS_HOST` / `DATABRICKS_TOKEN`:

```bash
databricks bundle deploy
```

## E2E Testing

Pre-requirements:
1. Catalog: `testing_${bundle.target}` (`testing_dev_01`)
2. Schema: `functional_testing`.
3. Volume: `/Volumes/testing_dev_01/functional_testing/`

```bash
databricks bundle run e2e_test_suite
```

Platform tests run as real Databricks jobs — each generates its own fixtures, runs the
framework over them, and asserts the resulting tables. See
[docs/05_testing.md](docs/05_testing.md).

## Unit Testing

```bash
make test    # unit tests, local Spark
```


## License

Apache 2.0 — see [LICENSE](LICENSE).
