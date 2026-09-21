# Escoto Data Framework for Databricks Data Engineering

A configuration-driven data platform for quick data onboarding and processing on Databricks. Datasets are onboarded by writing a workflow YAML, not by writing Python.

### Built With

- [![Databricks][Databricks]][Databricks-url]
- [![Python][Python]][Python-url]
- [![Spark][Spark]][Spark-url]
- [![Pandas][Pandas]][Pandas-url]


## Documentation

1. [00_overview.md](docs/00_overview.md) — goals, principles, glossary, layer diagram
2. [01_architecture.md](docs/01_architecture.md) — layers, protocols, execution flow, extension points
3. [02_config_schema.md](docs/02_config_schema.md) — the typed config schema, parameter by parameter
4. [03_write_verbs.md](docs/03_write_verbs.md) — verb semantics with worked examples
5. [04_policies.md](docs/04_policies.md) — the data quality gate, driven by a DQX ruleset
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

The bundle in [databricks.yml](databricks.yml) is configured to work with a python wheel. It builds the python wheel then deploys the the bundle. Authenticate with a CLI profile or with `DATABRICKS_HOST` / `DATABRICKS_TOKEN`:

```bash
databricks bundle deploy
```

## License

Apache 2.0 — see [LICENSE](LICENSE).

<!-- MARKDOWN LINKS & IMAGES -->

[Databricks]: https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white
[Databricks-url]: https://www.databricks.com/

[Python]: https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54
[Python-url]: https://www.python.org/

[Spark]: https://img.shields.io/badge/Apache_Spark-FFFFFF?style=for-the-badge&logo=apachespark&logoColor=#E35A16
[Spark-url]: https://spark.apache.org/

[Pandas]: https://img.shields.io/badge/-Pandas-333333?style=flat&logo=pandas
[Pandas-url]: https://pandas.pydata.org
