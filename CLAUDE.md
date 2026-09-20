# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## Running anything

**All Python runs through WSL.** Spark does not run natively on Windows, so pytest, black,
flake8, mypy and ruff all go through Ubuntu 24.04:

```bash
wsl -d Ubuntu-24.04 -- bash -lc 'cd /mnt/c/repos/escoto/EscotoDataFramework && poetry run pytest'
```

The Databricks CLI profile also lives inside WSL, not in the Windows `~/.databrickscfg`, so
`databricks` commands go the same way.

Python is 3.11 — matching Databricks Runtime 15.4 LTS. Invoke it as `python`, never
`python3`.

## Conventions

- **All Delta table names are UPPERCASE.** This is validated, not just convention.
- Keep implementations as simple as the problem allows.
- Comments explain *why*, not *what*. A comment that restates the code earns its deletion.
- This is a greenfield project. It has no predecessor to stay compatible with, so
  "parity" is never a reason to do something.

## Architecture

Five layers, each reaching the next only through a typed `Context` and a DataFrame:
Start (`context/`) → Pipeline (`pipelines/`) → Typing (`typecast/`) → Policies
(`policies/`) → Output (`output/`). Cross-cutting: `observability/`, `entrypoints/`.

A layer never reads raw task parameters — only the validated `Context`. Write verbs declare
their own `Requirements`, and the Start layer validates each origin × verb combination
before anything runs.

Full detail in [docs/](docs/00_overview.md); start with `00_overview.md`.

## Testing

```bash
make test    # unit tests, local Spark + Delta, 70% coverage gate
```

Platform tests are real Databricks jobs under `platform_tests/`, with their workflow YAML in
`resources/platform_tests/`. Each generates its own fixtures and asserts the resulting
tables. Deploy and run them with:

```bash
databricks bundle deploy -t dev_01 -p <profile>
databricks bundle run e2e_test_suite -t dev_01 -p <profile>
```

A platform test must clean only its own directories and tables. The suite runs its jobs in
parallel, so a wholesale cleanup of the shared inbound or metadata roots would destroy a
neighbour's checkpoints mid-run.

## Editing files from Windows

Bash heredocs mangle `\n` escapes in this environment. When a patch contains them, write the
patch script to the scratchpad with the Write tool and run it by path, rather than piping a
heredoc into `python -`.
