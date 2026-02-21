# fintran — Development

## Testing

Framework: **pytest**

```bash
uv run pytest
uv run pytest --cov=fintran --cov-report=term-missing
```

Conventions:
- Tests live in `tests/`, mirroring the `fintran/` structure (e.g. `tests/readers/test_hledger.py`)
- Each reader and writer has a dedicated test module with fixture files in `tests/fixtures/`
- Minimum coverage threshold: **80%** (enforced in CI)
- Use `pytest-cov` for coverage and `pytest-datadir` or plain `pathlib` for fixture file loading

Dependencies:
```
uv add --dev pytest pytest-cov
```

---

## Code Quality — pre-commit

Framework: **pre-commit** with the following hooks:

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.4.0
    hooks:
      - id: ruff          # linting
      - id: ruff-format   # formatting (replaces black)

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.9.0
    hooks:
      - id: mypy
        additional_dependencies: [polars, cyclopts]
```

Install hooks after cloning:
```bash
uv run pre-commit install
```

Run manually:
```bash
uv run pre-commit run --all-files
```

**ruff** is preferred over black + flake8 + isort — it covers all three and is consistent with the Astral/uv ecosystem.

---

## Docker

A Docker image provides a reproducible runtime for distribution to students or clients without a local Python setup.

```dockerfile
# Dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN pip install uv && uv sync --frozen

COPY fintran/ ./fintran/

ENTRYPOINT ["uv", "run", "fintran"]
```

Build and run:
```bash
docker build -t fintran .
docker run --rm -v $(pwd):/data fintran --from hledger --to twinfield /data/input.journal -o /data/output.csv
```

Mount the working directory as `/data` to pass files in and out.

---

## CI Pipeline (GitHub Actions)

Suggested workflow on push/PR:

1. `uv sync`
2. `pre-commit run --all-files`
3. `pytest --cov=fintran`
4. Coverage report as PR comment (optional)

---

## Recommended Dev Commands

| Task | Command |
|---|---|
| Install deps | `uv sync` |
| Run fintran | `uv run fintran` |
| Run tests | `uv run pytest` |
| Lint + format | `uv run pre-commit run --all-files` |
| Build Docker | `docker build -t fintran .` |
