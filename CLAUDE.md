# CLAUDE.md

## Project Overview

**hdx-python-api** is the official Python library for interacting with the [Humanitarian Data Exchange (HDX)](https://data.humdata.org/) platform. It provides ORM-like classes wrapping the CKAN API to read, create, update, and delete datasets, resources, organizations, users, vocabularies, showcases, and datastores.

## Source Layout

- `src/hdx/api/` — Configuration, session management, locations, utilities
  - `configuration.py` — `Configuration` class (credentials, site URL, user agent)
  - `remotehdx.py` — Low-level CKAN API client
- `src/hdx/data/` — Data model objects, all inheriting from `HDXObject`
  - `dataset.py`, `resource.py`, `organization.py`, `user.py`, `vocabulary.py`, `showcase.py`, `resource_view.py`
  - `hdxobject.py` — Base class providing `_read_from_hdx` / `_write_to_hdx` and CRUD scaffolding
- `src/hdx/facades/` — High-level entry-point helpers (`simple`, `keyword_arguments`, `infer_arguments`)
- `tests/hdx/` — Test suite mirroring src layout; fixtures in `tests/fixtures/`

## Running Tests

```bash
uv run pytest
```

Tests mock the CKAN HTTP session via inner `MockSession` classes in each test file — no live HDX connection needed for the standard suite. Integration tests require `HDX_KEY_TEST` and `HDX_PIPELINE_GSHEET_AUTH` environment variables.

Coverage is written to `coverage.lcov` and JUnit XML to `test-results.xml`.

## Code Style

Formatted and linted with `ruff` (rules: E, F, I, UP; line-length not enforced). Run before committing:

```bash
pre-commit run --all-files
```

- Python ≥ 3.10
- Type hints throughout; use `X | Y` union syntax (PEP 604), not `Optional`/`Union`
- Google-style docstrings with `Args:` and `Returns:` sections
- No inline comments unless the *why* is non-obvious

## Key Patterns

**HDXObject subclasses** expose a standard interface: `create_in_hdx()`, `update_in_hdx()`, `delete_from_hdx()`, `read_from_hdx()`. Each class defines an `actions()` static method mapping action names to CKAN API action strings.

**Writing to HDX** uses `self._write_to_hdx(action_key, data_dict, id_field_name)` inherited from `HDXObject`. The `action_key` must be in `actions()`.

**Test mocking** — each test fixture defines a `MockSession` with a `post()` method that inspects the URL and decoded JSON body to return `MockResponse` objects. State is tracked on class variables (e.g. `TestResource.datastore`).

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions, external systems). Search or read the actual source first. If you lack verified information, say so rather than speculate.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix.
