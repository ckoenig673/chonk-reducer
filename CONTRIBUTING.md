# Contributing to Chonk Reducer

Thanks for helping improve Chonk Reducer.

## Before You Start

- Open an issue for substantial behavior or architecture changes.
- Keep pull requests focused and avoid combining unrelated refactors and fixes.
- Never commit real media, credentials, webhook URLs, encryption keys, databases, logs, or host-specific paths.

## Development Setup

```bash
python -m pip install -r requirements-dev.txt
pytest -q
```

Tests must not require a NAS, real media library, Intel QSV hardware, FFmpeg execution, or production database. Use temporary paths and mocks for external tools.

See [the testing guide](docs/TESTING.md) for project-specific guidance.

## Pull Requests

- Describe the user-visible behavior and operational impact.
- Add or update tests for changed behavior.
- Update documentation when settings, routes, environment variables, or workflows change.
- Confirm the full test suite passes.
- Keep secrets and private filesystem details out of screenshots, logs, and fixtures.
