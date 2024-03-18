# Development

## Environment

Development is currently done using Python 3.11. We recommend using a virtual
environment such as ``venv``:

    python3.11 -m venv venv
    source venv/bin/activate

In your virtual environment, please install all packages for
development by running:

    pip install -r requirements.txt

## Pre-Commit

Also be sure to install `pre-commit`, which is run every time
you make a git commit:

    pre-commit install

The configuration file for this project is in a
non-standard location. Thus, you will need to edit your
`.git/hooks/pre-commit` file to reflect this. Change
the line that begins with `ARGS` to:

    ARGS=(hook-impl --config=.config/pre-commit-config.yaml --hook-type=pre-commit)

With pre-commit, all code is formatted according to
[ruff](https://github.com/astral-sh/ruff) guidelines.

To check if your changes pass pre-commit without committing, run:

    pre-commit run --all-files --config=.config/pre-commit-config.yaml

## Environment Variables

For the `test_ckan.py` tests to run successfully some configuration is required:

1. The environment variable `HDX_KEY_TEST` needs to contain a valid key from the HDX demo server at
https://demo.data-humdata-org.ahconu.org/
2. Authentication details for Google Sheets need to be obtained from Mike Rans and either saved in a file named `.gsheet_auth.json` in the home directory (~) or placed in an environment variable `GSHEET_AUTH`. The file is preferred for Windows systems since adding such a long text string to an environment variable in Windows is challenging.

## Testing

To run the tests and view coverage, execute:

    pytest -c .config/pytest.ini --cov hdx --cov-config .config/coveragerc

Follow the example set out already in ``documentation/main.md`` as you write the documentation.

## Packages

[pip-tools](https://github.com/jazzband/pip-tools) is used for
package management.  If you’ve introduced a new package to the
source code (i.e.anywhere in `src/`), please add it to the
`project.dependencies` section of
`pyproject.toml` with any known version constraints.

For adding packages for testing or development, add them to
the `test` or `dev` sections under `[project.optional-dependencies]`.

Any changes to the dependencies will be automatically reflected in
`requirements.txt` with `pre-commit`, but you can re-generate
the file without committing by executing:

    pre-commit run pip-compile --all-files --config=.config/pre-commit-config.yaml
