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
non-start location. Thus, you will need to edit your
`.git/hooks/pre-commit` file to reflect this. Change
the line that begins with `ARGS` to:

    ARGS=(hook-impl --config=.config/pre-commit-config.yaml --hook-type=pre-commit)

With pre-commit, all code is formatted according to
[black]("https://github.com/psf/black") and
[ruff]("https://github.com/charliermarsh/ruff") guidelines.

To check if your changes pass pre-commit without committing, run:

    pre-commit run --all-files --config=.config/pre-commit-config.yaml

## Testing

To run the tests and view coverage, execute:

    pytest -c .config/pytest.ini --cov hdx --cov-config .config/coveragerc

Follow the example set out already in ``api.rst`` as you write the documentation.

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
