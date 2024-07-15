# Contributing guidelines

## Before contributing

Welcome to desalkila! Before contributing to the project,
make sure that you **read our code of conduct** (`CODE_OF_CONDUCT.md`).

## Contributing code

1. Clone the repository
2. Set up and activate a Python development environment
   (advice: use [venv](https://docs.python.org/3/library/venv.html),
   [virtualenv](https://virtualenv.pypa.io/), or [miniconda](https://docs.conda.io/en/latest/miniconda.html))
3. Install tox: `python -m pip install tox`
4. Make sure the tests run: `tox -e py38`
   (change the version number according to the Python you are using)
5. Start a new branch: `git switch -c new-branch main`
6. Make your code changes
7. Check that your code follows the style guidelines of the project: `tox -e reformat && tox -e check`
8. Run the tests again and verify that they pass: `tox -e py38`
9. (optional) Build the documentation: `tox -e docs`
10. Commit, push, and open a pull request!
