.PHONY: help clean test coverage docs servedocs install bump publish release
.DEFAULT_GOAL := help
SHELL := /bin/bash

define BROWSER_PYSCRIPT
import os, webbrowser, sys

from urllib.request import pathname2url

rel_current_path = sys.argv[1]
abs_current_path = os.path.abspath(rel_current_path)
uri = "file://" + pathname2url(abs_current_path)

webbrowser.open(uri)
endef

export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

regex_pattern = r'^([a-zA-Z_-]+):.*?## (.*)$$'

for line in sys.stdin:
	match = re.match(regex_pattern, line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef

export PRINT_HELP_PYSCRIPT

BROWSER := python -c "$$BROWSER_PYSCRIPT"
DO_DOCS_HTML := $(MAKE) -C clean-docs && $(MAKE) -C docs html
SPHINXBUILD   = python3 -msphinx

PACKAGE_NAME = "pgutils"
PACKAGE_VERSION := poetry version -s

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-build clean-pyc clean-test clean-cache clean-docs ## remove all build, test, coverage, Python artifacts, cache and docs

clean-docs: # remove docs for update
	rm -fr "docs/$$PACKAGE_NAME.rst" "docs/modules.rst" "docs/conftest.rst" "docs/examples.rst" "docs/tests.rst" "docs/_build"

clean-build: # remove build artifacts
	rm -fr build/ dist/ .eggs/
	find . -name '*.egg-info' -o -name '*.egg' -exec rm -fr {} +

clean-pyc: # remove Python file artifacts
	find . -name '*.pyc' -o -name '*.pyo' -o -name '*~' -exec rm -rf {} +

clean-test: # remove test and coverage artifacts
	rm -fr .tox/ .coverage coverage.* htmlcov/ .pytest_cache

clean-cache: # remove test and coverage artifacts
	find . -name '*pycache*' -exec rm -rf {} +

search: ## Searchs for a token in the code. Usage: make search token=your_token
	grep -rnw . --exclude-dir=venv --exclude-dir=.git --exclude=poetry.lock -e "$(token)"

replace: ## Replaces a token in the code. Usage: make replace token=your_token
	sed -i 's/$(token)/$(new_token)/g' $$(grep -rl "$(token)" . \
		--exclude-dir=venv \
		--exclude-dir=.git \
		--exclude=poetry.lock)

test: ## run tests quickly with the default Python
	poetry shell
	pytest --durations=10

watch-test: ## run tests on watchdog mode
	poetry shell
	ptw --poll --clear .

lint: clean ## perform inplace lint fixes
	ruff check --fix .

cov: clean ##
	coverage run --source "$$PACKAGE_NAME" --omit "tests/*,*/__init__.py" -m pytest --durations=10
	coverage report -m

watch-cov: clean ## check code coverage quickly with the default Python
		find . -name '*.py' | entr -c make cov

env: ## Creates a virtual environment. Usage: make env
	pip install virtualenv
	virtualenv .venv

install: clean ## Installs the python requirements. Usage: make install
	poetry install

echo: ## echo current package version
	echo "v$$(poetry version -s)"

what: ## List all commits made since last version bump
	git log --oneline "$$(git rev-list -n 1 "v$$(poetry version -s)")..$$(git rev-parse HEAD)"

check-bump: # check if bump version is valid
	@if [ "$(v)" != "patch" ] && [ "$(v)" != "minor" ] && [ "$(v)" != "major" ]; then \
		echo "Invalid input for 'v': $(v). Please use 'patch', 'minor', or 'major'."; \
		exit 1; \
	fi; \

bump: ## bump version to user-provided {patch|minor|major} semantic
	@$(MAKE) check-bump v=$(v)
	poetry version $(v)
	git add pyproject.toml
	poetry lock
	git add poetry.lock
	git commit -m "release/ tag v$$(poetry version -s)"
	git tag "v$$(poetry version -s)"
	git push
	git push --tags
	poetry version

publish: clean ## build source and publish package
	poetry publish --build

release: bump v=$(v) ## release package on PyPI
	$(MAKE) -C publish
