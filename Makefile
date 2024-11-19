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

PACKAGE_NAME = "pgbase"
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
	grep -rnw . --exclude-dir=.venv --exclude-dir=.git --exclude=uv.lock -e "$(token)"


replace: ## Replaces a token in the code. Usage: make replace token=your_token
	sed -i 's/$(token)/$(new_token)/g' $$(grep -rl "$(token)" . \
		--exclude-dir=.venv \
		--exclude-dir=.git \
		--exclude=uv.lock)


uv: ## Install uv
	pip install uv


env: ## Creates a virtual environment. Usage: make env
	uv venv


test: ## run tests quickly with the default Python
	uv run pytest --durations=10


watch-test: ## run tests on watchdog mode
	uv run ptw --poll --clear .


lint: clean ## perform inplace lint fixes
	uv run ruff check --fix .


cov: clean ##
	uv run coverage run --source "$$PACKAGE_NAME" --omit "tests/*,*/__init__.py" -m pytest --durations=10
	uv run coverage report -m


watch-cov: clean ## check code coverage quickly with the default Python
	find . -name '*.py' | entr -c make cov


install: clean ## Installs the python requirements. Usage: make install
	uv sync


what: ## List all commits made since last version bump
	git log --oneline "$$(git rev-list -n 1 "v$$(poetry version -s)")..$$(git rev-parse HEAD)"


check-bump: # check if bump version is valid
	@if [ -z "$(v)" ]; then \
		echo "Error: Missing version bump type. Use: make bump v={patch|minor|major}"; \
		exit 1; \
	fi
	@if ! echo "patch minor major" | grep -qw "$(v)"; then \
		echo "Error: Invalid version bump type '$(v)'. Use: patch, minor, or major."; \
		exit 1; \
	fi; \


echo: ## echo current package version
	echo "v$$(grep '^version =' pyproject.toml | awk '{print $$3}' | tr -d '"')"


apply-bump: ## apply version bump
	@if [ -z "$(v)" ]; then \
		echo "Error: Missing version bump type. Use: make apply-bump v={patch|minor|major}"; \
		exit 1; \
	fi; \
	\
	# Validate bump type; \
	if ! echo "patch minor major" | grep -qw "$(v)"; then \
		echo "Error: Invalid bump type '$(v)'. Use: patch, minor, or major."; \
		exit 1; \
	fi; \
	\
	# Extract the current version from pyproject.toml; \
	CURRENT_VERSION=$$(grep '^version =' pyproject.toml | awk '{print $$3}' | tr -d '"'); \
	echo "Current version: $$CURRENT_VERSION"; \
	\
	# Validate that the version format is correct (x.y.z); \
	if ! echo "$$CURRENT_VERSION" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$$'; then \
		echo "Error: Invalid version format '$$CURRENT_VERSION'. Expected format: x.y.z"; \
		exit 1; \
	fi; \
	\
	# Split the version into major, minor, patch; \
	MAJOR=$$(echo $$CURRENT_VERSION | cut -d '.' -f 1); \
	MINOR=$$(echo $$CURRENT_VERSION | cut -d '.' -f 2); \
	PATCH=$$(echo $$CURRENT_VERSION | cut -d '.' -f 3); \
	\
	# Increment the appropriate version part based on the bump type; \
	if [ "$(v)" = "patch" ]; then \
		PATCH=$$((PATCH+1)); \
	elif [ "$(v)" = "minor" ]; then \
		MINOR=$$((MINOR+1)); \
		PATCH=0; \
	elif [ "$(v)" = "major" ]; then \
		MAJOR=$$((MAJOR+1)); \
		MINOR=0; \
		PATCH=0; \
	fi; \
	\
	# Construct the new version; \
	NEW_VERSION="$$MAJOR.$$MINOR.$$PATCH"; \
	\
	# Update the version in pyproject.toml using sed; \
	if [ "$(shell uname)" = "Darwin" ]; then \
		# macOS sed requires an extra argument for -i; it’s an empty string in this case; \
		sed -i '' "s/^version = \".*\"/version = \"$$NEW_VERSION\"/" pyproject.toml; \
	else \
		sed -i "s/^version = \".*\"/version = \"$$NEW_VERSION\"/" pyproject.toml; \
	fi; \
	\
	echo "Version bumped to $$NEW_VERSION"


bump: ## Bump version to user-provided {patch|minor|major} semantic version
	@$(MAKE) check-bump v=$(v)
	@$(MAKE) apply-bump v=$(v)
	uv lock
	git add pyproject.toml uv.lock
	@NEW_VERSION=$$(grep '^version =' pyproject.toml | awk '{print $$3}' | tr -d '"'); \
	git commit -m "release: tag v$$NEW_VERSION"; \
	git tag "v$$NEW_VERSION"; \
	git push; \
	git push --tags
	poetry version


publish: clean ## build source and publish package
	poetry publish --build


release: bump v=$(v) ## release package on PyPI
	$(MAKE) -C publish
