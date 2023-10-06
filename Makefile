.PHONY: install install-dev install-pre-commit test unit style check docs docs-serve

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

install-pre-commit:
	pre-commit install

test:
	python -m unittest

unit:
	python -m unittest discover -s tests/unit

style:
	pre-commit run --all-files

check: style unit

docs:
	python pdoc/cli.py -o docs

docs-serve:
	python pdoc/cli.py --port 8002
