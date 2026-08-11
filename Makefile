.DEFAULT_GOAL := help

PYTHON ?= python3
VENV ?= .venv

help:
	@echo "fast-tasks development commands"
	@echo "  make install    create the virtualenv and install the package with its development tools"
	@echo "  make servers    start the redis, mysql and postgres the full suite needs"
	@echo "  make test       run the suite"
	@echo "  make coverage   run the suite with the 100% branch coverage gate"
	@echo "  make lint       check the code"
	@echo "  make format     format the code"
	@echo "  make build      build the wheel and the sdist"
	@echo "  make clean      remove build and coverage artifacts"

install:
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/python -m pip install --upgrade pip
	$(VENV)/bin/python -m pip install -e ".[sqlalchemy,redis]" pytest pytest-asyncio pytest-cov pytest-timeout ruff black aiosqlite build

servers:
	docker run -d --name fast-tasks-redis -p 6399:6379 redis:7-alpine
	docker run -d --name fast-tasks-mysql -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=fast_tasks -p 3399:3306 mysql:8.4
	docker run -d --name fast-tasks-postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=fast_tasks -p 5499:5432 postgres:16-alpine

servers-stop:
	docker rm -f fast-tasks-redis fast-tasks-mysql fast-tasks-postgres

test:
	$(VENV)/bin/python -m pytest

coverage:
	$(VENV)/bin/python -m pytest --cov

lint:
	$(VENV)/bin/python -m ruff check .
	$(VENV)/bin/python -m black --check .

format:
	$(VENV)/bin/python -m ruff check --fix .
	$(VENV)/bin/python -m black .

build:
	$(VENV)/bin/python -m build

clean:
	rm -rf dist build htmlcov .coverage .pytest_cache .ruff_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
