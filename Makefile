SHELL := /bin/bash
PY ?= python
PIP ?= pip

.DEFAULT_GOAL := help

help:
	@echo "Targets:"
	@echo "  make up          - start docker services"
	@echo "  make down        - stop docker services"
	@echo "  make logs        - tail docker logs"
	@echo "  make install     - install project editable"
	@echo "  make fmt         - format (black)"
	@echo "  make lint        - lint (ruff)"
	@echo "  make test        - run tests"
	@echo "  make run-ui      - start streamlit ui"
	@echo "  make topics      - create kafka topics"
	@echo "  make bootstrap   - bootstrap duckdb schema"
	@echo "  make producer    - run replay producer"
	@echo "  make consumer-db - run duckdb consumer"
	@echo "  make consumer-rs - run redshift consumer"

up:
	docker compose up -d

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

install:
	$(PIP) install -U pip
	$(PIP) install -e ".[dev]"

fmt:
	$(PY) -m black .

lint:
	$(PY) -m ruff check .

test:
	$(PY) -m pytest

run-ui:
	$(PY) -m src.main ui

topics:
	bash scripts/create_kafka_topics.sh

bootstrap:
	$(PY) scripts/bootstrap_duckdb.py

producer:
	$(PY) -m src.main producer --input ./data

consumer-db:
	$(PY) -m src.main consumer-duckdb

consumer-rs:
	$(PY) -m src.main consumer-redshift