.PHONY: help install test lint run run-skip-db dashboard docker-up docker-down clean generate-data

help:
	@echo "Available commands:"
	@echo "  make install        Install project dependencies"
	@echo "  make test           Run Pytest suite"
	@echo "  make run            Run full DataFlow pipeline"
	@echo "  make run-skip-db    Run DataFlow pipeline offline (skipping PostgreSQL)"
	@echo "  make dashboard      Launch real-time web monitoring dashboard"
	@echo "  make generate-data  Generate synthetic customer dataset"
	@echo "  make docker-up      Start PostgreSQL container"
	@echo "  make docker-down    Stop PostgreSQL container"
	@echo "  make clean          Clean temporary files and cache"

install:
	pip install -r requirements-dev.txt

test:
	pytest

run:
	python -m src.pipeline

run-skip-db:
	python -m src.pipeline --skip-db

dashboard:
	python app.py

generate-data:
	python scripts/generate_data.py --count 200 --dirty-ratio 0.05

docker-up:
	docker compose up -d

docker-down:
	docker compose down

clean:
	rm -rf __pycache__ .pytest_cache data/processed/ data/quarantine/

