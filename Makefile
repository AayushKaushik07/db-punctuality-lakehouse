# Convenience commands. Works on Linux, macOS and Windows (WSL2 or Git Bash).
# Without `make`, run the same steps with `python -m dbpipe <stage>` (see docs/SETUP.md).

PY ?= python
SAMPLE_ENV = DBPIPE_MONTHS=2025-09,2025-10 DBPIPE_TEST_DAYS=10 DBPIPE_LANDING_PATH=data/sample/landing DBPIPE_LAKEHOUSE_PATH=data/sample/lakehouse \
             DBPIPE_ARTIFACTS_PATH=data/sample/artifacts DBPIPE_DOCS_PATH=data/sample/docs

.PHONY: help install download bronze silver gold quality features train report run all \
        sample-run sample-dashboard dashboard test lint format docker-build docker-run docker-dashboard clean

help:           ## show this help
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

install:        ## install Python dependencies + this package (inside your virtualenv)
	$(PY) -m pip install -r requirements-dev.txt
	$(PY) -m pip install -e .

download:       ## download the configured months from Hugging Face
	$(PY) -m dbpipe download

bronze silver gold quality features train report:  ## run a single stage
	$(PY) -m dbpipe $@

run:            ## run every stage after download (bronze -> report)
	$(PY) -m dbpipe run

all: download run  ## download + full pipeline

sample-run:     ## smoke test on a small SYNTHETIC sample (separate folder, never touches real data)
	$(SAMPLE_ENV) DBPIPE_STORAGE_FORMAT=parquet $(PY) -m dbpipe sample
	$(SAMPLE_ENV) DBPIPE_STORAGE_FORMAT=parquet $(PY) -m dbpipe run

sample-dashboard:  ## dashboard on the synthetic sample
	$(SAMPLE_ENV) DBPIPE_STORAGE_FORMAT=parquet streamlit run dashboard/app.py

dashboard:      ## start the Streamlit dashboard on http://127.0.0.1:8501
	streamlit run dashboard/app.py

test:           ## run the test suite
	$(PY) -m pytest

lint:           ## check code style
	ruff check . && ruff format --check .

format:         ## auto-format the code
	ruff check --fix . && ruff format .

docker-build:   ## build the Docker image (includes Java + Delta jars)
	docker compose build

docker-run:     ## run download + full pipeline in Docker
	mkdir -p data artifacts docs
	docker compose run --rm pipeline

docker-dashboard:  ## dashboard in Docker on http://127.0.0.1:8501
	docker compose up dashboard

clean:          ## delete lakehouse tables, sample data and artifacts (keeps downloaded files)
	rm -rf data/lakehouse data/sample artifacts spark-warehouse metastore_db derby.log
