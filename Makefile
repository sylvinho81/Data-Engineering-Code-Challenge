APP := sales-transactions-etl
SRC_WITH_DEPS ?= sales_transactions_etl
MAIN_PACKAGE_CODE = spark_jobs
IMAGE_NAME ?= docker-$(APP)
IMAGE_VERSION ?= latest
DOCKER_PROJECT_NAME := $(APP)

CONTAINER_CMD ?= docker
CONTAINER_RUN := IMAGE_TAG=$(IMAGE_VERSION) docker compose -p $(DOCKER_PROJECT_NAME) --file docker-compose.yaml run --rm
CONTAINER_RUN_CMD := $(CONTAINER_RUN) $(APP) bash -c
CONTAINER_RUN_STANDALONE_CMD := $(CONTAINER_RUN) --no-deps $(APP) bash -c

POETRY_VERSION := 1.8.3
POETRY_ARGS ?=
POETRY_HOME ?= $(CURDIR)/poetry
POETRY := $(POETRY_HOME)/bin/poetry $(POETRY_ARGS)

COVERAGE_THRESHOLD := 55


help:	## Show this help menu.
	@echo "Usage: make [TARGET ...]"
	@echo ""
	@@egrep -h "#[#]" $(MAKEFILE_LIST) | sed -e 's/\\$$//' | awk 'BEGIN {FS = "[:=].*?#[#] "}; \
	  {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""

image-dev: image_dev    ## build dev docker image
image_%:
	$(CONTAINER_CMD) build \
	 	-t $(IMAGE_NAME):latest \
	 	--target $*-image \
	 	.
install:
	@echo "Running install steps..."

clean:	## CleanUp Prior to Build
	@rm -Rf ./dist-sales
	@rm -Rf ./dist
	@rm -Rf ./${SRC_WITH_DEPS}
	@rm -Rf ./output
	@rm -Rf ./spark-warehouse
	@rm -f requirements.txt
	@rm -Rf .coverage coverage.xml junit.xml htmlcov
	docker system prune -f

clean-docker:	## remove all local docker images related to this project
	docker image rm -f $$(docker images -q $(IMAGE_NAME) | sort -u) 2>/dev/null || true
	docker system prune -f

install-poetry:	## install poetry command if required
install-poetry: INSTALLED_POETRY_VERSION := "$(shell poetry --version 2> /dev/null)"
install-poetry:
	@if [ $(INSTALLED_POETRY_VERSION) = "" ]; then \
		curl -sSL https://install.python-poetry.org | POETRY_HOME=$(POETRY_HOME) POETRY_VERSION=$(POETRY_VERSION) python3 -; \
	fi;

validate-env: ## Validate poetry environment and install missing dependencies
	$(CONTAINER_RUN_CMD) "poetry install --no-root"

build: clean install-poetry ## Build Python Package with Dependencies
	@echo "Packaging Code and Dependencies"
	@$(POETRY) update
	@$(POETRY) build
	@pwd
	## uncomment this line once we have the main pyspark jobs
	@cp $(MAIN_PACKAGE_CODE)/data_preparation.py ./dist
	@cp $(MAIN_PACKAGE_CODE)/data_transformation.py ./dist
	@cp $(MAIN_PACKAGE_CODE)/data_export.py ./dist
	# Dynamically rename files and upload to GCS
	@echo "Renaming files whl and tar.gz"
	@set -e; \
	whl_file=$$(ls dist/sales_transactions_etl-*.whl); \
	tar_file=$$(ls dist/sales_transactions_etl-*.tar.gz); \
	echo "Found wheel file: $$whl_file"; \
	echo "Found tar.gz file: $$tar_file"; \
	mkdir ./dist-sales; \
	mv ./dist/* ./dist-sales; \
	rm -Rf ./dist; \

lint:	## run linter
lint: MYPY_OPTS := > mypy.log
lint: image-dev
	@echo Running linters:
	@echo Running ruff check...
	$(CONTAINER_RUN_STANDALONE_CMD) "poetry run ruff check $(MAIN_PACKAGE_CODE) tests"
	@echo Running mypy ...
	$(CONTAINER_RUN_STANDALONE_CMD) "poetry run mypy --install-types --non-interactive $(MAIN_PACKAGE_CODE) tests $(MYPY_OPTS)"

format: ## Lint all files in the specific directories, and fix any fixable errors and format the code.
format: image-dev
	@echo Running code formatters:
	@echo Running check fix ...
	$(CONTAINER_RUN_STANDALONE_CMD) "poetry run ruff check $(MAIN_PACKAGE_CODE) tests --fix"
	@echo Running ruff format...
	$(CONTAINER_RUN_STANDALONE_CMD) "poetry run ruff format $(MAIN_PACKAGE_CODE) tests"

test-integration:    ## run integration tests and show report
test-integration: PYTEST_OPTS := -v --junit-xml=junit.xml
test-integration: build image-dev validate-env
	@echo Running integration tests
	@echo $(CONTAINER_RUN_CMD) "poetry run coverage run -m tests/integration $(PYTEST_OPTS) $(PYTEST_ARGS)"
	@$(CONTAINER_RUN_CMD) "poetry run coverage run -m pytest tests/integration $(PYTEST_OPTS) $(PYTEST_ARGS)"
	@$(CONTAINER_RUN_CMD) "poetry run coverage xml --fail-under $(COVERAGE_THRESHOLD)"
	@$(CONTAINER_RUN_CMD) "poetry run coverage report"
	@$(CONTAINER_RUN_CMD) "poetry run coverage html -i"

test-unit:    ## run unit tests and show report
test-unit: PYTEST_OPTS := -v --junit-xml=junit.xml
test-unit: clean image-dev validate-env
	@echo Running unit tests
	@echo $(CONTAINER_RUN_CMD) "poetry run coverage run -m tests/unit $(PYTEST_OPTS) $(PYTEST_ARGS)"
	@$(CONTAINER_RUN_CMD) "poetry run coverage run -m pytest tests/unit $(PYTEST_OPTS) $(PYTEST_ARGS)"
	@$(CONTAINER_RUN_CMD) "poetry run coverage xml --fail-under $(COVERAGE_THRESHOLD)"
	@$(CONTAINER_RUN_CMD) "poetry run coverage report"
	@$(CONTAINER_RUN_CMD) "poetry run coverage html -i"

test:    ## run tests and show report
test: PYTEST_OPTS := -v --junit-xml=junit.xml
test: build image-dev validate-env
	@echo Running tests
	@echo $(CONTAINER_RUN_CMD) "poetry run coverage run -m pytest tests/unit tests/integration $(PYTEST_OPTS) $(PYTEST_ARGS)"
	@$(CONTAINER_RUN_CMD) "poetry run coverage run -m pytest tests/unit tests/integration $(PYTEST_OPTS) $(PYTEST_ARGS)"
	@$(CONTAINER_RUN_CMD) "poetry run coverage xml --fail-under $(COVERAGE_THRESHOLD)"
	@$(CONTAINER_RUN_CMD) "poetry run coverage report"
	@$(CONTAINER_RUN_CMD) "poetry run coverage html -i"
