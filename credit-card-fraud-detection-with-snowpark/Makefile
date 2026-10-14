#!/usr/bin/env make

export APP_VERSION ?= $(shell git rev-parse --short HEAD)
export GIT_REPO_ROOT ?= $(shell git rev-parse --show-toplevel)

export DOCKER_IMAGE_NAME ?= localstack-credit-card-fraud-detection
# Flags for `docker run`; CI has no TTY, so it sets this to an empty value
DOCKER_RUN_FLAGS ?= -it

check:
	@echo "Checking system dependencies..."
	# GNU Make 4.3
	# Built for x86_64-pc-linux-gnu
	# Copyright (C) 1988-2020 Free Software Foundation, Inc.
	# License GPLv3+: GNU GPL version 3 or later <http://gnu.org/licenses/gpl.html>
	# This is free software: you are free to change and redistribute it.
	# There is NO WARRANTY, to the extent permitted by law.
	echo $$(make --version)
	# Docker version 26.0.0, build 2ae903e
	echo $$(docker --version)

install: build

start:
	lstk start --type snowflake --image localstack/snowflake-next:latest --non-interactive --timeout 3m

logs:
	lstk logs --tail 300

test:
	./test.sh

build:
	docker build \
			--file ./Dockerfile \
			--tag $(DOCKER_IMAGE_NAME):$(APP_VERSION) \
			.

# From: https://stackoverflow.com/a/14061796
# If the first argument is "docker-run"...
ifeq (localstack-run,$(firstword $(MAKECMDGOALS)))
  # use the rest as arguments for "docker-run"
  RUN_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  # ...and turn them into do-nothing targets
  $(eval $(RUN_ARGS):;@:)
endif

# From: https://stackoverflow.com/a/14061796
# If the first argument is "docker-run"...
ifeq (snowflake-run,$(firstword $(MAKECMDGOALS)))
  # use the rest as arguments for "docker-run"
  RUN_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  # ...and turn them into do-nothing targets
  $(eval $(RUN_ARGS):;@:)
endif

# Lifts command into `docker run` context.
#
# NOTE: Make sure you have a `.env.localstack` environment file copied from
# `.env.sample` into the same directory, and the LocalStack for Snowflake Docker
# emulator running (`make start`).
localstack-run: build
	docker run \
		--rm \
		$(DOCKER_RUN_FLAGS) \
		-v ./:/app \
		--env-file=.env.localstack \
		-e ENV_FILE=.env.localstack \
		--net=host \
		$(DOCKER_IMAGE_NAME):$(APP_VERSION) \
		bash -c "$(RUN_ARGS)"

# Lifts command into `docker run` context.
#
# NOTE: Make sure you have a `.env.snowflake` environment file copied from
# `.env.sample` into the same directory, and Snowflake Cloud with the session
# credentials container active and accessible.
snowflake-run: build
	docker run \
		--rm \
		$(DOCKER_RUN_FLAGS) \
		-v ./:/app \
		--env-file=.env.snowflake \
		-e ENV_FILE=.env.snowflake \
		--net=host \
		$(DOCKER_IMAGE_NAME):$(APP_VERSION) \
		bash -c "$(RUN_ARGS)"

# Runs the command $(2) in the sample's Docker image with the env file $(1).
define docker_run
docker run \
		--rm \
		$(DOCKER_RUN_FLAGS) \
		-v ./:/app \
		--env-file=$(1) \
		-e ENV_FILE=$(1) \
		--net=host \
		$(DOCKER_IMAGE_NAME):$(APP_VERSION) \
		bash -c "$(2)"
endef

update-deps: build
	$(call docker_run,.env.localstack,pip-compile --generate-hashes --allow-unsafe --output-file=requirements.txt requirements.in)

autoformat: build
	$(call docker_run,.env.localstack,black .)

load-tables: build
	$(call docker_run,.env.localstack,python 00_load_tables.py)

feature-engineering: build
	$(call docker_run,.env.localstack,python 01_feature_engineering.py)

train-model: build
	$(call docker_run,.env.localstack,python 02_train_model.py)

sf-load-tables: build
	$(call docker_run,.env.snowflake,python 00_load_tables.py)

sf-feature-engineering: build
	$(call docker_run,.env.snowflake,python 01_feature_engineering.py)

sf-train-model: build
	$(call docker_run,.env.snowflake,python 02_train_model.py)
