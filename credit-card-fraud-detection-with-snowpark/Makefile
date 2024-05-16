#!/usr/bin/env make

export APP_VERSION ?= $(shell git rev-parse --short HEAD)
export GIT_REPO_ROOT ?= $(shell git rev-parse --show-toplevel)

export DOCKER_IMAGE_NAME ?= localstack-credit-card-fraud-detection

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
# container running and accessible:
# https://snowflake.localstack.cloud/getting-started/installation/
localstack-run: build
	docker run \
		--rm \
		-it \
		-v ./:/app \
		--env-file=.env.localstack \
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
		-it \
		-v ./:/app \
		--env-file=.env.snowflake \
		--net=host \
		$(DOCKER_IMAGE_NAME):$(APP_VERSION) \
		bash -c "$(RUN_ARGS)"

update-deps:
	$(MAKE) localstack-run 'pip-compile --generate-hashes --allow-unsafe --output-file=requirements.txt requirements.in'

autoformat:
	$(MAKE) localstack-run 'black .'

load-tables:
	$(MAKE) localstack-run 'python 00_load_tables.py'

feature-engineering:
	$(MAKE) localstack-run 'python 01_feature_engineering.py'

train-model:
	$(MAKE) localstack-run 'python 02_train_model.py'

sf-feature-engineering:
	$(MAKE) snowflake-run 'python 01_feature_engineering.snowflake.py'

sf-train-model:
	$(MAKE) snowflake-run 'python 02_train_model.snowflake.py'
