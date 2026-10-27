# Claims Service: Spring Boot on Snowflake JDBC, tested against the Snowflake-Next emulator.
#
#   make up          generate the dev key, pull the emulator image, start a long-lived emulator
#   make seed        migrate that emulator and load deterministic sample claims (idempotent)
#   make run         run the service on http://localhost:8080 against that emulator
#   make test        ./mvnw -q verify: the full suite, on its own Testcontainers emulator
#   make demo        the demo moment: clean build plus full suite, timed, with a per-test summary
#   make down        stop and remove the long-lived emulator
#   make test-cloud  the same suite against real Snowflake (consumes credits)
#
# `make test` and `make demo` need only Docker and JDK 21: Testcontainers starts, provisions
# and removes a fresh emulator for every run, so they do not depend on `make up`.

-include $(HOME)/.localstack/default.env
export LOCALSTACK_AUTH_TOKEN
SNOWFLAKE_PORT ?= 4566
SNOWFLAKE_IMAGE ?= localstack/snowflake-next:latest
export SNOWFLAKE_PORT SNOWFLAKE_IMAGE

COMPOSE := docker compose -p snfs-claims-service
MVNW := ./mvnw -B
SECRETS := .secrets
DEV_KEY := $(SECRETS)/rsa_key.p8
APP_JAR := claims-api/target/claims-api-1.0.0.jar
JAVA_OPTS := -Dnet.snowflake.jdbc.disablePlatformDetection=true
# The default (no profile) config, which targets the compose emulator.
APP := java $(JAVA_OPTS) -jar $(APP_JAR) --claims.snowflake.private-key-path=$(DEV_KEY)

.PHONY: up seed run test demo down test-cloud build logs install start stop

up: $(DEV_KEY)
	$(COMPOSE) pull
	$(COMPOSE) up -d
	scripts/wait-for-emulator.sh 180

build:
	$(MVNW) -q -DskipTests package

seed: build
	$(APP) --spring.main.web-application-type=none --claims.seed.enabled=true

run: build
	$(APP)

test:
	./mvnw -q verify
	@scripts/test-summary.sh

demo:
	scripts/demo.sh

down:
	$(COMPOSE) down -v --remove-orphans

# Reads SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_PATH, SNOWFLAKE_ROLE and
# SNOWFLAKE_WAREHOUSE; creates and drops a CLAIMS_IT_<random> database.
test-cloud:
	TEST_TARGET=SNOWFLAKE_CLOUD ./mvnw -q verify
	@scripts/test-summary.sh

logs:
	$(COMPOSE) logs --tail=200 snowflake

# Throwaway unencrypted PKCS#8 key pair for key-pair (SNOWFLAKE_JWT) auth.
$(DEV_KEY):
	mkdir -p $(SECRETS)
	openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out $@
	openssl rsa -in $@ -pubout -out $(SECRETS)/rsa_key.pub
	chmod 600 $@

# The targets the repository CI runs for every sample (see the root README)
install:
	@echo "Nothing to install, make up and make test fetch the dependencies"

start: up

stop: down
