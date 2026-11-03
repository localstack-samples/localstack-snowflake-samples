# Order-to-Cash CDC: Kafka -> Snowflake Kafka connector -> stream -> task -> core and mart.
#
#   make up        generate the dev key, start emulator + Kafka + Connect, register the sink
#   make seed      push the seeded change events through the O2C deployment
#   make demo      forced failure, rollback, then exactly-once apply (readable output)
#   make test      full suite against the running stack (each run uses its own database)
#   make down      stop and remove everything

-include $(HOME)/.localstack/default.env
export LOCALSTACK_AUTH_TOKEN
SNOWFLAKE_PORT ?= 4566
KAFKA_PORT ?= 19092
CONNECT_PORT ?= 18083
export SNOWFLAKE_PORT KAFKA_PORT CONNECT_PORT

COMPOSE := docker compose -p snfs-order-to-cash-cdc
UV_RUN := uv run --frozen
SECRETS := .secrets
SNOWFLAKE_PRIVATE_KEY_PATH ?= $(SECRETS)/rsa_key.p8
export SNOWFLAKE_PRIVATE_KEY_PATH
PYTEST_ARGS ?=

.PHONY: up seed test demo down test-cloud logs install start stop

up: $(SNOWFLAKE_PRIVATE_KEY_PATH) $(SECRETS)/connector.properties
	uv sync --frozen
	$(COMPOSE) up -d --build
	scripts/wait-for-emulator.sh 180
	$(UV_RUN) python -m pipeline.kafka_connect setup

seed:
	$(UV_RUN) python -m pipeline.seed

test:
	$(UV_RUN) pytest $(PYTEST_ARGS)

demo:
	$(UV_RUN) python -m pipeline.demo

down:
	$(COMPOSE) down -v --remove-orphans

# Real Snowflake: Kafka and Connect still run locally (make up), the sink and
# the tests point at SNOWFLAKE_ACCOUNT. Consumes credits.
test-cloud:
	TEST_TARGET=SNOWFLAKE_CLOUD $(UV_RUN) pytest $(PYTEST_ARGS)

logs:
	$(COMPOSE) logs --tail=200 connect snowflake

# Throwaway unencrypted PKCS#8 key pair for key-pair (SNOWFLAKE_JWT) auth.
$(SECRETS)/rsa_key.p8:
	mkdir -p $(SECRETS)
	openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out $@
	openssl rsa -in $@ -pubout -out $(SECRETS)/rsa_key.pub
	chmod 600 $@

# The connector takes the key as one base64 line without PEM armor. Connect
# reads it through FileConfigProvider; the file must be readable by the
# container's user.
$(SECRETS)/connector.properties: $(SNOWFLAKE_PRIVATE_KEY_PATH)
	mkdir -p $(SECRETS)
	printf 'snowflake.private.key=%s\n' "$$(grep -v -- '-----' $< | tr -d '\n')" > $@
	chmod 644 $@

# The targets the repository CI runs for every sample (see the root README)
install:
	@echo "Nothing to install, make up and make test fetch the dependencies"

start: up

stop: down
