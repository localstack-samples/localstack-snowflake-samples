VENV_DIR ?= .venv
VENV_RUN = . $(VENV_DIR)/bin/activate
SNOWFLAKE_IMAGE ?= localstack/snowflake-next:latest

usage:		## Shows usage for this Makefile
	@cat Makefile | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install:	## Install Jupyter and the notebook's dependencies into a virtualenv
	test -d $(VENV_DIR) || python3 -m venv $(VENV_DIR)
	$(VENV_RUN) && pip install -q notebook nbconvert ipykernel 'snowflake-snowpark-python[pandas]' \
		pandas matplotlib seaborn scikit-learn

start:		## Start the Snowflake emulator (snowflake-next)
	lstk start --type snowflake --image $(SNOWFLAKE_IMAGE) --non-interactive --timeout 3m

stop:		## Stop the Snowflake emulator
	lstk stop

logs:		## Show the LocalStack logs
	lstk logs --tail 300

notebook:	## Open the notebook in Jupyter
	$(VENV_RUN) && jupyter notebook

test:		## Run every cell of the notebook and check the results in Snowflake
	./test.sh

.PHONY: usage install start stop logs notebook test
