usage:		## Shows usage for this Makefile
	@cat Makefile | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

init:       ## Initialize the test data
	snow sql -c local -f .assets/setup.sql

start:      ## Start up streamlit app
	streamlit run main.py

.PHONY: usage init start
