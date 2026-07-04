.DEFAULT_GOAL := help
.PHONY: help tests

## Show help for all commands
help:
	@echo "Available commands:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z0-9._-]+:.*?## / {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

tests: ## Run unit tests with an 80% coverage gate for the core library modules
	poetry run pytest
