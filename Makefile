include ./Makefile.Common

# All source code and documents. Used in spell check.
ALL_DOC := $(shell find . \( -name "*.md" -o -name "*.yaml" \) \
                                -type f | sort)

# ALL_MODULES includes ./* dirs (excludes . dir)
ALL_MODULES := $(shell find . -type f -name "go.mod" -exec dirname {} \; | sort | grep -E '^./' )

GROUP ?= all
FOR_GROUP_TARGET=for-$(GROUP)-target

.DEFAULT_GOAL := all

.PHONY: all
all: misspell

# Append root module to all modules
GOMODULES = $(ALL_MODULES)

# Define a delegation target for each module
.PHONY: $(GOMODULES)
$(GOMODULES):
	@echo "Running target '$(TARGET)' in module '$@'"
	$(MAKE) -C $@ $(TARGET)

# Triggers each module's delegation target
.PHONY: for-all-target
for-all-target: $(GOMODULES)

.PHONY: gomoddownload
gomoddownload:
	@$(MAKE) $(FOR_GROUP_TARGET) TARGET="moddownload"

.PHONY: gotest
gotest:
	@$(MAKE) $(FOR_GROUP_TARGET) TARGET="test"

.PHONY: golint
golint:
	@$(MAKE) $(FOR_GROUP_TARGET) TARGET="lint"

.PHONY: golicense
golicense:
	@$(MAKE) $(FOR_GROUP_TARGET) TARGET="license-check"

.PHONY: gofmt
gofmt:
	@$(MAKE) $(FOR_GROUP_TARGET) TARGET="fmt"

.PHONY: gotidy
gotidy:
	@$(MAKE) $(FOR_GROUP_TARGET) TARGET="tidy"

.PHONY: gogenerate
gogenerate:
	@$(MAKE) $(FOR_GROUP_TARGET) TARGET="generate"
	@$(MAKE) $(FOR_GROUP_TARGET) TARGET="fmt"

.PHONY: gogovulncheck
gogovulncheck:
	$(MAKE) $(FOR_GROUP_TARGET) TARGET="govulncheck"

.PHONY: goporto
goporto:
	$(MAKE) $(FOR_GROUP_TARGET) TARGET="porto"

# Build a collector based on the Elastic components (generate Elastic collector)
.PHONY: genelasticcol
genelasticcol: $(BUILDER)
	$(BUILDER) --config ./distributions/elastic-components/manifest.yaml

# Validate that the Elastic components collector can run with the example configuration.
.PHONY: elasticcol-validate
elasticcol-validate: genelasticcol
	./_build/elastic-collector-components validate --config ./distributions/elastic-components/config.yaml
