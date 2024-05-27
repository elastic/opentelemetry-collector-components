include ./Makefile.Common

# All source code and documents. Used in spell check.
ALL_DOC := $(shell find . \( -name "*.md" -o -name "*.yaml" \) \
                                -type f | sort)

# ALL_MODULES includes ./* dirs (excludes . dir)
ALL_MODULES := $(shell find . -type f -name "go.mod" -exec dirname {} \; | sort | grep -E '^./' )

.DEFAULT_GOAL := all

.PHONY: all
all: misspell

.PHONY: misspell
misspell: $(MISSPELL)
	$(MISSPELL) -error $(ALL_DOC)

.PHONY: misspell-correction
misspell-correction: $(MISSPELL)
	$(MISSPELL) -w $(ALL_DOC)

# Append root module to all modules
GOMODULES = $(ALL_MODULES) $(PWD)

# Define a delegation target for each module
.PHONY: $(GOMODULES)
$(GOMODULES):
	@echo "Running target '$(TARGET)' in module '$@'"
	$(MAKE) -C $@ $(TARGET)

# Triggers each module's delegation target
.PHONY: for-all-target
for-all-target: $(GOMODULES)

.PHONY: golint
golint:
	@$(MAKE) for-all-target TARGET="lint"
