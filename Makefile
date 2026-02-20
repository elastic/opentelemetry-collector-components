include ./Makefile.Common

# All source code and documents. Used in spell check.
ALL_DOC := $(shell find . \( -name "*.md" -o -name "*.yaml" \) \
                                -type f | sort)

# ALL_MODULES includes ./* dirs with a go.mod file (excludes . and ./_build dirs)
ALL_MODULES := $(shell find . -type f -name "go.mod" -not -wholename "./go.mod" -not -wholename "./_build/*" -exec dirname {} \; | sort )

FIND_INTEGRATION_TEST_MODS={ find . -type f -name "*integration_test.go" & find . -type f -name "*e2e_test.go"; }
TO_MOD_DIR=dirname {} \; | sort | grep -E '^./'
INTEGRATION_MODS := $(shell $(FIND_INTEGRATION_TEST_MODS) | xargs $(TO_MOD_DIR) | uniq)

GROUP ?= all
FOR_GROUP_TARGET=for-$(GROUP)-target
GOTOOLCHAIN ?= auto

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

.PHONY: for-integration-target
for-integration-target: $(INTEGRATION_MODS)

.PHONY: gomoddownload
gomoddownload:
	@$(MAKE) $(FOR_GROUP_TARGET) TARGET="moddownload"

.PHONY: gotest
gotest:
	@$(MAKE) $(FOR_GROUP_TARGET) TARGET="test"

.PHONY: gointegration-test
gointegration-test:
	@$(MAKE) for-integration-target TARGET="integration-test"

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
	# This is a workaround for a bug in mdatagen upstream: https://github.com/open-telemetry/opentelemetry-collector/issues/13069
	@command -v goimports >/dev/null 2>&1 || $(MAKE) -B install-tools
	@$(MAKE) $(FOR_GROUP_TARGET) TARGET="generate"
	@$(MAKE) $(FOR_GROUP_TARGET) TARGET="fmt"

.PHONY: gogovulncheck
gogovulncheck:
	$(MAKE) $(FOR_GROUP_TARGET) TARGET="govulncheck"

.PHONY: goporto
goporto:
	$(MAKE) $(FOR_GROUP_TARGET) TARGET="porto"

.PHONY: remove-toolchain
remove-toolchain:
	$(MAKE) $(FOR_GROUP_TARGET) TARGET="toolchain"

# Build a collector based on the Elastic components (generate Elastic collector)
.PHONY: genelasticcol
genelasticcol: $(BUILDER)
	GOTOOLCHAIN=${GOTOOLCHAIN} GOOS=${TARGET_GOOS} GOARCH=${TARGET_GOARCH} $(BUILDER) --config ./distributions/elastic-components/manifest.yaml

# Validate that the Elastic components collector can run with the example configuration.
.PHONY: elasticcol-validate
elasticcol-validate: genelasticcol
	./_build/elastic-collector-components validate --config ./distributions/elastic-components/config.yaml

.PHONY: builddocker
builddocker:
	@if [ -z "$(TAG)" ]; then \
		echo "TAG is not set. Please provide a tag using 'make builddocker TAG=<tag>'"; \
		exit 1; \
	fi
	@if [ ! -f "_build/elastic-collector-components" ]; then \
		GOOS=linux $(MAKE) genelasticcol; \
	fi
	@if [ -n "$(USERNAME)" ]; then \
		IMAGE_NAME=$(USERNAME)/elastic-collector-components:$(TAG); \
	else \
		IMAGE_NAME=elastic-collector-components:$(TAG); \
	fi; \
	docker build -t $$IMAGE_NAME -f distributions/elastic-components/Dockerfile .

# Validate that the Elastic components collector can run with the example otelsoak configuration.
.PHONY: otelsoak-validate
otelsoak-validate: genelasticcol
	ELASTIC_APM_SERVER_URL=http://localhost:8200 ELASTIC_APM_API_KEY=foobar ./loadgen/cmd/otelsoak/otelsoak validate --config ./loadgen/cmd/otelsoak/config.example.yaml

# Run otelsoak
.PHONY: otelsoak-run
otelsoak-run: genelasticcol
	./loadgen/cmd/otelsoak/otelsoak --config ./loadgen/cmd/otelsoak/config.example.yaml $(ARGS)


# Clones the upstream opentelemetry-collector repository in a temporal .release
# directory. If the directory already exists,
UPSTREAM_REPO_URL := https://github.com/open-telemetry/opentelemetry-collector.git
RELEASE_REPO_DIR := $(SRC_ROOT)/.release/opentelemetry-collector
$(RELEASE_REPO_DIR):
	echo "Cloning repository into $@..."; \
	mkdir -p $@; \
	git clone $(UPSTREAM_REPO_URL) $@ ; \

.PHONY: update-otel
update-otel: $(MULTIMOD) $(RELEASE_REPO_DIR)
	@(echo "Pulling upstream $(RELEASE_REPO_DIR)..."; cd $(RELEASE_REPO_DIR); git pull origin main)
	$(MULTIMOD) sync -s=true -o $(RELEASE_REPO_DIR) -m stable --commit-hash "$(OTEL_STABLE_VERSION)"
	git add . && git commit -s -m "[chore] multimod update stable modules" ; \
	$(MULTIMOD) sync -s=true -o $(RELEASE_REPO_DIR) -m beta --commit-hash "$(OTEL_VERSION)"
	git add . && git commit -s -m "[chore] multimod update beta modules" ; \
	$(MAKE) gotidy
	$(MAKE) -B install-tools
	$(MAKE) genelasticcol
	$(MAKE) gogenerate license-update
	# Tidy again after generating code
	$(MAKE) gotidy
	$(MAKE) remove-toolchain
	git add . && git commit -s -m "[chore] mod and toolchain tidy" ; \

COMMIT?=HEAD
MODSET?=edot-base
REMOTE?=git@github.com:elastic/opentelemetry-collector-components.git
.PHONY: push-tags
push-tags: $(MULTIMOD)
	$(MULTIMOD) verify
	set -e; for tag in `$(MULTIMOD) tag -m ${MODSET} -c ${COMMIT} --print-tags | grep -v "Using" `; do \
		echo "pushing tag $${tag}"; \
		git push ${REMOTE} $${tag}; \
	done;

.PHONY: clean
clean:
	rm -r $(dir $(RELEASE_REPO_DIR))
