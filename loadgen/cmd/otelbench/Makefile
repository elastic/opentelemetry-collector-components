include ../../../Makefile.Common

VERSION := v0.4.0

.PHONY: get-version
get-version:
	@echo $(VERSION)

FILENAME?=$(shell git branch --show-current)
.PHONY: chlog-new
chlog-new: $(CHLOGGEN) # Create new changelog entry
	$(CHLOGGEN) new --filename $(FILENAME)

.PHONY: chlog-validate
chlog-validate: $(CHLOGGEN) # Validate the new changelog entry
	$(CHLOGGEN) validate

.PHONY: chlog-preview
chlog-preview: $(CHLOGGEN) # Preview the changelog entry in CHANGELOG.md
	$(CHLOGGEN) update --dry
