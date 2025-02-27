include ../../../Makefile.Common

VERSION := "v1.0.0"

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

# .PHONY: chlog-update
# chlog-update: $(CHLOGGEN) # Update the CHANGELOG.md
# 	$(CHLOGGEN) update --version $(VERSION)