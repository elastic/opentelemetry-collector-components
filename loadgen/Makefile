include ../Makefile.Common

.PHONY: build
build: $(BUILDER)
	$(BUILDER) --config ocb.yaml

.PHONY: run
run:
	./_build/loadgencol --config otelcol.yaml
