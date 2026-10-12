PYTHON ?= python
.PHONY: help db-up db-down migrate-up migrate-status run fmt vet test test-race test-integration build smoke verify-stage-01 verify-stage-02 run-scheduler
help db-up db-down migrate-up migrate-status run fmt vet test test-race test-integration build smoke verify-stage-01 verify-stage-02 run-scheduler:
	$(PYTHON) scripts/dev.py $@
