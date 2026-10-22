PYTHON ?= python
.PHONY: help db-up db-down migrate-up migrate-status run fmt vet test test-race test-integration build smoke verify-stage-01 verify-stage-02 run-scheduler
.PHONY: verify-stage-03 verify-stage-04 proto-check proto-generate run-coordinator run-relay run-worker
.PHONY: verify-stage-05 sandbox-prepare sandbox-fixture sandbox-demo sandbox-test
help db-up db-down migrate-up migrate-status run fmt vet test test-race test-integration build smoke verify-stage-01 verify-stage-02 run-scheduler:
	$(PYTHON) scripts/dev.py $@
verify-stage-03 verify-stage-04 proto-check proto-generate run-coordinator run-relay run-worker:
	$(PYTHON) scripts/dev.py $@
verify-stage-05 sandbox-prepare sandbox-fixture sandbox-demo sandbox-test:
	$(PYTHON) scripts/dev.py $@

.PHONY: agent-demo-fixture agent-test verify-stage-06 agent-runtime-prepare
agent-demo-fixture agent-test verify-stage-06 agent-runtime-prepare:
	$(PYTHON) scripts/dev.py $@

.PHONY: verify-stage-07 collaboration-fixture collaboration-test dashboard-test
verify-stage-07 collaboration-fixture collaboration-test dashboard-test:
	$(PYTHON) scripts/dev.py $@
