# Publication is gated on `make verify` returning 0. See scripts/verify.sh and README "Testing".
.PHONY: setup verify verify-selftest verify-python verify-go precommit-selftest

setup:
	@git config core.hooksPath .githooks
	@echo "commit gate enabled: .githooks/pre-commit verifies the staged tree (see README, Testing)"

verify:
	@scripts/verify.sh

verify-python:
	@scripts/verify.sh --python-only

verify-go:
	@scripts/verify.sh --go-only

verify-selftest:
	@scripts/verify-selftest.sh

precommit-selftest:
	@scripts/precommit-selftest.sh
