# Publication is gated on `make verify` returning 0. See scripts/verify.sh and README "Testing".
.PHONY: verify verify-selftest verify-python verify-go

verify:
	@scripts/verify.sh

verify-python:
	@scripts/verify.sh --python-only

verify-go:
	@scripts/verify.sh --go-only

verify-selftest:
	@scripts/verify-selftest.sh
