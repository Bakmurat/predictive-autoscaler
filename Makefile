# Publication is gated on `make verify` returning 0. See scripts/verify.sh and README "Testing".
.PHONY: setup verify verify-selftest verify-python verify-go precommit-selftest envtest manifests manifests-check chart-check

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

# The CRD and the deepcopy functions are generated from the API types' markers (controller-gen, pinned in the script).
manifests:
	@scripts/gen-manifests.sh write

manifests-check:
	@scripts/gen-manifests.sh check

# The chart's static checks (helm lint/template, kubeconform for Kubernetes 1.33-1.35); needs helm and kubeconform.
chart-check:
	@scripts/chart-check.sh

precommit-selftest:
	@scripts/precommit-selftest.sh

# The operator's write path against a real Kubernetes API server: setup-envtest (pinned) downloads kube-apiserver and
# etcd for ENVTEST_K8S_VERSION into ENVTEST_BIN_DIR (checksums verified), then the TestEnvtest* tests run with them.
ENVTEST_K8S_VERSION ?= 1.35.0
SETUP_ENVTEST_VERSION ?= v0.0.0-20260305142021-f9589b9f2b9d
ENVTEST_BIN_DIR ?= $(HOME)/.local/share/kubebuilder-envtest

envtest:
	@assets=$$(go run sigs.k8s.io/controller-runtime/tools/setup-envtest@$(SETUP_ENVTEST_VERSION) use $(ENVTEST_K8S_VERSION) --bin-dir "$(ENVTEST_BIN_DIR)" -p path) && \
	cd k8s-operator && env -u KUBERNETES_SERVICE_HOST -u KUBERNETES_SERVICE_PORT KUBECONFIG=/dev/null KUBEBUILDER_ASSETS="$$assets" ENVTEST_REQUIRED=1 go test -race -count=1 -run Envtest -v ./controllers/
