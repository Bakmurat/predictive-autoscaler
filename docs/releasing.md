# Releasing

A release is a version tag `vX.Y.Z` on `main`. The `release` workflow (`.github/workflows/release.yml`) then publishes:

| Artifact | Where | Platforms |
|---|---|---|
| operator image | `ghcr.io/bakmurat/predictive-autoscaler-operator:X.Y.Z` | linux/amd64, linux/arm64 |
| forecaster image | `ghcr.io/bakmurat/predictive-autoscaler-ml-api:X.Y.Z` | linux/amd64, linux/arm64 |
| Helm chart | `oci://ghcr.io/bakmurat/charts/predictive-autoscaler`, version `X.Y.Z` | — |

Each image carries an SBOM and SLSA provenance. The images and the chart are signed keyless with cosign (the signing
identity is this repository's `release` workflow on that tag).

## Steps
1. Set `version` and `appVersion` in `charts/predictive-autoscaler/Chart.yaml` to `X.Y.Z` (the workflow refuses a tag
   that differs from either), update `docs/upgrading.md`, and merge to `main` with CI green.
2. Optional dry run: run the `release` workflow by hand with `version: X.Y.Z` (it builds both images and packages the
   chart, and pushes nothing).
3. Tag and push: `git tag -s vX.Y.Z -m "vX.Y.Z" && git push origin vX.Y.Z`.
4. The first time only: make the three GHCR packages public (package settings on GitHub).

## Installing a release
```bash
helm install pa oci://ghcr.io/bakmurat/charts/predictive-autoscaler --version X.Y.Z \
  -n predictive-autoscaler --create-namespace -f my-values.yaml
```

## Verifying a release
```bash
cosign verify ghcr.io/bakmurat/predictive-autoscaler-operator:X.Y.Z \
  --certificate-identity-regexp '^https://github.com/Bakmurat/predictive-autoscaler/\.github/workflows/release\.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```
The same command verifies the forecaster image and the chart
(`ghcr.io/bakmurat/charts/predictive-autoscaler:X.Y.Z`).
