# Releasing

A release is a version tag `vX.Y.Z` on `main`. The `release` workflow (`.github/workflows/release.yml`) then publishes:

| Artifact | Where | Platforms |
|---|---|---|
| operator image | `ghcr.io/bakmurat/predictive-autoscaler-operator:X.Y.Z` | linux/amd64, linux/arm64 |
| forecaster image | `ghcr.io/bakmurat/predictive-autoscaler-ml-api:X.Y.Z` | linux/amd64, linux/arm64 |
| Helm chart | `oci://ghcr.io/bakmurat/charts/predictive-autoscaler`, version `X.Y.Z` | — |

Each image carries an SBOM and SLSA provenance. The images and the chart are signed keyless with cosign; the signing
identity is this repository's `release` workflow at that exact tag. Before anything is pushed, the workflow requires:
- the tag equals the chart's version and appVersion;
- the tagged commit is on `main`, and every required CI check passed on it;
- the packaged chart's default images are this release's images;
- none of the artifacts exists yet (versions are immutable).
Each run uploads a `release-inventory-X.Y.Z` artifact (version, source commit, image and chart digests).

## Steps
1. Set `version` and `appVersion` in `charts/predictive-autoscaler/Chart.yaml` to `X.Y.Z` (the workflow refuses a tag
   that differs from either), update `docs/upgrading.md`, and merge to `main` with CI green.
2. Optional dry run: run the `release` workflow by hand with `version: X.Y.Z` (it builds both images and packages the
   chart, and pushes nothing).
3. Tag and push: `git tag -s vX.Y.Z -m "vX.Y.Z" && git push origin vX.Y.Z`.
4. The first time only: make the three GHCR packages public (package settings on GitHub), and add a repository
   ruleset for tags `v*` that allows only the maintainer to create them and blocks updates and deletions (a workflow
   cannot protect its own tags).
5. If a release fails half-way (for example one image pushed), do not rerun it: versions are immutable, and the rerun
   refuses. Delete the partial package versions on GitHub, or release the next patch version.

## Installing a release
```bash
helm install pa oci://ghcr.io/bakmurat/charts/predictive-autoscaler --version X.Y.Z \
  -n predictive-autoscaler --create-namespace -f my-values.yaml
```

## Verifying a release
```bash
cosign verify ghcr.io/bakmurat/predictive-autoscaler-operator@sha256:<digest from the inventory> \
  --certificate-identity 'https://github.com/Bakmurat/predictive-autoscaler/.github/workflows/release.yml@refs/tags/vX.Y.Z' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```
The same command verifies the forecaster image and the chart (`ghcr.io/bakmurat/charts/predictive-autoscaler@sha256:…`).
To pin an installation to the verified chart, install by digest:
`helm install pa oci://ghcr.io/bakmurat/charts/predictive-autoscaler@sha256:<chart digest> -n predictive-autoscaler -f my-values.yaml`.
