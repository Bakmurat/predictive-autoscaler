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

## Before the first release (once)
- Add a repository ruleset for tags `v*`: only the maintainer may create them; updates and deletions are blocked. This
  must be in place **before the first tag is pushed**, because a workflow cannot protect its own tags.
- After the first release, make the three GHCR packages public (package settings on GitHub).

## Steps
1. Set `version` and `appVersion` in `charts/predictive-autoscaler/Chart.yaml` to `X.Y.Z` (the workflow refuses a tag
   that differs from either), update `docs/upgrading.md`, and merge to `main` with every CI check green (the workflow
   checks them on the tagged commit itself).
2. Optional dry run: run the `release` workflow by hand with `version: X.Y.Z`. It runs the gate, builds both images and
   checks the packaged chart, and pushes nothing.
3. Tag and push: `git tag -s vX.Y.Z -m "vX.Y.Z" && git push origin vX.Y.Z`.
4. If a release fails half-way (for example one image pushed), that version is **abandoned**: never delete its packages
   to reuse the number, and never rerun it (the preflight refuses). Fix the cause and release the next patch version.

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
