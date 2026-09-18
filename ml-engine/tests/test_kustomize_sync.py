"""Tests for PRED-03: Kustomize CronJob image tag sync with ML API Deployment.

Verifies that kustomization.yaml images stanza is the single source of truth
and that rendering via 'kubectl kustomize' produces identical image tags for
both the Deployment and the CronJob.
"""

import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent


class TestKustomizeImageStanza:
    """Test that kustomization.yaml has a valid images stanza for the ML API."""

    def test_kustomization_has_images_stanza(self):
        """kustomization.yaml must contain an 'images:' block."""
        kustomization = PROJECT_ROOT / "k8s-manifests" / "base" / "kustomization.yaml"
        content = kustomization.read_text()
        assert "images:" in content, "kustomization.yaml is missing 'images:' stanza"

    def test_kustomization_has_ml_api_image_name(self):
        """images stanza must name the ML API image."""
        kustomization = PROJECT_ROOT / "k8s-manifests" / "base" / "kustomization.yaml"
        content = kustomization.read_text()
        assert (
            "registry.example.com/predictive-autoscaler/predictive-autoscaler-ml-api" in content
        ), "kustomization.yaml images stanza missing ML API image name"

    def test_kustomization_has_new_tag(self):
        """images stanza must contain a newTag field."""
        kustomization = PROJECT_ROOT / "k8s-manifests" / "base" / "kustomization.yaml"
        content = kustomization.read_text()
        assert "newTag:" in content, "kustomization.yaml images stanza missing 'newTag:' field"

    def test_new_tag_is_not_empty(self):
        """newTag value must be a non-empty string (e.g. 'v3.9.4')."""
        kustomization = PROJECT_ROOT / "k8s-manifests" / "base" / "kustomization.yaml"
        content = kustomization.read_text()
        for line in content.splitlines():
            if "newTag:" in line:
                tag_value = line.split("newTag:")[1].strip()
                assert tag_value, "newTag in kustomization.yaml is empty"
                assert tag_value.startswith("v"), f"newTag '{tag_value}' does not look like a version tag"
                return
        pytest.fail("No newTag line found in kustomization.yaml")


class TestKustomizeRenderSync:
    """Test that 'kubectl kustomize' renders both Deployment and CronJob with the same image tag."""

    @pytest.fixture(scope="class")
    def rendered_yaml(self):
        """Run 'kubectl kustomize' and return the rendered output."""
        result = subprocess.run(
            ["kubectl", "kustomize", str(PROJECT_ROOT / "k8s-manifests" / "base")],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.skip(f"kubectl kustomize failed (kubectl may not be installed): {result.stderr}")
        return result.stdout

    def test_rendered_output_contains_ml_api_image_twice(self, rendered_yaml):
        """Both Deployment and CronJob must reference the ML API image."""
        ml_api_lines = [
            line
            for line in rendered_yaml.splitlines()
            if "predictive-autoscaler-ml-api" in line
        ]
        assert len(ml_api_lines) >= 2, (
            f"Expected at least 2 references to predictive-autoscaler-ml-api in rendered YAML, "
            f"found {len(ml_api_lines)}: {ml_api_lines}"
        )

    def test_all_ml_api_image_references_have_same_tag(self, rendered_yaml):
        """Every occurrence of the ML API image in an 'image:' field must have the same tag."""
        # Only consider lines that contain 'image:' followed by the ML API image name
        # (excludes 'name: registry...' lines from VMServiceScrape or other non-image fields)
        image_lines = [
            line.strip()
            for line in rendered_yaml.splitlines()
            if "image:" in line and "predictive-autoscaler-ml-api" in line
        ]
        assert len(image_lines) >= 2, (
            f"Expected at least 2 'image:' lines referencing predictive-autoscaler-ml-api, "
            f"found {len(image_lines)}"
        )
        # Extract tag from each image line (format: image: registry/name:tag)
        tags = set()
        for line in image_lines:
            image_ref = line.split("image:")[-1].strip()
            if ":" in image_ref:
                tag = image_ref.rsplit(":", 1)[-1]
                tags.add(tag)

        assert len(tags) == 1, (
            f"ML API image 'image:' lines have mismatched tags in rendered YAML: {tags}. "
            f"All image lines: {image_lines}"
        )

    def test_rendered_tag_matches_kustomization_new_tag(self, rendered_yaml):
        """The rendered tag in all 'image:' lines must match the newTag in kustomization.yaml."""
        kustomization = PROJECT_ROOT / "k8s-manifests" / "base" / "kustomization.yaml"
        content = kustomization.read_text()
        new_tag = None
        for line in content.splitlines():
            if "newTag:" in line:
                new_tag = line.split("newTag:")[1].strip()
                break
        assert new_tag is not None, "Could not find newTag in kustomization.yaml"

        # Only check lines that have 'image:' and the ML API image name
        image_lines = [
            line
            for line in rendered_yaml.splitlines()
            if "image:" in line and "predictive-autoscaler-ml-api" in line
        ]
        for line in image_lines:
            assert new_tag in line, (
                f"Rendered image line '{line.strip()}' does not contain expected tag '{new_tag}'"
            )
