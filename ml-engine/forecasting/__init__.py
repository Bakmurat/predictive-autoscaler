"""Forecast contract v2 (window-max-rps/v1): the statistical forecaster's contracts, history, journal and clock.

Nothing in this package imports the legacy service (api.main), TensorFlow or Keras; tests/test_forecasting_imports.py
holds it to that. Nothing here is wired into the live API yet (design C2, PRs C2.1 and C2.2).
"""
