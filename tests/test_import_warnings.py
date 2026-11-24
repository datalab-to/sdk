"""Importing the SDK raises no DeprecationWarning, and retries keep their backoff.

tenacity 9.2 deprecated the `initial` argument of wait_exponential_jitter, so
`python -W error::DeprecationWarning -c "import datalab_sdk"` failed with it.
The SDK now computes the same backoff itself.
"""

import inspect
import pkgutil
import random
import subprocess
import sys
from types import SimpleNamespace

import pytest

import datalab_sdk


def test_import_raises_no_deprecation_warning_from_the_sdk():
    # Only warnings attributed to the SDK's own modules fail the import, so a
    # dependency's unrelated warning can't break this test.
    # (-W matches a module name exactly, so the filter is set in code.)
    script = (
        "import warnings\n"
        "warnings.filterwarnings('error', category=DeprecationWarning,"
        " module=r'datalab_sdk(\\.|$)')\n"
        "import datalab_sdk\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_the_sdk_does_not_use_tenacitys_exponential_jitter():
    # The locked tenacity (before 9.2) doesn't warn, so the import test alone
    # can't catch the old call coming back. Check the source instead.
    for module in pkgutil.iter_modules(datalab_sdk.__path__, "datalab_sdk."):
        source = inspect.getsource(__import__(module.name, fromlist=["_"]))
        assert "wait_exponential_jitter(" not in source, module.name


@pytest.mark.parametrize("attempt", [1, 2, 3, 4, 5, 6, 10, 2000])
@pytest.mark.parametrize("jitter", [0.0, 0.5, 1.0])
def test_backoff_is_exponential_from_5_seconds_and_at_most_120(
    monkeypatch, attempt, jitter
):
    from datalab_sdk.client import _wait_retry_after_or_backoff

    monkeypatch.setattr(random, "random", lambda: jitter)
    state = SimpleNamespace(attempt_number=attempt, outcome=None)
    try:
        expected = min(5 * 2 ** (attempt - 1) + jitter, 120)
    except OverflowError:
        expected = 120
    assert _wait_retry_after_or_backoff(state) == pytest.approx(expected)
