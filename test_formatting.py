"""
Code Style Tests.
"""

import subprocess


def test_black():
    cmd = ("black", "--diff", "main.py")
    output = subprocess.check_output(cmd)
    assert not output, "The python code does not adhere to the project style."


def test_ruff():
    passed = subprocess.call(("ruff", "check", "main.py", "--target-version", "py314"))
    assert not passed, "ruff did not run cleanly."
