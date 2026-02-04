"""
Code Style Tests.
"""

import subprocess


def test_black():
    cmd = ("black", "--diff", ".")
    output = subprocess.check_output(cmd)
    assert not output, "The python code does not adhere to the project style."


def test_ruff():
    passed = subprocess.call(("ruff", "check", "."))
    assert not passed, "ruff did not run cleanly."
