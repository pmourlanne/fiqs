import pytest

import sh


def test_flake8(run_lint):
    if not run_lint:
        return

    try:
        sh.flake8('fiqs/')
    except sh.ErrorReturnCode as e:
        pytest.fail(e.stdout.decode('utf-8'))


def test_isort(run_lint):
    if not run_lint:
        return

    try:
        sh.isort('--recursive', '--check-only', 'fiqs/')
    except sh.ErrorReturnCode as e:
        pytest.fail(e.stdout.decode('utf-8'))
