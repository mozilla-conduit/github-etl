"""
Tests for installation-token rate-limit detection and caching.

Covers:
  - _is_rate_limited: treats both 403 and 429 as rate-limit signals, but only
    when X-RateLimit-Remaining is 0.
  - TokenStore: caches valid tokens, expires tokens within the 60s skew window,
    and hands out a stable per-installation lock.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import main
from main import AccessToken, TokenStore, _is_rate_limited


def _resp(status_code, remaining=None):
    resp = Mock()
    resp.status_code = status_code
    resp.headers = {} if remaining is None else {"X-RateLimit-Remaining": remaining}
    return resp


def test_is_rate_limited_403_remaining_zero():
    assert _is_rate_limited(_resp(403, "0")) is True


def test_is_rate_limited_429_remaining_zero():
    """429 (secondary/abuse rate limit) is handled the same as 403."""
    assert _is_rate_limited(_resp(429, "0")) is True


def test_is_rate_limited_403_with_remaining():
    assert _is_rate_limited(_resp(403, "5")) is False


def test_is_rate_limited_429_with_remaining():
    assert _is_rate_limited(_resp(429, "12")) is False


def test_is_rate_limited_other_status():
    assert _is_rate_limited(_resp(500, "0")) is False


def test_is_rate_limited_missing_header_defaults_not_limited():
    """A missing X-RateLimit-Remaining header defaults to 1 (not limited)."""
    assert _is_rate_limited(_resp(403)) is False


def test_token_store_caches_valid_token():
    store = TokenStore()
    expires = datetime.now(timezone.utc) + timedelta(minutes=30)
    store.store(123, AccessToken(token="abc", expires_at=expires))

    assert store.cached_token(123) == "abc"


def test_token_store_misses_for_unknown_installation():
    store = TokenStore()
    assert store.cached_token(999) is None


def test_token_store_expires_token_within_skew_window():
    """Tokens within 60s of expiry are treated as expired."""
    store = TokenStore()
    expires = datetime.now(timezone.utc) + timedelta(seconds=30)
    store.store(123, AccessToken(token="abc", expires_at=expires))

    assert store.cached_token(123) is None


def test_token_store_lock_is_stable_per_installation():
    store = TokenStore()
    lock_a = store.lock_for(1)
    lock_b = store.lock_for(1)
    lock_c = store.lock_for(2)

    assert lock_a is lock_b
    assert lock_a is not lock_c


def test_module_level_token_store_exists():
    assert isinstance(main.token_store, TokenStore)
