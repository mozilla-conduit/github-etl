"""
Tests for GitHub App JWT generation (generate_github_jwt).

Uses a dynamically-generated RSA key so no real credentials are needed.
Covers:
  - Correct claims (iss, iat, exp)
  - RS256 algorithm
  - Proper handling of PEM keys whose newlines were serialized as \\n literals
    (the common Docker / CI environment-variable encoding bug)
"""

import time

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from main import generate_github_jwt


@pytest.fixture(scope="module")
def rsa_key_pair() -> tuple[str, str]:
    """Generate a fresh RSA-2048 key pair; returns (private_pem, public_pem)."""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    public_pem = (
        private_key.public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )
    return private_pem, public_pem


class TestGenerateGithubJwt:
    def test_returns_string(self, rsa_key_pair: tuple[str, str]) -> None:
        private_pem, _ = rsa_key_pair
        token = generate_github_jwt("12345", private_pem)
        assert isinstance(token, str)
        assert len(token) > 0

    def test_algorithm_is_rs256(self, rsa_key_pair: tuple[str, str]) -> None:
        private_pem, public_pem = rsa_key_pair
        token = generate_github_jwt("12345", private_pem)
        header = jwt.get_unverified_header(token)
        assert header["alg"] == "RS256"

    def test_iss_claim_matches_app_id(self, rsa_key_pair: tuple[str, str]) -> None:
        private_pem, public_pem = rsa_key_pair
        app_id = "99887"
        token = generate_github_jwt(app_id, private_pem)
        claims = jwt.decode(token, public_pem, algorithms=["RS256"])
        assert claims["iss"] == app_id

    def test_iat_is_backdated_60_seconds(self, rsa_key_pair: tuple[str, str]) -> None:
        """iat should be ~60 s in the past to absorb clock skew."""
        private_pem, public_pem = rsa_key_pair
        before = int(time.time())
        token = generate_github_jwt("12345", private_pem)
        claims = jwt.decode(
            token,
            public_pem,
            algorithms=["RS256"],
            options={"verify_iat": False},
        )
        assert claims["iat"] <= before - 59

    def test_exp_is_roughly_9_minutes(self, rsa_key_pair: tuple[str, str]) -> None:
        """exp should be ~540 s from now (GitHub maximum is 600 s)."""
        private_pem, public_pem = rsa_key_pair
        now = int(time.time())
        token = generate_github_jwt("12345", private_pem)
        claims = jwt.decode(token, public_pem, algorithms=["RS256"])
        ttl = claims["exp"] - now
        assert 530 <= ttl <= 550, f"Expected TTL near 540 s, got {ttl}"

    def test_token_verifies_with_public_key(
        self, rsa_key_pair: tuple[str, str]
    ) -> None:
        """The token must be verifiable with the corresponding public key."""
        private_pem, public_pem = rsa_key_pair
        token = generate_github_jwt("12345", private_pem)
        # Raises if signature is invalid or token is malformed
        claims = jwt.decode(token, public_pem, algorithms=["RS256"])
        assert "iss" in claims

    def test_escaped_newlines_in_pem_are_normalized(
        self, rsa_key_pair: tuple[str, str]
    ) -> None:
        """
        When GITHUB_PRIVATE_KEY is passed via Docker / CI, real newlines are often
        serialized as the two-character sequence \\n.  main._main() normalizes this
        before calling generate_github_jwt.  Verify the function still works when
        the caller passes an already-normalized key (i.e. real newlines).

        This test also covers the normalization path directly: we convert the key to
        the escaped form and then apply the same replace() that _main() uses, then
        confirm the result produces a valid JWT.
        """
        private_pem, public_pem = rsa_key_pair

        # Simulate the escaped form as stored in an env var
        escaped_pem = private_pem.replace("\n", "\\n")
        assert (
            "\\n" in escaped_pem
        ), "Sanity check: escaped form should contain literal \\n"

        # Apply the normalization that _main() applies
        normalized_pem = escaped_pem.replace("\\n", "\n")
        assert (
            normalized_pem == private_pem
        ), "Normalization should restore original PEM"

        # JWT generation must succeed with the normalized key
        token = generate_github_jwt("12345", normalized_pem)
        claims = jwt.decode(token, public_pem, algorithms=["RS256"])
        assert claims["iss"] == "12345"
