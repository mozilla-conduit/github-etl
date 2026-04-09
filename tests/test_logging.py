#!/usr/bin/env python3
"""
Tests for setup_logging function.

Tests logging configuration including log level and handler setup.
"""

import logging

import main


def test_setup_logging():
    """Test that setup_logging configures logging correctly."""
    main.setup_logging()

    root_logger = logging.getLogger()
    assert root_logger.level == logging.INFO
    assert len(root_logger.handlers) > 0

    # Check that at least one handler is a StreamHandler
    has_stream_handler = any(
        isinstance(handler, logging.StreamHandler) for handler in root_logger.handlers
    )
    assert has_stream_handler
