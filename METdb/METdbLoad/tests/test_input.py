#!/usr/bin/env python3
"""Test inputs to METdbLoad."""
import METdbLoad
import sys

def test_argv():
    """Look for -index."""
    sys.argv = ["-index"]