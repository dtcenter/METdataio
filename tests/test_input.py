#!/usr/bin/env python3
"""Test inputs to METdbLoad."""

import sys
import argparse

def test_argxmlfile():
    """Look for -index."""
    sys.argv = ["METdbLoad", "-index", "/Users/venita.hagerty/metviewer/testloads.xml"]

    parser = argparse.ArgumentParser()
    parser.add_argument("xmlfile", help="please provide required xml load_spec filename")
    parser.add_argument("-index", action="store_true", help="only process index commands, do not load data")

    # get the command line arguments
    args =  parser.parse_args()

    assert args.xmlfile == "/Users/venita.hagerty/metviewer/testloads.xml"

def test_argindexyes():
    sys.argv = ["METdbLoad", "-index", "/Users/venita.hagerty/metviewer/testloads.xml"]

    parser = argparse.ArgumentParser()
    parser.add_argument("xmlfile", help="please provide required xml load_spec filename")
    parser.add_argument("-index", action="store_true", help="only process index commands, do not load data")

    # get the command line arguments
    args =  parser.parse_args()

    assert args.index == True

def test_argindexno():
    sys.argv = ["METdbLoad", "/Users/venita.hagerty/metviewer/testloads.xml"]

    parser = argparse.ArgumentParser()
    parser.add_argument("xmlfile", help="please provide required xml load_spec filename")
    parser.add_argument("-index", action="store_true", help="only process index commands, do not load data")

    # get the command line arguments
    args = parser.parse_args()

    assert args.index == False
