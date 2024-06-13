#!/usr/bin/env python3
"""Test reading XML file."""

import pytest

REQUIRED_INPUT_MESSAGE = 'Required input file not available'

def test_loadflags(get_xml_loadfile):
    """Read various flags from XML file."""
    pytest.skip(REQUIRED_INPUT_MESSAGE)
    XML_LOADFILE = get_xml_loadfile()
    assert XML_LOADFILE.flags['load_stat']
    assert not XML_LOADFILE.flags['load_mode']
    assert not XML_LOADFILE.flags['load_mtd']
    assert XML_LOADFILE.flags['load_mpr']
    assert XML_LOADFILE.flags['load_orank']
    assert XML_LOADFILE.flags['verbose']
    assert not XML_LOADFILE.flags['drop_indexes']
    assert not XML_LOADFILE.flags['apply_indexes']
    assert XML_LOADFILE.flags['stat_header_db_check']
    assert not XML_LOADFILE.flags['mode_header_db_check']
    assert not XML_LOADFILE.flags['mtd_header_db_check']
    assert not XML_LOADFILE.flags['force_dup_file']
    assert XML_LOADFILE.flags['load_xml']

def test_loadgroup(get_xml_loadfile):
    """Read group and description from XML file."""
    pytest.skip(REQUIRED_INPUT_MESSAGE)
    XML_LOADFILE = get_xml_loadfile()
    assert XML_LOADFILE.group == "vhagerty"
    assert XML_LOADFILE.description == "v projects"

def test_connection(get_xml_loadfile):
    """Read connection tags from XML file."""
    pytest.skip(REQUIRED_INPUT_MESSAGE)
    XML_LOADFILE = get_xml_loadfile()
    assert XML_LOADFILE.connection['db_host'] == "192.168.0.42"
    assert XML_LOADFILE.connection['db_port'] == 3306
    assert XML_LOADFILE.connection['db_database'] == "mv_test_3"
    assert XML_LOADFILE.connection['db_user'] == "met_admin"
    assert XML_LOADFILE.connection['db_management_system'] == "mysql"

def test_insertsize(get_xml_loadfile):
    """Read insert_size from XML file."""
    pytest.skip(REQUIRED_INPUT_MESSAGE)
    XML_LOADFILE = get_xml_loadfile()
    assert XML_LOADFILE.insert_size == 1
