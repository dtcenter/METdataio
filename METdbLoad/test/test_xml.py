#!/usr/bin/env python3

import os
import pytest

import utils
from read_load_xml import XmlLoadFile

"""Test reading XML file."""

# Location of the XML specification files that are used to test XML validation
TEST_XML_SPECIFICATION_FILEPATH = os.path.join(os.path.dirname(os.path.abspath(__file__)))

def test_loadflags(tmp_path, get_xml_loadfile):
    """Read various flags from XML file."""
    XML_LOADFILE = get_xml_loadfile(tmp_path)
    assert XML_LOADFILE.flags['load_stat']
    assert XML_LOADFILE.flags['load_mode']
    assert XML_LOADFILE.flags['load_mtd']
    assert XML_LOADFILE.flags['load_mpr']
    assert XML_LOADFILE.flags['load_orank']
    assert XML_LOADFILE.flags['verbose']
    assert not XML_LOADFILE.flags['drop_indexes']
    assert not XML_LOADFILE.flags['apply_indexes']
    assert XML_LOADFILE.flags['stat_header_db_check']
    assert not XML_LOADFILE.flags['mode_header_db_check']
    assert not XML_LOADFILE.flags['mtd_header_db_check']
    assert XML_LOADFILE.flags['force_dup_file']
    assert XML_LOADFILE.flags['load_xml']

def test_loadgroup(tmp_path, get_xml_loadfile):
    """Read group and description from XML file."""
    XML_LOADFILE = get_xml_loadfile(tmp_path)
    assert XML_LOADFILE.group == "Testing"
    assert XML_LOADFILE.description == "testing with pytest"

def test_connection(tmp_path, get_xml_loadfile):
    """Read connection tags from XML file."""
    XML_LOADFILE = get_xml_loadfile(tmp_path)
    assert XML_LOADFILE.connection['db_host'] == "localhost"
    assert XML_LOADFILE.connection['db_port'] == 3306
    assert XML_LOADFILE.connection['db_database'] == "mv_test"
    assert XML_LOADFILE.connection['db_user'] == "root"
    assert XML_LOADFILE.connection['db_management_system'] == "mysql"

def test_insertsize(tmp_path, get_xml_loadfile):
    """Read insert_size from XML file."""
    XML_LOADFILE = get_xml_loadfile(tmp_path)
    assert XML_LOADFILE.insert_size == 1

def test_validation_recursive_payload(get_specified_xml_loadfile):
    """
       Test validation against attempted recursive payload, ValueError should be raised for
       the test_recursive_payload.xml XML-specification file.
    """
    # Get the XML specification file that has a recursive payload
    xml_spec_filename = "test_recursive_payload.xml"
    xml_load_file_obj = get_specified_xml_loadfile(TEST_XML_SPECIFICATION_FILEPATH, xml_spec_filename)
    with pytest.raises(ValueError):
        xml_load_file_obj.read_xml()

def test_validation_large_payload(get_specified_xml_loadfile):
    """
       Test validation against attempted "large" payload, ValueError should be raised for
       the test_size_payload.xml XML specification file.
    """
    # Get the XML specification file that has a recursive payload
    xml_spec_filename = "test_size_payload.xml"
    xml_load_file_obj = get_specified_xml_loadfile(TEST_XML_SPECIFICATION_FILEPATH, xml_spec_filename)
    with pytest.raises(ValueError):
        xml_load_file_obj.read_xml()


def test_validation_valid_xml(get_specified_xml_loadfile):
    """
       Test validation against a real-world, valid XML specification file.
       ValueError should be NOT be raised for
       the full_example.xml specification file which has been used on real data.
    """
    # Get the XML specification file that has a recursive payload

    xml_spec_filename = "full_example.xml"
    xml_load_file_obj = get_specified_xml_loadfile(TEST_XML_SPECIFICATION_FILEPATH, xml_spec_filename)
    try:
        xml_load_file_obj.read_xml()
    except ValueError:
        msg = f"Unexpected ValueError when validating {os.path.join(TEST_XML_SPECIFICATION_FILEPATH,xml_spec_filename)}"
        pytest.fail(msg)

