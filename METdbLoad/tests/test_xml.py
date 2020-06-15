#!/usr/bin/env python3
"""Test reading XML file."""

# pylint:disable=import-error
# imported modules exist

from read_load_xml import XmlLoadFile

XML_FILE = '/Users/venita.hagerty/metviewer/testloadv10fewp3.xml'
XML_LOADFILE = XmlLoadFile(XML_FILE)
XML_LOADFILE.read_xml()

def test_loadflags():
    """Read various flags from XML file."""
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

def test_loadgroup():
    """Read group and description from XML file."""
    assert XML_LOADFILE.group == "vhagerty"
    assert XML_LOADFILE.description == "v projects"

def test_connection():
    """Read connection tags from XML file."""
    assert XML_LOADFILE.connection['db_host'] == "192.168.0.42"
    assert XML_LOADFILE.connection['db_port'] == 3306
    assert XML_LOADFILE.connection['db_database'] == "mv_test_3"
    assert XML_LOADFILE.connection['db_user'] == "met_admin"
    assert XML_LOADFILE.connection['db_management_system'] == "mysql"

def test_insertsize():
    """Read insert_size from XML file."""
    assert XML_LOADFILE.insert_size == 1
