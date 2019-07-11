#!/usr/bin/env python3
"""Test reading XML file."""

from lxml import etree

from read_load_xml import XmlLoadFile

XML_FILE = '/Users/venita.hagerty/metviewer/testloadv10few3.xml'
xml_loadfile = XmlLoadFile(XML_FILE)
xml_loadfile.read_xml()

def test_loadflags():
    """Read various flags from XML file."""
    assert xml_loadfile.flags['load_stat']
    assert not xml_loadfile.flags['load_mode']
    assert xml_loadfile.flags['load_mtd']
    assert xml_loadfile.flags['load_mpr']
    assert xml_loadfile.flags['load_orank']
    assert xml_loadfile.flags['verbose']
    assert not xml_loadfile.flags['drop_indexes']
    assert not xml_loadfile.flags['apply_indexes']
    assert xml_loadfile.flags['stat_header_db_check']
    assert not xml_loadfile.flags['mode_header_db_check']
    assert not xml_loadfile.flags['mtd_header_db_check']
    assert not xml_loadfile.flags['force_dup_file']
    assert xml_loadfile.flags['load_xml']

def test_loadgroup():
    """Read group and description from XML file."""
    assert xml_loadfile.group == "vhagerty"
    assert xml_loadfile.description == "v projects"

def test_connection():
    """Read connection tags from XML file."""
    assert xml_loadfile.connection['db_host'] == "137.75.129.120"
    assert xml_loadfile.connection['db_port'] == 3312
    assert xml_loadfile.connection['db_name'] == "mv_test_3"
    assert xml_loadfile.connection['db_user'] == "met_admin"
    assert xml_loadfile.connection['db_management_system'] == "mysql"

def test_insertsize():
    """Read insert_size from XML file."""
    assert xml_loadfile.insert_size == 1


