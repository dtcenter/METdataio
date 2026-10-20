#!/usr/bin/env python3

import os
import pytest
from unittest.mock import MagicMock

import METdbLoad.test.utils as utils

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
    assert XML_LOADFILE.connection['db_host'] == utils.TEST_DB_HOST
    assert XML_LOADFILE.connection['db_port'] == utils.TEST_DB_PORT
    assert XML_LOADFILE.connection['db_database'] == utils.TEST_DB_NAME
    assert XML_LOADFILE.connection['db_user'] == utils.TEST_DB_USER
    assert XML_LOADFILE.connection['db_management_system'] == "mysql"

def test_insertsize(tmp_path, get_xml_loadfile):
    """Read insert_size from XML file."""
    XML_LOADFILE = get_xml_loadfile(tmp_path)
    assert XML_LOADFILE.insert_size == 1

# @pytest.mark.skip
def test_validation_recursive_payload_fields(get_specified_xml_loadfile):
    """
       Test validation against attempted recursive payload, ValueError should be raised for
       the test_recursive_payload_fields.xml XML-specification file because the max allowed number
       of field elements is 5 and the test config file has 6.
    """
    # Get the XML specification file that has a recursive payload
    xml_spec_filename = "test_recursive_payload_fields.xml"
    xml_load_file_obj = get_specified_xml_loadfile(TEST_XML_SPECIFICATION_FILEPATH, xml_spec_filename)
    with pytest.raises(ValueError):
        xml_load_file_obj.read_xml()


def test_validation_recursive_payload_vals(get_specified_xml_loadfile):
    """
       Test validation against attempted recursive payload, ValueError should be raised for
       the test_recursive_payload.xml XML-specification file.
    """
    # Get the XML specification file that has a recursive payload
    xml_spec_filename = "test_recursive_payload_vals.xml"
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


def test_validation_simple_xml(get_specified_xml_loadfile):
    """
       Test validation against a simple, valid XML specification file.
       ValueError should be NOT be raised for
       the full_example.xml specification file which has been used on real data.
    """

    xml_spec_filename = "test_load_specification.xml"
    xml_load_file_obj = get_specified_xml_loadfile(TEST_XML_SPECIFICATION_FILEPATH, xml_spec_filename)
    try:
        xml_load_file_obj.read_xml()
    except ValueError:
        msg = f"Unexpected ValueError when validating {os.path.join(TEST_XML_SPECIFICATION_FILEPATH,xml_spec_filename)}"
        pytest.fail(msg)

@pytest.mark.parametrize("xmlconfig", ["modified_example.xml", "full_example.xml"])
def test_validation_real_xml(get_specified_xml_loadfile, xmlconfig):
    """
       Test validation against an XML specification file that is in use by a project.
       ValueError should be NOT be raised for
       the specification file that has been used on real data.
    """

    xml_spec_filename = xmlconfig
    xml_load_file_obj = get_specified_xml_loadfile(TEST_XML_SPECIFICATION_FILEPATH, xml_spec_filename)
    try:
        xml_load_file_obj.read_xml()
    except ValueError:
        msg = f"Unexpected ValueError when validating {os.path.join(TEST_XML_SPECIFICATION_FILEPATH,xml_spec_filename)}"
        pytest.fail(msg)

def test_tmp_xml(get_specified_xml_loadfile):
    """
       Test validation against an XML specification file that was created as a fixture.
       ValueError should be NOT be raised for
       the full_example.xml specification file which has been used on real data.
    """
    # Get the XML specification file that has a recursive payload

    xml_spec_filename = "tmp.xml"
    xml_load_file_obj = get_specified_xml_loadfile(TEST_XML_SPECIFICATION_FILEPATH, xml_spec_filename)
    try:
        xml_load_file_obj.read_xml()
    except ValueError:
        msg = (f"Unexpected ValueError when validating {os.path.join(TEST_XML_SPECIFICATION_FILEPATH,xml_spec_filename)} against "
               f"schema {utils.LOAD_SPECIFICATION_SCHEMA}")
        pytest.fail(msg)

@pytest.mark.parametrize("xml_config", ["load_met_gha_prod.xml", "load_met_gha_new.xml", "example_load_specific_files.xml"])
def test_db_xml(get_specified_xml_loadfile, xml_config):
    """
       Test validation against XML specification files used in another test that compares two
       databases and an XML specification file that explicitly lists specific files to load.
       ValueError should be NOT be raised for
       the load_met_gha_prod/new specification files which are used on real data.
    """

    xml_spec_filename = xml_config
    xml_load_file_obj = get_specified_xml_loadfile(TEST_XML_SPECIFICATION_FILEPATH, xml_spec_filename)
    try:
        xml_load_file_obj.read_xml()
    except ValueError:
        msg = (f"Unexpected ValueError when validating {os.path.join(TEST_XML_SPECIFICATION_FILEPATH,xml_spec_filename)} against "
               f"schema {utils.LOAD_SPECIFICATION_SCHEMA}")
        pytest.fail(msg)

def test_line_type(get_specified_xml_loadfile):
    """
       Test validation against an XML specification file.
       ValueError should be NOT be raised for
       the test_line_type.xml specification file.
    """
    # Get the XML specification file that has a line_type following the newly updated
    # schema to match the code in the METdbLoad read_load_xml.py module.

    xml_spec_filename = "test_line_type.xml"
    xml_load_file_obj = get_specified_xml_loadfile(TEST_XML_SPECIFICATION_FILEPATH, xml_spec_filename)
    try:
        xml_load_file_obj.read_xml()
    except ValueError:
        msg = (f"Unexpected ValueError when validating {os.path.join(TEST_XML_SPECIFICATION_FILEPATH,xml_spec_filename)} against "
               f"schema {utils.LOAD_SPECIFICATION_SCHEMA}")
        pytest.fail(msg)

def test_bad_line_type(get_specified_xml_loadfile):
    """
       Test validation against an XML specification file that incorrectly has the line_type as a child of
       the load_val element.
       ValueError SHOULD be raised for
       the test_line_type.xml specification file.
    """
    # Get the XML specification file that has a line_type following the newly updated
    # schema to match the code in the METdbLoad read_load_xml.py module.

    xml_spec_filename = "test_bad_line_type.xml"
    xml_load_file_obj = get_specified_xml_loadfile(TEST_XML_SPECIFICATION_FILEPATH, xml_spec_filename)

    with pytest.raises(ValueError):
        xml_load_file_obj.read_xml()





def connection_xml(tags):
    """Return the root of a load_spec with a connection tag containing tags"""
    from lxml import etree

    return etree.fromstring(f"<load_spec><connection>{tags}</connection></load_spec>")


def test_connection_default_port():
    """The default MySQL port is used when the host has no port"""
    from METdbLoad.ush.read_load_xml import XmlLoadFile

    xml_loadfile = XmlLoadFile(None)
    xml_loadfile.read_db_connect(
        connection_xml(
            "<host>dbhost</host><database>mv_test</database>"
            "<user>me</user><password>pw</password>"
        )
    )
    assert xml_loadfile.connection["db_host"] == "dbhost"
    assert xml_loadfile.connection["db_port"] == 3306
    assert xml_loadfile.connection["db_database"] == "mv_test"


@pytest.mark.parametrize(
    "tags",
    [
        # no host
        "<database>mv_test</database><user>me</user><password>pw</password>",
        # no database
        "<host>dbhost</host><user>me</user><password>pw</password>",
        # empty user
        "<host>dbhost</host><database>mv_test</database><user></user><password>pw</password>",
    ],
)
def test_connection_missing_tag(tags):
    """A missing host, database or user tag is an error"""
    from METdbLoad.ush.read_load_xml import XmlLoadFile

    with pytest.raises(SystemExit, match="connection tag"):
        XmlLoadFile(None).read_db_connect(connection_xml(tags))


def test_xml_file_not_found(tmp_path):
    from METdbLoad.ush.read_load_xml import XmlLoadFile

    with pytest.raises(SystemExit, match="can not be found"):
        XmlLoadFile(str(tmp_path / "missing.xml")).read_xml()


def test_filenames_from_date():
    """A date_list is expanded to every date from start to end"""
    from METdbLoad.ush.read_load_xml import XmlLoadFile

    dates = XmlLoadFile(None).filenames_from_date(
        {"format": "yyyyMMddHH", "start": "2019061500", "end": "2019061512", "inc": "21600"}
    )
    assert dates == ["2019061500", "2019061506", "2019061512"]


def test_filenames_from_date_bad_format():
    """A date format with unknown characters gives no dates"""
    from METdbLoad.ush.read_load_xml import XmlLoadFile

    mock_logger = MagicMock()
    dates = XmlLoadFile(None, logger=mock_logger).filenames_from_date(
        {"format": "yyyy-MM-dd", "start": "2019-06-15", "end": "2019-06-16", "inc": "86400"}
    )
    assert dates == []
    mock_logger.error.assert_called_with("*** date_list tag has unknown characters ***")


def test_filenames_from_date_bad_date():
    """A start date that does not match the format is an error"""
    from METdbLoad.ush.read_load_xml import XmlLoadFile

    with pytest.raises(SystemExit, match="Value Error"):
        XmlLoadFile(None).filenames_from_date(
            {"format": "yyyyMMdd", "start": "June 15", "end": "20190616", "inc": "86400"}
        )


def test_filenames_from_template(tmp_path):
    """Every combination of template values is filled in, and files in existing
    directories are listed"""
    from METdbLoad.ush.read_load_xml import XmlLoadFile

    for model, date in (("gfs", "20190615"), ("gfs", "20190616"), ("nam", "20190615")):
        file_dir = tmp_path / model / date
        file_dir.mkdir(parents=True)
        (file_dir / "b.stat").touch()
        (file_dir / "a.stat").touch()

    files = XmlLoadFile(None).filenames_from_template(
        str(tmp_path / "{model}" / "{date}"),
        # unused values are ignored
        {"model": ["gfs", "nam"], "date": ["20190615", "20190616"], "unused": ["x"]},
    )
    # nam/20190616 does not exist
    assert files == [
        str(tmp_path / model / date / name)
        for model, date in (("gfs", "20190615"), ("gfs", "20190616"), ("nam", "20190615"))
        for name in ("a.stat", "b.stat")
    ]


@pytest.mark.parametrize(
    "template, fills",
    [
        ("/data/{model/{date}", {"model": ["gfs"], "date": ["20190615"]}),
        ("/data/{model}/{date}", {"model": ["gfs"]}),
    ],
)
def test_filenames_from_template_errors(template, fills):
    """Mismatched braces or a missing template value is an error"""
    from METdbLoad.ush.read_load_xml import XmlLoadFile

    with pytest.raises(SystemExit, match="Value Error"):
        XmlLoadFile(None).filenames_from_template(template, fills)
