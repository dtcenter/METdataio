#!/usr/bin/env python3
"""Test reading data files."""

# pylint:disable=import-error
# imported modules exist

# import constants as CN

from METdataio.METdbLoad.ush.read_load_xml import XmlLoadFile
from METdataio.METdbLoad.ush.read_data_files import ReadDataFiles

# Read in the XML load file
XML_FILE = '/Users/venita.hagerty/metviewer/testloadv10fewp3.xml'

XML_LOADFILE = XmlLoadFile(XML_FILE)
XML_LOADFILE.read_xml()

# Read all of the data from the data files into a dataframe
FILE_DATA = ReadDataFiles()

# read in the data files, with options specified by XML flags
FILE_DATA.read_data(XML_LOADFILE.flags,
                    XML_LOADFILE.load_files,
                    XML_LOADFILE.line_types)

def test_counts():
    """Count parts of the files loaded in."""
    # number of files
    assert len(XML_LOADFILE.load_files) == 7
    # number of lines of data
    assert FILE_DATA.stat_data.shape[0] == 18106
    # number of line types
    assert FILE_DATA.stat_data.line_type.unique().size == 22
