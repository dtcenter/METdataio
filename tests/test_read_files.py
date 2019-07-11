#!/usr/bin/env python3
"""Test reading XML file."""

import constants as CN

from read_load_xml import XmlLoadFile
from read_data_files import ReadDataFiles

# Read in the XML load file
XML_FILE = '/Users/venita.hagerty/metviewer/testloadv10few3.xml'

xml_loadfile = XmlLoadFile(XML_FILE)
xml_loadfile.read_xml()

# Read all of the data from the data files into a dataframe
file_data = ReadDataFiles()

# read in the data files, with options specified by XML flags
file_data.read_data(xml_loadfile.flags,
                    xml_loadfile.load_files,
                    xml_loadfile.line_types)

def test_counts():
    """Count parts of the files loaded in."""
    # number of files
    assert len(xml_loadfile.load_files) == 4
    # number of lines of data
    assert file_data.stat_data.shape[0] == 11198
    # number of line types
    assert file_data.stat_data.line_type.unique().size == 7