#!/usr/bin/env python3
"""Test reading data files."""

import pytest

from METdataio.METdbLoad.ush.read_data_files import ReadDataFiles


def test_counts(get_xml_loadfile):
    """Count parts of the files loaded in."""
    XML_LOADFILE = get_xml_loadfile()

    # Read all of the data from the data files into a dataframe
    FILE_DATA = ReadDataFiles()
    
    # read in the data files, with options specified by XML flags
    FILE_DATA.read_data(XML_LOADFILE.flags,
                        XML_LOADFILE.load_files,
                        XML_LOADFILE.line_types)

    # number of files
    assert len(XML_LOADFILE.load_files) == 1
    # number of lines of data
    assert FILE_DATA.stat_data.shape[0] == 6
    # number of line types
    assert FILE_DATA.stat_data.line_type.unique().size == 5
