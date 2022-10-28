#!/usr/bin/env python3
"""Test reading point stat file."""

# pylint:disable=import-error
# imported modules exist

from METdbLoad.ush.read_load_xml import XmlLoadFile
from METdbLoad.ush.read_data_files import ReadDataFiles
from METreformat.write_stat_ascii import WriteStatAscii

# Read in the XML load file. This contains information about which MET output files are to be loaded.
xml_file = '/Volumes/d1/minnawin/feature_121_met_reformatter/METdataio/METreformat/point_stat.xml'

xml_loadfile_obj = XmlLoadFile(xml_file)
xml_loadfile_obj.read_xml()

# Read all of the data from the data files into a dataframe
rdf_obj = ReadDataFiles()

# read in the data files, with options specified by XML flags
rdf_obj.read_data(xml_loadfile_obj.flags,
                    xml_loadfile_obj.load_files,
                    xml_loadfile_obj.line_types)

# Write stat file in ASCII format, one for each line type?
stat_lines_obj = WriteStatAscii()

stat_lines_obj.write_stat_ascii(xml_loadfile_obj.flags,
                           rdf_obj.stat_data)


