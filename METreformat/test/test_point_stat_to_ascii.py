import pytest
import os
import pandas as pd
import numpy as np
from METdataio.METdbLoad.ush.read_load_xml import XmlLoadFile
import METdataio.METdbLoad.ush.constants as cn
from METdataio.METdbLoad.ush.read_data_files import ReadDataFiles
from METdataio.METreformat.write_stat_ascii import WriteStatAscii


@pytest.fixture
def setup():
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

    return rdf_obj


def test_point_stat_FHO_consistency(setup):
    '''
           For the data frame for the FHO line type, verify that the number of rows of data is
           increased three-fold (from the original dataframe) since we are expanding the
           F_RATE, H_RATE, and O_RATE values into their own rows.

    '''

    # Subset the input dataframe to include only the FHO linetype
    stat_data = setup.stat_data
    fho_columns_to_use = np.arange(0, 29).tolist()
    linetype = cn.FHO
    fho_df = stat_data[stat_data['line_type'] == linetype].iloc[:, fho_columns_to_use]

    wsa = WriteStatAscii()
    reshaped_df = wsa.process_by_stat_linetype(linetype, stat_data)

    expected_num_rows = fho_df.shape[0]
    actual_num_rows = reshaped_df.shape[0]

    # Expect three times as many rows now that we've reshaped the dataframe (we took the F_RATE, H_RATE, and
    # O_RATE columns and put them under the stat_name and stat_value columns)
    assert actual_num_rows == 3 * expected_num_rows

    # same check, different method
    assert 3*fho_df.shape[0] == reshaped_df.shape[0]







