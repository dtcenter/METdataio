import pytest
from typing import List
import numpy as np
import pandas as pd
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
           For the data frame for the FHO line type, verify that a value in the original data
           corresponds to the value identified with the same criteria in the newly reformatted
           dataframe.

    '''

    # Subset the input dataframe to include only the FHO linetype
    stat_data = setup.stat_data
    fho_columns_to_use = np.arange(0, 29).tolist()
    linetype = cn.FHO
    fho_df = stat_data[stat_data['line_type'] == linetype].iloc[:, fho_columns_to_use]
    # Add the stat columns header names for the CTC line type
    fho_columns: List[str] = cn.FHO_FULL_HEADER
    fho_df.columns: List[str] = fho_columns

    # get the value of the record corresponding to line_type FHO, total number of pairs=3962, obs_var=WIND,
    # obs_lev=Z10, and stat_name=F_RATE
    total = 3962
    obs_var = 'WIND'
    obs_level = 'Z10'
    fcst_thresh = '>=10.288'
    expected_df:pd.DataFrame = fho_df.loc[(fho_df['total'] == total) & (fho_df['obs_var'] == obs_var) &
                                    (fho_df['obs_lev'] == obs_level) &
                                    (fho_df['fcst_thresh'] == fcst_thresh)]
    expected_row:pd.Series = expected_df.iloc[0]
    expected_name: str = "F_RATE"
    expected_val:float = expected_row.loc[expected_name]

    wsa = WriteStatAscii()
    reshaped_df = wsa.process_fho(stat_data)
    actual_df:pd.DataFrame = reshaped_df.loc[(reshaped_df['total'] == total) & (reshaped_df['obs_var'] == obs_var) &
                                           (reshaped_df['obs_lev'] == obs_level) &
                                           (reshaped_df['fcst_thresh'] == fcst_thresh) &
                                           (reshaped_df['stat_name'] == expected_name)]
    actual_row:pd.Series = actual_df.iloc[0]
    actual_value:float = actual_row['stat_value']

    actual_name:str = actual_row['stat_name']

    # Checking for consistency between the reformatted/reshaped data and the "original" data.
    assert expected_val == actual_value
    assert expected_name == actual_name

def test_point_stat_SL1L2_consistency(setup):
    '''
           For the data frame for the SL1L2 line type, verify that a value in the original data
           corresponds to the value identified with the same criteria in the newly reformatted
           dataframe.

    '''

    # Original data
    stat_data = setup.stat_data

    # Relevant columns for the CTC line type
    linetype: str = cn.SL1L2
    sl1l2_columns_to_use: List[str] = np.arange(0, cn.NUM_STAT_SL1L2_COLS).tolist()

    # Subset original dataframe to one containing only the CTC data
    sl1l2_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:, sl1l2_columns_to_use]

    # Add the stat columns header names for the CTC line type
    sl1l2_columns: List[str] = cn.SL1L2_HEADERS
    sl1l2_df.columns: List[str] = sl1l2_columns

    # get the value of the record corresponding to line_type Sl1L2, total number of pairs, obs_var,
    # obs_lev, and fcst_thresh, for the MAE statistic.
    total = 3965
    obs_var = 'RH'
    obs_level = 'Z2'
    fcst_thresh = 'NA'
    expected_df:pd.DataFrame = sl1l2_df.loc[(sl1l2_df['total'] == total) & (sl1l2_df['obs_var'] == obs_var) &
                                    (sl1l2_df['obs_lev'] == obs_level) &
                                    (sl1l2_df['fcst_thresh'] == fcst_thresh)]
    expected_row:pd.Series = expected_df.iloc[0]
    expected_name: str = "MAE"
    expected_val:float = expected_row.loc[expected_name]

    wsa = WriteStatAscii()
    reshaped_df = wsa.process_sl1l2(stat_data)
    actual_df:pd.DataFrame = reshaped_df.loc[(reshaped_df['total'] == total) & (reshaped_df['obs_var'] == obs_var) &
                                           (reshaped_df['obs_lev'] == obs_level) &
                                           (reshaped_df['fcst_thresh'] == fcst_thresh) &
                                           (reshaped_df['stat_name'] == expected_name)]
    actual_row:pd.Series = actual_df.iloc[0]
    actual_value:float = actual_row['stat_value']

    actual_name:str = actual_row['stat_name']

    # Checking for consistency between the reformatted/reshaped data and the "original" data.
    assert expected_val == actual_value


def test_point_stat_CTC_consistency(setup):
    '''
           For the data frame for the CTC line type, verify that a value in the original data
           corresponds to the value identified with the same criteria in the newly reformatted
           dataframe.

    '''

    # Original data
    stat_data = setup.stat_data

    # Relevant columns for the CTC line type
    linetype: str = cn.CTC
    ctc_columns_to_use: List[str] = np.arange(0, cn.NUM_STAT_CTC_COLS).tolist()

    # Subset original dataframe to one containing only the CTC data
    ctc_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:, ctc_columns_to_use]

    # Add the stat columns header names for the CTC line type
    ctc_columns: List[str] = cn.CTC_HEADERS
    ctc_df.columns: List[str] = ctc_columns

    # get the value of the record corresponding to line_type CTC, total number of pairs=3956, obs_var=CEILING,
    # obs_lev=L0, and fcst_thresh<3040, for the FN_ON statistic.
    total = 3956
    obs_var = 'CEILING'
    obs_level = 'L0'
    fcst_thresh = '<3040'
    expected_df:pd.DataFrame = ctc_df.loc[(ctc_df['total'] == total) & (ctc_df['obs_var'] == obs_var) &
                                    (ctc_df['obs_lev'] == obs_level) &
                                    (ctc_df['fcst_thresh'] == fcst_thresh)]
    expected_row:pd.Series = expected_df.iloc[0]
    expected_name: str = "FN_ON"
    expected_val:float = expected_row.loc[expected_name]

    wsa = WriteStatAscii()
    reshaped_df = wsa.process_ctc(stat_data)
    actual_df:pd.DataFrame = reshaped_df.loc[(reshaped_df['total'] == total) & (reshaped_df['obs_var'] == obs_var) &
                                           (reshaped_df['obs_lev'] == obs_level) &
                                           (reshaped_df['fcst_thresh'] == fcst_thresh) &
                                           (reshaped_df['stat_name'] == expected_name)]
    actual_row:pd.Series = actual_df.iloc[0]
    actual_value:float = actual_row['stat_value']

    actual_name:str = actual_row['stat_name']

    # Checking for consistency between the reformatted/reshaped data and the "original" data.
    assert expected_val == actual_value








