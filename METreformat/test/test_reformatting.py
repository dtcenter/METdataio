import pytest
import pathlib
import os
import yaml
from collections import namedtuple
from typing import List
import numpy as np
import pandas as pd
from METdataio.METdbLoad.ush.read_load_xml import XmlLoadFile
import METdataio.METdbLoad.ush.constants as cn
from METdataio.METdbLoad.ush.read_data_files import ReadDataFiles
from METdataio.METreformat.write_stat_ascii import WriteStatAscii

def read_input(config_file):
    """
       Read in the input .stat data file, return a data frame representation of all the data in the specified
       input data directory.

    :param input_data_dir: The full path of the directory where the input data is located.
    :return: file_df, the dataframe representation of the input data
    """

    with open(config_file, 'r') as stream:
        try:
            parms: dict = yaml.load(stream, Loader=yaml.FullLoader)
            pathlib.Path(parms['output_dir']).mkdir(parents=True, exist_ok=True)
        except yaml.YAMLError as exc:
            print(exc)

    input_data_filename = parms['input_data_dir']
    input_data = os.path.join(os.path.dirname(__file__), input_data_filename)

    # Replacing the need for an XML specification file, pass in the XMLLoadFile and
    # ReadDataFile parameters
    rdf_obj: ReadDataFiles = ReadDataFiles()
    xml_loadfile_obj: XmlLoadFile = XmlLoadFile(None)

    # Retrieve all the filenames in the data_dir specified in the YAML config file
    load_files = xml_loadfile_obj.filenames_from_template(input_data, {})

    flags = xml_loadfile_obj.flags
    line_types = xml_loadfile_obj.line_types
    rdf_obj.read_data(flags, load_files, line_types)
    file_df = rdf_obj.stat_data

    # Check if the output file already exists, if so, delete it to avoid
    # appending output from subsequent runs into the same file.
    existing_output_file = os.path.join(parms['output_dir'], parms['output_filename'])
    if os.path.exists(existing_output_file):
        os.remove(existing_output_file)

    return file_df, parms

def setup_test(yaml_file):
    """
       Read in the YAML config settings, then generate the input data as a data frame and perform reformatting.

    """

    cwd = os.path.dirname(__file__)
    full_yaml_file = os.path.join(cwd, yaml_file)
    file_df, config = read_input(full_yaml_file)

    return file_df, config
def test_point_stat_FHO_consistency():
    '''
           For the data frame for the FHO line type, verify that a value in the
           original data
           corresponds to the value identified with the same criteria in the newly
           reformatted
           dataframe.

    '''

    # Subset the input dataframe to include only the FHO linetype
    stat_data, parms= setup_test("FHO.yaml")
    end = cn.NUM_STAT_FHO_COLS
    fho_columns_to_use = np.arange(0, end).tolist()
    linetype = cn.FHO
    fho_df = stat_data[stat_data['line_type'] == linetype].iloc[:, fho_columns_to_use]
    # Add the stat columns header names for the FHO line type
    fho_columns: List[str] = cn.FHO_FULL_HEADER
    fho_df.columns: List[str] = fho_columns

    # get the value of the record corresponding to line_type FHO, total number of
    # pairs=3962, obs_var=WIND,
    # obs_lev=Z10, and stat_name=F_RATE
    total = str(3962)
    obs_var = 'WIND'
    obs_level = 'Z10'
    fcst_thresh = '>=10.288'
    expected_df: pd.DataFrame = fho_df.loc[
        (fho_df['total'] == total) & (fho_df['obs_var'] == obs_var) &
        (fho_df['obs_lev'] == obs_level) &
        (fho_df['fcst_thresh'] == fcst_thresh)]
    expected_row: pd.Series = expected_df.iloc[0]
    expected_name: str = "F_RATE"
    expected_val: float = expected_row.loc[expected_name]

    wsa = WriteStatAscii(parms)
    reshaped_df = wsa.process_fho(stat_data)
    actual_df: pd.DataFrame = reshaped_df.loc[
        (reshaped_df['total'] == total) & (reshaped_df['obs_var'] == obs_var) &
        (reshaped_df['obs_lev'] == obs_level) &
        (reshaped_df['fcst_thresh'] == fcst_thresh) &
        (reshaped_df['stat_name'] == expected_name)]
    actual_row: pd.Series = actual_df.iloc[0]
    actual_value: float = actual_row['stat_value']

    actual_name: str = actual_row['stat_name']

    # Checking for consistency between the reformatted/reshaped data and the
    # "original" data.
    assert expected_val == actual_value
    assert expected_name == actual_name


# @pytest.mark.skip()
def test_point_stat_SL1L2_consistency():
    '''
           For the data frame for the SL1L2 line type, verify that a value in the
           original data
           corresponds to the value identified with the same criteria in the newly
           reformatted
           dataframe.

    '''

    # Original data
    stat_data, parms = setup_test('SL1L2.yaml')

    # Relevant columns for the SL1L2 line type
    linetype: str = cn.SL1L2
    sl1l2_columns_to_use: List[str] = np.arange(0, cn.NUM_STAT_SL1L2_COLS).tolist()

    # Subset original dataframe to one containing only the SL1L2 data
    sl1l2_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                             sl1l2_columns_to_use]

    # Add the stat columns header names for the SL1L2 line type
    sl1l2_columns: List[str] = cn.SL1L2_HEADERS
    sl1l2_df.columns: List[str] = sl1l2_columns

    # get the value of the record corresponding to line_type Sl1L2, total number of
    # pairs, obs_var,
    # obs_lev, and fcst_thresh, for the MAE statistic.
    total = str(3965)
    obs_var = 'RH'
    obs_level = 'Z2'
    fcst_thresh = 'NA'
    expected_df: pd.DataFrame = sl1l2_df.loc[
        (sl1l2_df['total'] == total) & (sl1l2_df['obs_var'] == obs_var) &
        (sl1l2_df['obs_lev'] == obs_level) &
        (sl1l2_df['fcst_thresh'] == fcst_thresh)]
    expected_row: pd.Series = expected_df.iloc[0]
    expected_name: str = "MAE"
    expected_val: float = expected_row.loc[expected_name]

    wsa = WriteStatAscii(parms)
    reshaped_df = wsa.process_sl1l2(stat_data)
    actual_df: pd.DataFrame = reshaped_df.loc[
        (reshaped_df['total'] == total) & (reshaped_df['obs_var'] == obs_var) &
        (reshaped_df['obs_lev'] == obs_level) &
        (reshaped_df['fcst_thresh'] == fcst_thresh) &
        (reshaped_df['stat_name'] == expected_name)]
    actual_row: pd.Series = actual_df.iloc[0]
    actual_value: float = actual_row['stat_value']

    actual_name: str = actual_row['stat_name']

    # Checking for consistency between the reformatted/reshaped data and the
    # "original" data.
    assert expected_val == actual_value

    # Check for any nan values in the dataframe
    assert reshaped_df.isnull().values.any() == False


def test_point_stat_VL1L2_consistency():
    '''
           For the data frame for the VL1L2 line type, verify that a value in the
           original data corresponds to the value identified with the same criteria
           in the newly reformatted dataframe.

    '''

    # Original data
    stat_data, parms = setup_test('VL1L2.yaml')

    # Relevant columns for the VL1L2 line type
    linetype: str = cn.VL1L2
    vl1l2_columns_to_use: List[str] = np.arange(0, cn.NUM_STAT_VL1L2_COLS).tolist()

    # Subset original dataframe to one containing only the VL1L2 data
    vl1l2_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                             vl1l2_columns_to_use]

    # Add the stat columns header names for the VL1L2 line type
    vl1l2_columns: List[str] = cn.VL1L2_HEADERS
    vl1l2_df.columns: List[str] = vl1l2_columns

    # get the value of the record corresponding to line_type Vl1L2, total number of
    # pairs, obs_var, obs_lev, and fcst_thresh, for the MAE statistic.
    total = str(934)
    interp = 'DW_MEAN_SQUARE'
    obs_level = 'Z10'
    fcst_thresh = 'NA'

    # Filter the original dataframe to a particular UFBAR row
    expected_df: pd.DataFrame = vl1l2_df.loc[(vl1l2_df['total'] == total) &
                                             (vl1l2_df['interp_mthd'] == interp) &
                                             (vl1l2_df['obs_lev'] == obs_level) &
                                             (vl1l2_df['fcst_thresh'] == fcst_thresh)]
    expected_row: pd.Series = expected_df.iloc[0]
    expected_name: str = "UFBAR"
    expected_val: float = expected_row.loc[expected_name]

    wsa = WriteStatAscii(parms)
    obs_var = 'UGRD_VGRD'
    reshaped_df = wsa.process_vl1l2(stat_data)
    actual_df: pd.DataFrame = reshaped_df.loc[(reshaped_df['total'] == total) &
                                              (reshaped_df['interp_mthd'] == interp) &
                                              (reshaped_df['obs_var'] == obs_var) &
                                              (reshaped_df['obs_lev'] == obs_level) &
                                              (reshaped_df[
                                                   'fcst_thresh'] == fcst_thresh) &
                                              (reshaped_df[
                                                   'stat_name'] == expected_name)]
    actual_row: pd.Series = actual_df.iloc[0]
    actual_value: float = actual_row['stat_value']

    actual_name: str = actual_row['stat_name']

    # Checking for consistency between the reformatted/reshaped data and the
    # "original" data.
    assert expected_val == actual_value

    # Check for any nan values in the dataframe
    assert reshaped_df.isnull().values.any() == False


def test_point_stat_CTC_consistency():
    '''
           For the data frame for the CTC line type, verify that a value in the
           original data
           corresponds to the value identified with the same criteria in the newly
           reformatted
           dataframe.

    '''

    # Original data
    stat_data, parms = setup_test('CTC.yaml')

    # Relevant columns for the CTC line type
    linetype: str = cn.CTC
    end = cn.NUM_STAT_CTC_COLS
    ctc_columns_to_use: List[str] = np.arange(0, end).tolist()

    # Subset original dataframe to one containing only the CTC data
    ctc_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                           ctc_columns_to_use]

    # Add the stat columns header names for the CTC line type
    ctc_columns: List[str] = cn.CTC_HEADERS
    ctc_df.columns: List[str] = ctc_columns

    # get the value of the record corresponding to line_type CTC, total number of
    # pairs=3956, obs_var=CEILING,
    # obs_lev=L0, and fcst_thresh<3040, for the FN_ON statistic.
    total = str(3956)
    obs_var = 'CEILING'
    obs_level = 'L0'
    fcst_thresh = '<3040'
    expected_df: pd.DataFrame = ctc_df.loc[
        (ctc_df['total'] == total) & (ctc_df['obs_var'] == obs_var) &
        (ctc_df['obs_lev'] == obs_level) &
        (ctc_df['fcst_thresh'] == fcst_thresh)]
    expected_row: pd.Series = expected_df.iloc[0]
    expected_name: str = "FN_ON"
    expected_val: float = expected_row.loc[expected_name]

    wsa = WriteStatAscii(parms)
    reshaped_df = wsa.process_ctc(stat_data)
    actual_df: pd.DataFrame = reshaped_df.loc[
        (reshaped_df['total'] == total) & (reshaped_df['obs_var'] == obs_var) &
        (reshaped_df['obs_lev'] == obs_level) &
        (reshaped_df['fcst_thresh'] == fcst_thresh) &
        (reshaped_df['stat_name'] == expected_name)]
    actual_row: pd.Series = actual_df.iloc[0]
    actual_value: float = actual_row['stat_value']

    actual_name: str = actual_row['stat_name']

    # Checking for consistency between the reformatted/reshaped data and the
    # "original" data.
    assert expected_val == actual_value

    # Check for any nan values in the dataframe
    assert reshaped_df.isnull().values.any() == False


def test_point_stat_CTS_consistency():
    '''
           For the data frame for the CTS line type, verify that a value in the
           original data
           corresponds to the value identified with the same criteria in the newly
           reformatted
           dataframe.

    '''

    # Original data
    stat_data, parms = setup_test('CTS.yaml')

    # Relevant columns for the CTS line type
    linetype: str = cn.CTS
    end = cn.NUM_STAT_CTS_COLS
    cts_columns_to_use: List[str] = np.arange(0, end).tolist()

    # Subset original dataframe to one containing only the CTS data
    cts_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                           cts_columns_to_use]

    # Add all the columns header names for the CTS line type
    cts_columns: List[str] = cn.CTS_SPECIFIC_HEADERS
    cts_df.columns: List[str] = cts_columns

    # get the value of the record corresponding to line_type CTS, total number of
    # pairs, obs_var,
    # obs_lev, and fcst_thresh, for the BASER statistic.
    total = str(3956)
    obs_var = 'CEILING'
    obs_level = 'L0'
    fcst_thresh = '<152'
    expected_df: pd.DataFrame = cts_df.loc[
        (cts_df['total'] == total) & (cts_df['obs_var'] == obs_var) &
        (cts_df['obs_lev'] == obs_level) &
        (cts_df['fcst_thresh'] == fcst_thresh)]
    expected_row: pd.Series = expected_df.iloc[0]
    expected_name: str = "BASER"
    expected_ncu: str = "BASER_NCU"
    expected_ncl: str = "BASER_NCL"
    expected_val: float = expected_row.loc[expected_name]
    expected_ncl: float = expected_row.loc[expected_ncl]
    expected_ncu: float = expected_row.loc[expected_ncu]

    wsa = WriteStatAscii(parms)
    reshaped_df = wsa.process_cts(stat_data)
    actual_df: pd.DataFrame = reshaped_df.loc[
        (reshaped_df['total'] == total) & (reshaped_df['obs_var'] == obs_var) &
        (reshaped_df['obs_lev'] == obs_level) &
        (reshaped_df['fcst_thresh'] == fcst_thresh) &
        (reshaped_df['stat_name'] == expected_name)]
    actual_row: pd.Series = actual_df.iloc[0]
    actual_value: float = actual_row['stat_value']
    actual_ncl: float = actual_row['stat_ncl']
    actual_ncu: float = actual_row['stat_ncu']

    # Checking for consistency between the reformatted/reshaped data and the
    # "original" data.
    assert expected_val == actual_value
    assert expected_ncl == actual_ncl
    assert expected_ncu == actual_ncu

    # Check for any nan values in the dataframe
    assert reshaped_df.isnull().values.any() == False


def test_point_stat_CNT_consistency():
    '''
           For the data frame for the CNT line type, verify that a value in the
           original data
           corresponds to the value identified with the same criteria in the newly
           reformatted
           dataframe.

    '''

    # Original data
    stat_data, parms = setup_test('CNT.yaml')

    # Relevant columns for the CNT line type
    linetype: str = cn.CNT
    end = cn.NUM_STAT_CNT_COLS
    cnt_columns_to_use: List[str] = np.arange(0, end).tolist()

    # Subset original dataframe to one containing only the CNT data
    cnt_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                           cnt_columns_to_use]

    # Add the stat columns for the CNT line type
    cnt_columns: List[str] = cn.FULL_CNT_HEADER
    cnt_df.columns: List[str] = cnt_columns

    # get the value of the record corresponding to line_type CNT, total number of
    # pairs, obs_var,
    # obs_lev, and fcst_thresh, for the ME statistic.
    total = str(4028)
    obs_var = 'TMP'
    obs_level = 'Z2'
    fcst_thresh = 'NA'
    expected_df: pd.DataFrame = cnt_df.loc[
        (cnt_df['total'] == total) & (cnt_df['obs_var'] == obs_var) &
        (cnt_df['obs_lev'] == obs_level) &
        (cnt_df['fcst_thresh'] == fcst_thresh)]
    expected_row: pd.Series = expected_df.iloc[0]
    expected_name: str = "ME"
    expected_ncu: str = "ME_NCU"
    expected_ncl: str = "ME_NCL"
    expected_val: float = expected_row.loc[expected_name]
    expected_ncl: float = expected_row.loc[expected_ncl]
    expected_ncu: float = expected_row.loc[expected_ncu]

    wsa = WriteStatAscii(parms)
    reshaped_df = wsa.process_cnt(stat_data)
    actual_df: pd.DataFrame = reshaped_df.loc[
        (reshaped_df['total'] == total) & (reshaped_df['obs_var'] == obs_var) &
        (reshaped_df['obs_lev'] == obs_level) &
        (reshaped_df['fcst_thresh'] == fcst_thresh) &
        (reshaped_df['stat_name'] == expected_name)]
    actual_row: pd.Series = actual_df.iloc[0]
    actual_value: float = actual_row['stat_value']
    actual_ncl: float = actual_row['stat_ncl']
    actual_ncu: float = actual_row['stat_ncu']

    # Checking for consistency between the reformatted/reshaped data and the
    # "original" data.
    assert expected_val == actual_value
    assert expected_ncl == actual_ncl
    assert expected_ncu == actual_ncu

    # Check for any nan values in the dataframe
    assert reshaped_df.isnull().values.any() == False

@pytest.mark.skip('Work in progress')
def test_point_stat_VCNT_consistency():
    '''
           For the data frame for the VCNT line type, verify that a value in the
           original data
           corresponds to the value identified with the same criteria in the newly
           reformatted
           dataframe.

    '''

    # Original data
    stat_data, parms = setup_test('VCNT.yaml')

    # Relevant columns for the VCNT line type
    linetype: str = cn.VCNT
    end = cn.NUM_STAT_VCNT_COLS
    vcnt_columns_to_use: List[str] = np.arange(0, end).tolist()

    # Subset original dataframe to one containing only the VCNT data
    vcnt_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                           vcnt_columns_to_use]

    # Add the stat columns for the VCNT line type
    vcnt_columns: List[str] = cn.FULL_VCNT_HEADER
    vcnt_df.columns: List[str] = vcnt_columns

    # get the value of the record corresponding to line_type VCNT, total number, obs_var,
    # obs_lev, and fcst_thresh, for the ME statistic.
    total = str(934)
    obs_var = 'UGRD_VGRD'
    obs_level = 'Z10'
    fcst_thresh = 'NA'
    expected_df: pd.DataFrame = vcnt_df.loc[
        (vcnt_df['total'] == total) & (vcnt_df['obs_var'] == obs_var) &
        (vcnt_df['obs_lev'] == obs_level) &
        (vcnt_df['fcst_thresh'] == fcst_thresh)]
    expected_row: pd.Series = expected_df.iloc[0]
    expected_name: str = "VCNT"
    wsa = WriteStatAscii(parms)
    reshaped_df = wsa.process_cnt(stat_data)
    actual_df: pd.DataFrame = reshaped_df.loc[
        (reshaped_df['total'] == total) & (reshaped_df['obs_var'] == obs_var) &
        (reshaped_df['obs_lev'] == obs_level) &
        (reshaped_df['fcst_thresh'] == fcst_thresh)]
    actual_row: pd.Series = actual_df.iloc[0]
    actual_value: float = actual_row['stat_value']
    expected_val: float = expected_row.loc[expected_name]

    # Checking for consistency between the reformatted/reshaped data and the
    # "original" data.
    assert expected_val == actual_value


    # Check for any nan values in the dataframe
    assert reshaped_df.isnull().values.any() == False


def test_point_stat_MCTS_consistency():
    '''
           For the data frame for the MCTS line type, verify that a value in the
           original data corresponds to the value identified with the same criteria
           in the newly reformatted dataframe.

    '''

    # Original data
    stat_data, parms = setup_test('./MCTS.yaml')

    # Relevant columns for the MCTS line type
    linetype: str = cn.MCTS
    end = cn.NUM_STAT_MCTS_COLS
    mcts_columns_to_use: List[str] = np.arange(0, end).tolist()

    # Subset original dataframe to one containing only the MCTS data
    mcts_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                            mcts_columns_to_use]

    # Add all the columns header names for the MCTS line type
    mcts_columns: List[str] = cn.MCTS_SPECIFIC_HEADERS
    mcts_df.columns: List[str] = mcts_columns

    # get the value of the record corresponding to line_type MCTS, total number of
    # pairs, obs_var,
    #
    total = str(213840)
    obs_var = 'edr'
    obs_level = '0,0,*,*'
    fcst_thresh = '>=0.0,>=0.15,>=0.31'
    expected_df: pd.DataFrame = mcts_df.loc[(mcts_df['total'] == total) & (mcts_df[
                                                                               'obs_var'] == obs_var) &
                                            (mcts_df['obs_lev'] == obs_level) &
                                            (mcts_df['fcst_thresh'] == fcst_thresh)]
    expected_row: pd.Series = expected_df.iloc[0]
    expected_name: str = "ACC"
    expected_ncu: str = "ACC_NCU"
    expected_ncl: str = "ACC_NCL"
    expected_val: float = expected_row.loc[expected_name]
    expected_ncl: float = expected_row.loc[expected_ncl]
    expected_ncu: float = expected_row.loc[expected_ncu]

    wsa = WriteStatAscii(parms)
    reshaped_df = wsa.process_mcts(stat_data)
    actual_df: pd.DataFrame = reshaped_df.loc[
        (reshaped_df['total'] == total) & (reshaped_df['obs_var'] == obs_var) &
        (reshaped_df['obs_lev'] == obs_level) &
        (reshaped_df['fcst_thresh'] == fcst_thresh) &
        (reshaped_df['stat_name'] == expected_name)]
    actual_row: pd.Series = actual_df.iloc[0]
    actual_value: float = actual_row['stat_value']
    actual_ncl: float = actual_row['stat_ncl']
    actual_ncu: float = actual_row['stat_ncu']

    # Checking for consistency between the reformatted/reshaped data and the
    # "original" data.
    assert expected_val == actual_value
    assert expected_ncl == actual_ncl
    assert expected_ncu == actual_ncu

    # Check for any nan values in the dataframe
    assert reshaped_df.isnull().values.any() == False


def test_ensemble_stat_ecnt_consistency():
    '''
           For the data frame for the ECNT line type, verify that a value in the
           original data corresponds to the value identified with the same criteria
           in the newly reformatted dataframe.

    '''

    # Original data
    stat_data, config = setup_test('ECNT.yaml')

    # Relevant columns for the ECNT line type
    linetype: str = cn.ECNT
    ecnt_columns_to_use: List[str] = np.arange(0, cn.NUM_STAT_ECNT_COLS).tolist()

    # Subset original dataframe to one containing only the ECNT data
    ecnt_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                            ecnt_columns_to_use]

    # Add the stat columns header names for the ECNT line type
    ecnt_columns: List[str] = cn.ECNT_HEADERS
    ecnt_df.columns: List[str] = ecnt_columns

    # get the value of the record corresponding to line_type ECNT, total number of
    # pairs, obs_var,
    # obs_lev, and fcst_thresh, for the MAE statistic.
    total = str(1125)
    obs_var = 'APCP_24'
    obs_level = 'A24'
    obs_lead = 0
    fcst_thresh = 'NA'
    vx_mask = 'FULL'
    interp_mthd = 'NEAREST'
    expected_df: pd.DataFrame = ecnt_df.loc[(ecnt_df['total'] == total) & (ecnt_df[
                                                             'obs_var'] == obs_var) &
                                            (ecnt_df['obs_lev'] == obs_level) &
                                            (ecnt_df['obs_lead'] == obs_lead)  &
                                            (ecnt_df['vx_mask'] == vx_mask) &
                                            (ecnt_df['interp_mthd'] == interp_mthd)]
    expected_row: pd.Series = expected_df.iloc[0]
    expected_name: str = "ME"
    expected_val: float = expected_row.loc[expected_name]
    me_val = float(0.97455)
    assert me_val == float(expected_val)

    wsa = WriteStatAscii(config)
    reshaped_df = wsa.process_ecnt(stat_data)
    actual_df: pd.DataFrame = reshaped_df.loc[
        (reshaped_df['total'] == total) & (reshaped_df['obs_var'] == obs_var) &
        (reshaped_df['obs_lev'] == obs_level) &
        (reshaped_df['obs_lead'] == obs_lead) &
        (reshaped_df['vx_mask'] == vx_mask) &
        (reshaped_df['fcst_thresh'] == fcst_thresh) &
        (reshaped_df['stat_name'] == expected_name)]
    actual_row: pd.Series = actual_df.iloc[0]
    actual_value: float = actual_row['stat_value']

    actual_name: str = actual_row['stat_name']

    # Checking for consistency between the reformatted/reshaped data and the
    # "original" data.
    assert expected_val == actual_value

    # Check for any all expected values in the stat_name column
    ecnt_stats = cn.ECNT_STATISTICS_HEADERS
    num_ecnt_headers = len(ecnt_stats)
    num_matches = 0
    stat_name_col: pd.Series = reshaped_df['stat_name'].unique()
    for stat in stat_name_col:
        if stat in ecnt_stats:
            num_matches += 1

    assert num_matches == num_ecnt_headers

    # Check for some expected values in the reformatted dataframe for the
    # row where VX_MASK = FULL and TOTAL=1125
    expected_rmse = 12.52909
    actual_rmse_df: pd.DataFrame = reshaped_df.loc[(reshaped_df['stat_name'] == 'RMSE') &
                                  (reshaped_df['vx_mask'] == 'FULL') &
                                  (reshaped_df['total'] == '1125')]
    print(f"RMSE from reformatted: {actual_rmse_df} of type {type(actual_rmse_df)}")
    assert str(actual_rmse_df.iloc[0].stat_value) == str(expected_rmse)


def test_pct_consistency():
    '''

    :return: None
    '''

    # Original data
    stat_data, config = setup_test('PCT_ROC.yaml')

    # Relevant columns for the PCT line type
    linetype: str = cn.PCT

    wsa = WriteStatAscii(config)
    reshaped_df = wsa.process_pct(stat_data)

    # Verify that the following values are found for the rows with these columns + values:
    # fcst_thresh ==0.10000
    # obs_thresh>=288
    # total=4307
    # fcst_lead 110000
    # fcst_lev=Z2
    ExpectedValues = namedtuple('ExpectedValues', 'thresh, oy, on, i_value')
    thresh = [0, 0.1, 0.2, 0.9]
    oy_i = [17, 7, 17, 1071]
    on_i = [2831, 71, 30, 24]
    i_value = [1, 2, 3, 10]
    expected_values = []
    zipped = zip(thresh, oy_i, on_i, i_value)
    zipped_list = list(zipped)
    for cur in zipped_list:
        expected_values.append(ExpectedValues(*cur))

    # Values from reformatting
    subset = reshaped_df.loc[(reshaped_df['fcst_thresh'] == '==0.10000') & (reshaped_df['obs_thresh'] == '>=288') &
                    (reshaped_df['total'] == '4307') & (reshaped_df['fcst_lev'] == 'Z2') &
                    (reshaped_df['fcst_lead'] == 110000) & (reshaped_df['obs_var'] == 'DPT')]

    values = [1, 2, 3, 10]
    for idx, val in enumerate(values):
        subset_by_value:pd.DataFrame = subset.loc[subset['i_value']== val]
        cur_thresh = list(subset_by_value['thresh_i'])[0]
        cur_oy = list(subset_by_value['oy_i'])[0]
        cur_on = list(subset_by_value['on_i'])[0]
        cur_val = list(subset_by_value['i_value'])[0]
        expected_thresh = expected_values[idx].thresh
        expected_oy = expected_values[idx].oy
        expected_on = expected_values[idx].on
        expected_val = expected_values[idx].i_value

        assert str(expected_thresh) == cur_thresh
        assert str(expected_oy) == cur_oy
        assert str(expected_on) == cur_on
        assert expected_val == cur_val


def test_rhist_consistency():
    '''

      Verify that values match what is expected in the reformatted output.

    :return:  None
    '''


    # Original data
    stat_data, config = setup_test('RHIST.yaml')

   # Relevant columns for the RHIST line type
    linetype: str = cn.RHIST

    wsa = WriteStatAscii(config)
    reshaped_df = wsa.process_rhist(stat_data)

    # Verify that the following values are found for the rows with these columns + values (corresponding to the
    # last row of data in the raw RHIST input dataframe):
    fcst_lead=280000
    obs_var='DPT'
    obtype='ADPSFC'
    model='RRFS_GEFS_GF.SPP.SPPT'
    fcst_var='DPT'
    total= '4089'
    fcst_lev='Z2'
    obs_lev='Z2'

    ExpectedValues = namedtuple('ExpectedValues', 'rank, i_value')
    rank_i = [955, 460, 389, 220]
    i_value = [1, 2, 3, 11]
    expected_values = []
    zipped = zip(rank_i, i_value)
    zipped_list = list(zipped)
    for cur in zipped_list:
        expected_values.append(ExpectedValues(*cur))

    # Values from reformatted stat file
    subset = reshaped_df.loc[(reshaped_df['fcst_lead'] == fcst_lead) & (reshaped_df['obs_var'] == obs_var) &
                    (reshaped_df['total'] == total) & (reshaped_df['fcst_lev'] == fcst_lev) &
                    (reshaped_df['fcst_lead'] == fcst_lead) & (reshaped_df['obs_var'] == obs_var) &
                    (reshaped_df['model'] == model) & (reshaped_df['obtype'] == obtype) &
                    (reshaped_df['fcst_var'] == fcst_var) & (reshaped_df['obs_lev'] == obs_lev)]

    # Check that our subsetted dataframe isn't an empty data frame and it contains n_rank rows
    num_ranks = reshaped_df['n_rank'][0]
    assert reshaped_df.shape[0] > 1
    assert subset.shape[0] == num_ranks

    values = [1, 2, 3, 11]
    for idx, val in enumerate(values):
        subset_by_value:pd.DataFrame = subset.loc[subset['i_value']== val]
        cur_rank = list(subset_by_value['rank_i'])[0]
        cur_val = list(subset_by_value['i_value'])[0]
        expected_rank = expected_values[idx].rank
        expected_val = expected_values[idx].i_value

        assert str(expected_rank) == cur_rank
        assert expected_val == cur_val


def test_ecnt_reformat_for_agg():
    '''

         Verify that values match what is expected in the reformatted output.

       :return:  None
       '''

    # Original reformatted data
    stat_data, config = setup_test('ECNT_for_agg.yaml')

    wsa = WriteStatAscii(config)
    reformatted_df = wsa.process_ecnt_for_agg(stat_data)

    ExpectedValues = namedtuple('ExpectedValues', 'total, n_ens, crps')

    total_vals = [1125, 503]
    n_ens_vals = [6.0, 6.0]
    crps_vals = [8.21904, 0.1367]
    expected_values = []
    zipped = zip(total_vals, n_ens_vals, crps_vals)
    zipped_list = list(zipped)
    for cur in zipped_list:
        expected_values.append(ExpectedValues(*cur))

    ref1:pd.Series = reformatted_df.loc[reformatted_df['total'] == str(expected_values[0].total)]
    ref2:pd.Series = reformatted_df.loc[reformatted_df['total'] == str(expected_values[1].total)]

    # Verify that we still have a row of ECNT linetype data with the original total values of 1125 and 503
    if ref1 is not None and ref2 is not None :
        # There are rows of data corresponding to the requested total values
        assert True
    else:
        assert False

    # Verify that the values for crps and n_ens are consistent with the original input data.
    assert float(ref1['crps'].iloc[0]) == expected_values[0].crps
    assert float(ref2['crps'].iloc[0]) == expected_values[1].crps
    assert ref1['n_ens'].iloc[0] == expected_values[0].n_ens
    assert ref2['n_ens'].iloc[0] == expected_values[1].n_ens


def test_fho_reformat_for_agg():
    '''
       Verify that the NotImplementedError is raised when attempting
       to invoke the process_fho_for_agg() within write_stat_ascii.

    :return:
    '''

    stat_data, parms = setup_test("FHO_for_agg.yaml")
    wsa = WriteStatAscii(parms)

    # Expect error when invoking the process_fho_for_agg directly
    with pytest.raises(NotImplementedError):
        reshaped_df = wsa.process_fho_for_agg(stat_data)

    # Expect SystemExit within the process_by_stat_line_type when a NotImplementedError is caught
    with pytest.raises(SystemExit):
        wsa.write_stat_ascii(stat_data, parms)