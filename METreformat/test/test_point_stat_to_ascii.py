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
    # Read in the XML load file. This contains information about which MET output
    # files are to be loaded.
    xml_file = './point_stat.xml'

    xml_loadfile_obj = XmlLoadFile(xml_file)
    xml_loadfile_obj.read_xml()

    # Read all of the data from the data files into a dataframe
    rdf_obj = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    rdf_obj.read_data(xml_loadfile_obj.flags,
                      xml_loadfile_obj.load_files,
                      xml_loadfile_obj.line_types)

    return rdf_obj


@pytest.fixture
def setup_mctc_mcts():
    # Read in the XML load file. This contains information about which MET output files
    # are to be loaded (that contain the mctc and mcts line types).
    xml_file = './mctc_mcts.xml'

    xml_loadfile_obj = XmlLoadFile(xml_file)
    xml_loadfile_obj.read_xml()

    # Read all of the data from the data files into a dataframe
    rdf_obj = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    rdf_obj.read_data(xml_loadfile_obj.flags,
                      xml_loadfile_obj.load_files,
                      xml_loadfile_obj.line_types)

    return rdf_obj


@pytest.fixture
def setup_eclv_mpr_pc_vl1l2_vcnt():
    # Read in the XML load file. This contains information about which MET output files
    # are to be loaded (that contain the  line types).
    xml_file = './eclv_mpr_pct_vl1l2_vcnt.xml'

    xml_loadfile_obj = XmlLoadFile(xml_file)
    xml_loadfile_obj.read_xml()

    # Read all of the data from the data files into a dataframe
    rdf_obj = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    rdf_obj.read_data(xml_loadfile_obj.flags,
                      xml_loadfile_obj.load_files,
                      xml_loadfile_obj.line_types)

    return rdf_obj


@pytest.fixture
def setup_ens_stat_ecnt():
    # Read in the XML load file. This contains information about which MET output files
    # are to be loaded (that contain the  line types).
    xml_file = './ens_stat_ecnt.xml'

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
           For the data frame for the FHO line type, verify that a value in the
           original data
           corresponds to the value identified with the same criteria in the newly
           reformatted
           dataframe.

    '''

    # Subset the input dataframe to include only the FHO linetype
    stat_data = setup.stat_data
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

    wsa = WriteStatAscii()
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
def test_point_stat_SL1L2_consistency(setup):
    '''
           For the data frame for the SL1L2 line type, verify that a value in the
           original data
           corresponds to the value identified with the same criteria in the newly
           reformatted
           dataframe.

    '''

    # Original data
    stat_data = setup.stat_data

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

    wsa = WriteStatAscii()
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


def test_point_stat_VSL1L2_consistency(setup_eclv_mpr_pc_vl1l2_vcnt):
    '''
           For the data frame for the VL1L2 line type, verify that a value in the
           original data corresponds to the value identified with the same criteria
           in the newly reformatted dataframe.

    '''

    # Original data
    stat_data = setup_eclv_mpr_pc_vl1l2_vcnt.stat_data

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

    wsa = WriteStatAscii()
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


def test_point_stat_CTC_consistency(setup):
    '''
           For the data frame for the CTC line type, verify that a value in the
           original data
           corresponds to the value identified with the same criteria in the newly
           reformatted
           dataframe.

    '''

    # Original data
    stat_data = setup.stat_data

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

    wsa = WriteStatAscii()
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


def test_point_stat_CTS_consistency(setup):
    '''
           For the data frame for the CTS line type, verify that a value in the
           original data
           corresponds to the value identified with the same criteria in the newly
           reformatted
           dataframe.

    '''

    # Original data
    stat_data = setup.stat_data

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

    wsa = WriteStatAscii()
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


def test_point_stat_CNT_consistency(setup):
    '''
           For the data frame for the CNT line type, verify that a value in the
           original data
           corresponds to the value identified with the same criteria in the newly
           reformatted
           dataframe.

    '''

    # Original data
    stat_data = setup.stat_data

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

    wsa = WriteStatAscii()
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


@pytest.mark.skip()
def test_point_stat_MCTC_consistency(setup_mctc_mcts):
    '''
           For the data frame for the MCTC line type, verify that a value in the
           original data corresponds to the value identified with the same
           criteria in the newly reformatted dataframe.

    '''

    # Original data
    stat_data = setup_mctc_mcts.stat_data

    # Relevant columns for the MCTC line type
    linetype: str = cn.MCTC
    mctc_columns_to_use: List[str] = np.arange(0, cn.NUM_STAT_MCTC_COLS).tolist()

    # Subset original dataframe to one containing only the MCTC data
    mctc_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                            mctc_columns_to_use]

    # Add the stat columns header names for the MCTC line type
    mctc_columns: List[str] = cn.MCTC_HEADERS
    mctc_df.columns: List[str] = mctc_columns

    # get the value of the record corresponding to line_type MCTC, total number of
    # pairs=213840, ,
    # fcst_lev='0,0,*,*', and fcst_thresh'>=0.0,>=0.15,>=0.310',
    # for the N_CAT statistic.
    total = str(213840)
    obs_var = 'edr'
    fcst_lev = '0,0,*,*'
    fcst_thresh = '>=0.0,>=0.15,>=0.31'
    expected_df: pd.DataFrame = mctc_df.loc[(mctc_df['total'] == total) &
                                            (mctc_df['obs_var'] == obs_var) &
                                            (mctc_df['fcst_lev'] == fcst_lev) &
                                            (mctc_df['fcst_thresh'] == fcst_thresh)]
    expected_row: pd.Series = expected_df.iloc[0]

    wsa = WriteStatAscii()
    reshaped_df = wsa.process_mctc(stat_data)
    actual_df: pd.DataFrame = reshaped_df.loc[(reshaped_df['total'] == total) &
                                              (reshaped_df['obs_var'] == obs_var) &
                                              (reshaped_df['fcst_lev'] == fcst_lev) &
                                              (reshaped_df[
                                                   'fcst_thresh'] == fcst_thresh)
                                              ]
    actual_row: pd.Series = actual_df.iloc[0]
    actual_value: str = actual_row['stat_value']

    actual_name: str = actual_row['stat_name']

    # Checking for consistency between the reformatted/reshaped data
    # and the "original" data. Make sure we correctly captured the
    # statistic into stat_name and its corresponding value into stat_value.
    expected_name: str = "N_CAT"
    expected_val: str = expected_row.loc[expected_name]
    assert expected_val == actual_value

    # Check for any nan values in the dataframe
    assert reshaped_df.isnull().values.any() == False


def test_point_stat_MCTS_consistency(setup_mctc_mcts):
    '''
           For the data frame for the MCTS line type, verify that a value in the
           original data corresponds to the value identified with the same criteria
           in the newly reformatted dataframe.

    '''

    # Original data
    stat_data = setup_mctc_mcts.stat_data

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

    wsa = WriteStatAscii()
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


def test_ensemble_stat_ecnt_consistency(setup_ens_stat_ecnt):
    '''
           For the data frame for the ECNT line type, verify that a value in the
           original data corresponds to the value identified with the same criteria
           in the newly reformatted dataframe.

    '''

    # Original data
    stat_data = setup_ens_stat_ecnt.stat_data

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
    fcst_thresh = 'NA'
    expected_df: pd.DataFrame = ecnt_df.loc[(ecnt_df['total'] == total) & (ecnt_df[
                                                             'obs_var'] == obs_var) &
                                            (ecnt_df['obs_lev'] == obs_level) &
                                            (ecnt_df['fcst_thresh'] == fcst_thresh)]
    expected_row: pd.Series = expected_df.iloc[0]
    expected_name: str = "ME"
    expected_val: float = expected_row.loc[expected_name]

    wsa = WriteStatAscii()
    reshaped_df = wsa.process_ecnt(stat_data)
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
    # assert reshaped_df.isnull().values.any() == False
