#!/usr/bin/env python3
"""Test reading data files."""

import pytest
import logging

from METdbLoad.ush import constants as CN
from METdbLoad.ush.read_data_files import ReadDataFiles
from METdbLoad.ush.read_load_xml import XmlLoadFile
from METdbLoad.test.utils import (
    POINT_STAT_DATA_DIR,
    EMPTY_DIR,
    ONE_EMPTY_DIR,
    VSDB_DIR,
    VSDB_NO_EQUALS_DIR,
    VSDB_EMPTY,
    VSDB_ONE_EMPTY,
    MODE_CTS_EMPTY,
    MODE_OBJ_EMPTY,
    MODE_NO_HEADER,
    MODE_ONLY_CTS,
    MODE_EMPTY,
    MTD_DATA_DIR,
    MTD_EMPTY,
    MTD_HEADER_NO_DATA,
    TCSTAT_DIR,
    TCSTAT_NO_HEADER,
    MTD_HEADER_NO_DATA,
    MTD_INTENSITY_90_LAST_COL,
    MTD_NO_FCST_T_BEG,
    MTD_ONE_EMPTY,
    MTD_MISSING_COLUMNS
)
import pandas as pd


from METdataio.METdbLoad.conftest import get_generic_xml_loadfile


def test_counts(tmp_path, get_xml_loadfile):
    """Count parts of the files loaded in."""
    XML_LOADFILE = get_xml_loadfile(tmp_path, POINT_STAT_DATA_DIR)

    # Read all of the data from the data files into a dataframe
    FILE_DATA = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    FILE_DATA.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )

    # number of files
    assert len(XML_LOADFILE.load_files) == 1
    # number of lines of data
    assert FILE_DATA.stat_data.shape[0] == 94
    # number of line types
    assert FILE_DATA.stat_data.line_type.unique().size == 7


def test_mtd_loads(tmp_path, get_xml_loadfile):
    XML_LOADFILE = get_xml_loadfile(tmp_path, MTD_DATA_DIR)

    # Read all of the data from the data files into a dataframe
    FILE_DATA = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    FILE_DATA.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )

    # number of files
    assert len(XML_LOADFILE.load_files) == 3
    # number of lines of data
    assert FILE_DATA.mtd_2d_data.shape == (278, 43)
    assert FILE_DATA.mtd_3d_single_data.shape == (8, 48)


def test_mtd_loads_revision(tmp_path, get_xml_loadfile):
    # Create a test MTD 2D revision file
    data = (
        """VERSION  MODEL  DESC  FCST_LEAD       FCST_VALID  OBS_LEAD        OBS_VALID  T_DELTA  FCST_T_BEG  FCST_T_END  FCST_RAD  FCST_THR  OBS_T_BEG  OBS_T_END  OBS_RAD  OBS_THR  FCST_VAR  FCST_UNITS  FCST_LEV  OBS_VAR  OBS_UNITS  OBS_LEV  OBJECT_ID  OBJECT_CAT  TIME_INDEX  AREA  CENTROID_X  CENTROID_Y  CENTROID_LAT  CENTROID_LON  AXIS_ANG  INTENSITY_10  INTENSITY_25  INTENSITY_50  INTENSITY_75  INTENSITY_90  INTENSITY_99\n"""
        """V12.0.0   FCST    NA     010000  20100517_010000    010000  20100517_010000   010000          -1           1         2     >=0.5         -1          1        2    >=0.5   APCP_01      kg/m^2       A01  APCP_01     kg/m^2      A01       F001       CF001           0  3640      420.52      167.55         35.53        -85.21      5.46          0.00          0.10          0.99          2.91          5.59         20.83\n"""
        """V12.0.0   FCST    NA     010000  20100517_010000    010000  20100517_010000   010000          -1           1         2     >=0.5         -1          1        2    >=0.5   APCP_01      kg/m^2       A01  APCP_01     kg/m^2      A01        new       CF002           0  3640      420.52      167.55         35.53        -85.21      5.46          0.00          0.99          0.99          2.99          5.99         99.00\n"""
        """V12.0.0   FCST    NA     010000  20100517_010000    010000  20100517_010000   010000          -1           1         2     >=0.5         -1          1        2    >=0.5   APCP_01      kg/m^2       A01  APCP_01     kg/m^2      A01        new       CF001           0  3640      420.52      167.55         35.53        -85.21      5.46          0.00          0.10          0.99          2.91          5.59         20.83\n"""
        """V12.0.0   FCST    NA     010000  20100517_010000    010000  20100517_010000   010000          -1           1         2     >=0.5         -1          1        2    >=0.5   APCP_01      kg/m^2       A01  APCP_01     kg/m^2      A01        new       CF002           0  3640      420.52      167.55         35.53        -85.21      5.46          0.00          0.99          0.99          2.99          5.99         99.00\n"""
        """V12.0.0   FCST    NA     010000  20100517_010000    010000  20100517_010000   010000          -1           1         2     >=0.5         -1          1        2    >=0.5   APCP_01      kg/m^2       A01  APCP_01     kg/m^2      A01        new       CF001           0  3640      420.52      167.55         35.53        -85.21      5.46          0.00          0.10          0.99          2.91          5.59         20.83\n"""
        """V12.0.0   FCST    NA     010000  20100517_010000    010000  20100517_010000   010000          -1           1         2     >=0.5         -1          1        2    >=0.5   APCP_01      kg/m^2       A01  APCP_01     kg/m^2      A01        new       CF002           0  3640      420.52      167.55         35.53        -85.21      5.46          0.00          0.99          0.99          2.99          5.99         99.00"""
    )
    tmp_mtd_dir = tmp_path / "mtd_revision"
    tmp_mtd_dir.mkdir()

    with open(tmp_mtd_dir / "mtd_REVISION_TEST_2d.txt", "w") as f:
        f.write(data)

    XML_LOADFILE = get_xml_loadfile(tmp_path, tmp_mtd_dir)
    FILE_DATA = ReadDataFiles()
    FILE_DATA.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )

    assert len(XML_LOADFILE.load_files) == 1
    assert FILE_DATA.mtd_2d_data.shape == (10, 43)
    assert FILE_DATA.mtd_3d_single_data.shape == (0, 0)

    # Check revision have been correctly labeled
    revs = FILE_DATA.mtd_2d_data["fcst_var"] == "REV_APCP_01"
    assert sum(revs) == 4
    revs = FILE_DATA.mtd_2d_data["obs_var"] == "REV_APCP_01"
    assert sum(revs) == 4


def test_read_data_logger():
    """
        verify expected behavior when logger is None and a valid logger

    """
    rdf = ReadDataFiles()

    # make sure a logger is created when no logger is supplied
    assert rdf.logger

    # make sure the input logger is being used
    logger_name = "bogus"
    logger = logging.Logger(name=logger_name)
    rdf_logger_given = ReadDataFiles(logger)
    assert rdf_logger_given.logger.name == logger_name


def test_read_data_no_valid_files():
    """
          test that when no valid files are available to load, a ValueError is raised

    """
    line_types = None
    load_flags = None
    load_files = None
    rdf = ReadDataFiles()

    # A ValueError is raised, but sys.exit is invoked, resulting
    # in a SystemExit in the exception block

    # load_files is None
    with pytest.raises(SystemExit):
        rdf.read_data(load_flags, load_files, line_types)

    # load_files is an empty list
    with pytest.raises(SystemExit):
        load_files = []
        rdf.read_data(load_flags, load_files, line_types)


def test_only_empty_data(tmp_path, get_xml_loadfile):
    """

      test that empty data file exercises the exception block that corresponds
      to an empty file, or file with no header yet continues and the resulting
      data frame is empty.

    """
    XML_LOADFILE = get_xml_loadfile(tmp_path, EMPTY_DIR)

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    rdf.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )

    # Empty stat_data data frame with no rows and no columns
    assert rdf.stat_data.shape[0] == 0
    assert rdf.stat_data.shape[1] == 0


def test_one_empty_data(tmp_path, get_xml_loadfile):
    """
      For point-stat data,
      test that when there is one empty data file and another that isn't empty,
      the empty data/empty header exceptions continue resulting in a final
      data frame with data.

    """
    XML_LOADFILE = get_xml_loadfile(tmp_path, ONE_EMPTY_DIR)

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    rdf.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )

    # Empty stat_data data frame with no rows and no columns
    assert rdf.stat_data.shape[0] > 0


@pytest.mark.parametrize("get_generic_xml_loadfile", ["vsdb"], indirect=True)
def test_empty_vsdb(tmp_path, get_generic_xml_loadfile):
    '''
       Verify that expected behavior is observed for empty VSDB data.

    '''

    XML_LOADFILE = get_generic_xml_loadfile(tmp_path, VSDB_EMPTY, 'vsdb')

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    rdf.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )

    assert rdf.stat_data.shape[0] == 0
    assert rdf.stat_data.shape[1] == 0


@pytest.mark.parametrize("get_generic_xml_loadfile", ["vsdb"], indirect=True)
def test_vsdb(tmp_path, get_generic_xml_loadfile):
    '''
       Verify that expected behavior is observed when the VSDB data contains one
       data file that is empty.

    '''
    XML_LOADFILE = get_generic_xml_loadfile(tmp_path, VSDB_ONE_EMPTY, "vsdb")

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    rdf.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )
    assert rdf.stat_data.shape[0] > 0


def test_vsdb_no_equals(tmp_path, get_xml_loadfile):
    '''
       Verify that expected behavior is observed when the VSDB data is not
       separated by '=' sign.

    '''
    XML_LOADFILE = get_xml_loadfile(tmp_path, VSDB_NO_EQUALS_DIR)

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    rdf.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )
    assert rdf.stat_data.shape[0] > 0


@pytest.mark.parametrize("get_generic_xml_loadfile", ['mode_cts'], indirect=True)
def test_empty_mode_cts(tmp_path, get_generic_xml_loadfile):
    '''
       Verify that expected behavior is observed when the mode cts data file is empty.

    '''
    XML_LOADFILE = get_generic_xml_loadfile(tmp_path, MODE_CTS_EMPTY, 'mode_cts')

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    rdf.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )

    assert rdf.stat_data.shape[0] == 0
    assert rdf.stat_data.shape[1] == 0


@pytest.mark.parametrize("get_generic_xml_loadfile", ['mode_cts'], indirect=True)
def test_empty_mode_no_header(tmp_path, get_generic_xml_loadfile):
    '''
       Verify that expected behavior is observed when the mode cts data file is
       missing a header

    '''

    XML_LOADFILE = get_generic_xml_loadfile(tmp_path, MODE_NO_HEADER, 'mode_cts')

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    with pytest.raises(SystemExit):
        rdf.read_data(
            XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
        )


    @pytest.mark.parametrize("get_generic_xml_loadfile", ['mode_cts', ' mode_obj'], indirect=True)
    def test_empty_mode_cts_mode_obj(tmp_path, get_generic_xml_loadfile):
        '''
           Verify that expected behavior is observed when the mode cts data file is empty.

        '''

        XML_LOADFILE = get_generic_xml_loadfile(tmp_path, MODE_EMPTY, 'mode_cts')

        # Read all of the data from the data files into a dataframe
        rdf = ReadDataFiles()

        # read in the data files, with options specified by XML flags
        rdf.read_data(
            XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
        )


    assert rdf.stat_data.shape[0] == 0
    assert rdf.stat_data.shape[1] == 0


@pytest.mark.parametrize("get_generic_xml_loadfile", ['mode_cts'], indirect=True)
def test_empty_mode_cts_valid_mod_obj(tmp_path, get_generic_xml_loadfile):
    '''
       Verify that expected behavior is observed when the mode cts data file is empty
       but there is a valid mode_obj file

    '''
    XML_LOADFILE = get_generic_xml_loadfile(tmp_path, MODE_CTS_EMPTY, 'mode_cts')

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    rdf.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )

    assert rdf.stat_data.shape[0] == 0
    assert rdf.stat_data.shape[1] == 0


@pytest.mark.parametrize("get_generic_xml_loadfile", ['mode_cts'], indirect=True)
def test_empty_mode_obj_valid_mod_cts(tmp_path, get_generic_xml_loadfile):
    '''
       Verify that expected behavior is observed when the mode obj data file is empty
       but there is a valid mode cts file

    '''
    XML_LOADFILE = get_generic_xml_loadfile(tmp_path, MODE_OBJ_EMPTY, 'mode_cts')

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    rdf.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )

    assert rdf.stat_data.shape[0] == 0
    assert rdf.stat_data.shape[1] == 0


@pytest.mark.parametrize("get_generic_xml_loadfile", ['mtd_2d'], indirect=True)
def test_mtd_2d(tmp_path, get_generic_xml_loadfile):
    '''
       Verify that MTD 2D data is correctly read in
    '''

    XML_LOADFILE = get_generic_xml_loadfile(tmp_path, MTD_DATA_DIR, 'mtd_2d')

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    rdf.read_data(XML_LOADFILE.flags, XML_LOADFILE.load_files,
                  XML_LOADFILE.line_types)

    assert rdf.mtd_2d_data.shape[0] > 0
    assert rdf.mtd_2d_data.shape[1] > 0


@pytest.mark.parametrize("get_generic_xml_loadfile", ['mtd_2d'], indirect=True)
def test_empty_mtd(tmp_path, get_generic_xml_loadfile):
    '''
       Verify that expected behavior is observed when there is only one
        mtd file and that mtd file is empty: an empty dataframe

    '''
    XML_LOADFILE = get_generic_xml_loadfile(tmp_path, MTD_EMPTY, 'mtd_2d')

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()
    rdf.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )
    assert rdf.mtd_2d_data.shape[0] == 0


@pytest.mark.parametrize("get_generic_xml_loadfile", ['mtd_2d'], indirect=True)
def test_one_empty_mtd(tmp_path, get_generic_xml_loadfile):
    '''
       Verify that expected behavior is observed when one of the 2D mtd files
        is empty:  a dataframe with the data from the non-empty mtd file

    '''
    XML_LOADFILE = get_generic_xml_loadfile(tmp_path, MTD_ONE_EMPTY, 'mtd_2d')

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()
    rdf.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )
    assert rdf.mtd_2d_data.shape[0] > 0


@pytest.mark.parametrize("get_generic_xml_loadfile", ['mtd_2d'], indirect=True)
def test_mtd_header_no_data(tmp_path, get_generic_xml_loadfile):
    '''
       Verify that expected behavior is observed when the mtd file has data
       but no header.

    '''
    XML_LOADFILE = get_generic_xml_loadfile(tmp_path, MTD_HEADER_NO_DATA, 'mtd_2d')

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()
    rdf.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )

    # Should produce an empty dataframe
    assert rdf.stat_data.shape[0] == 0
    assert rdf.stat_data.shape[1] == 0


@pytest.mark.parametrize("get_generic_xml_loadfile", ['mtd_2d'], indirect=True)
def test_mtd_no_fcst_t_beg(tmp_path, get_generic_xml_loadfile):
    '''
       Verify that expected behavior is observed when the mtd file does not
       have the fcst_t_beg column.  The fcst_t_beg column should be added
       with the CN.MV_NULL value.

    '''
    XML_LOADFILE = get_generic_xml_loadfile(tmp_path, MTD_NO_FCST_T_BEG, 'mtd_2d')

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()
    rdf.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )

    # Verify that the missing fcst_t_beg column has been added with CN.MV_NULL
    assert rdf.mtd_2d_data['fcst_t_beg'][0] == CN.MV_NULL

@pytest.mark.parametrize("get_generic_xml_loadfile", ['mtd_2d'], indirect=True)
def test_mtd_missing_columns(tmp_path, get_generic_xml_loadfile):
    '''
       Verify that expected behavior is observed when the mtd file is missing the
       FCST_T_END, OBS_T_BEG, OBS_T_END, and FCST_UNITS columns.
       These columns should be added to the dataframe with the expected
       default value.

    '''
    XML_LOADFILE = get_generic_xml_loadfile(tmp_path, MTD_MISSING_COLUMNS, 'mtd_2d')

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()
    rdf.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )

    # Verify that the missing fcst_t_end column has been added with CN.MV_NULL
    assert rdf.mtd_2d_data['fcst_t_end'][0] == CN.MV_NULL
    assert rdf.mtd_2d_data['obs_t_beg'][0] == CN.MV_NULL
    assert rdf.mtd_2d_data['obs_t_end'][0] == CN.MV_NULL
    assert rdf.mtd_2d_data['fcst_units'][0] == CN.NOTAV
    assert rdf.mtd_2d_data['obs_units'][0] == CN.NOTAV


@pytest.mark.parametrize("get_generic_xml_loadfile", ['mtd_2d'], indirect=True)
def test_mtd_intensity_90_last_col(tmp_path, get_generic_xml_loadfile):
    '''
       Verify that expected behavior is observed when the mtd file is missing the
       intensity_99 column and intensity_90 is the last column.  Another
       intensity column should be added.

    '''
    XML_LOADFILE = get_generic_xml_loadfile(tmp_path, MTD_INTENSITY_90_LAST_COL, 'mtd_2d')

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()
    rdf.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )

    # verify that a column was added after the intensity_90 column and the
    # added column is 'intensity_nn'
    columns = rdf.mtd_2d_data.columns.to_list()
    for idx, col in enumerate(columns):
        if col == 'intensity_90':
            idx_intensity_90 = idx

    assert len(rdf.mtd_2d_data.iloc[idx_intensity_90 + 1]) > 0
    assert rdf.mtd_2d_data['intensity_nn'].shape[0] > 0
    rdf.mtd_2d_data.to_csv("./intensity_90.txt", header=True, sep=" ")


@pytest.mark.parametrize("get_generic_xml_loadfile", ['tcst'], indirect=True)
def test_tcst(tmp_path, get_generic_xml_loadfile):
    '''
       Verify that expected behavior is observed when the tcst  file has data
       but no header.

    '''
    XML_LOADFILE = get_generic_xml_loadfile(tmp_path, TCSTAT_DIR, 'tcst')

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()

    rdf.read_data(
        XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
    )

    # Expect 8 rows of data
    assert rdf.stat_data.shape[0] == 8


@pytest.mark.parametrize("get_generic_xml_loadfile", ['tcst'], indirect=True)
def test_tcst_no_header(tmp_path, get_generic_xml_loadfile):
    '''
       Verify that expected behavior is observed when the tcst  file has data
       but no header.

    '''
    XML_LOADFILE = get_generic_xml_loadfile(tmp_path, TCSTAT_NO_HEADER, 'tcst')

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()

    with pytest.raises(SystemExit):
        rdf.read_data(
            XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
        )
