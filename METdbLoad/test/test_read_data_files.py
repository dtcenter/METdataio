#!/usr/bin/env python3
"""Test reading data files."""

import pytest
import logging
from unittest.mock import MagicMock

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
    TCSTAT_EMPTY,
    MTD_HEADER_NO_DATA,
    MTD_INTENSITY_90_LAST_COL,
    MTD_NO_FCST_T_BEG,
    MTD_ONE_EMPTY,
    MTD_MISSING_COLUMNS,
    get_xml_test_file,
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


@pytest.mark.parametrize("get_generic_xml_loadfile", ['mtd_3d'], indirect=True)
def test_mtd_3d(tmp_path, get_generic_xml_loadfile):
    '''
       Verify that MTD 3D data is correctly read in
    '''

    XML_LOADFILE = get_generic_xml_loadfile(tmp_path, MTD_DATA_DIR, 'mtd_3d')

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
    rdf.mtd_2d_data.to_csv(tmp_path / "intensity_90.txt", header=True, sep=" ")


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

@pytest.mark.parametrize("get_generic_xml_loadfile", ['tcst'], indirect=True)
def test_tcst_empty(tmp_path, get_generic_xml_loadfile):
    '''
       Verify that expected behavior is observed when the tcst  file has data
       but no header.

    '''
    XML_LOADFILE = get_generic_xml_loadfile(tmp_path, TCSTAT_EMPTY, 'tcst')

    # Read all of the data from the data files into a dataframe
    rdf = ReadDataFiles()

    # with pytest.raises(SystemExit):
    rdf.read_data(
            XML_LOADFILE.flags, XML_LOADFILE.load_files, XML_LOADFILE.line_types
        )
    assert rdf.tcst_data.shape[0] == 0



# Header line of a MET .stat file
STAT_HEADER = (
    "VERSION MODEL DESC FCST_LEAD FCST_VALID_BEG FCST_VALID_END OBS_LEAD OBS_VALID_BEG"
    " OBS_VALID_END FCST_VAR FCST_UNITS FCST_LEV OBS_VAR OBS_UNITS OBS_LEV OBTYPE VX_MASK"
    " INTERP_MTHD INTERP_PNTS FCST_THRESH OBS_THRESH COV_THRESH ALPHA LINE_TYPE\n"
)
# Start of a .stat data line, up to and including INTERP_PNTS
STAT_PREFIX = (
    "V10.1.1 GFS NA 010000 20190615_010000 20190615_010000 000000 20190615_003001"
    " 20190615_013000 TMP K Z2 TMP K Z2 ADPSFC CONUS BILIN 4 "
)


def read_files(tmp_path, files, met_tool="point_stat", logger=None, extra_files=()):
    """Write files (a dict of filename: contents) to a directory, then read them

    Args:
        extra_files: paths added to the list of files to load after the directory
            contents, e.g. a file that does not exist

    Returns:
        ReadDataFiles object after read_data was called
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    for filename, contents in files.items():
        (data_dir / filename).write_text(contents)

    xml_loadfile = XmlLoadFile(get_xml_test_file(tmp_path, str(data_dir), met_tool))
    xml_loadfile.read_xml()
    file_data = ReadDataFiles(logger)
    file_data.read_data(
        xml_loadfile.flags,
        xml_loadfile.load_files + list(extra_files),
        xml_loadfile.line_types,
    )
    return file_data


def warnings_logged(mock_logger):
    return [str(c.args[0]) for c in mock_logger.warning.call_args_list]


@pytest.mark.parametrize(
    "header, line, expected_descr",
    [
        # MET 4: no DESC or units columns
        (
            "VERSION MODEL FCST_LEAD FCST_VALID_BEG FCST_VALID_END OBS_LEAD OBS_VALID_BEG"
            " OBS_VALID_END FCST_VAR FCST_LEV OBS_VAR OBS_LEV OBTYPE VX_MASK INTERP_MTHD"
            " INTERP_PNTS FCST_THRESH OBS_THRESH COV_THRESH ALPHA LINE_TYPE\n",
            "V4.2 GFS 010000 20190615_010000 20190615_010000 000000 20190615_003001"
            " 20190615_013000 TMP Z2 TMP Z2 ADPSFC CONUS BILIN 4 >273 >273 NA NA CTC"
            " 100 10 20 30 40\n",
            "NA",
        ),
        # MET 5: DESC but no units columns
        (
            "VERSION MODEL DESC FCST_LEAD FCST_VALID_BEG FCST_VALID_END OBS_LEAD"
            " OBS_VALID_BEG OBS_VALID_END FCST_VAR FCST_LEV OBS_VAR OBS_LEV OBTYPE VX_MASK"
            " INTERP_MTHD INTERP_PNTS FCST_THRESH OBS_THRESH COV_THRESH ALPHA LINE_TYPE\n",
            "V5.2 GFS mydesc 010000 20190615_010000 20190615_010000 000000 20190615_003001"
            " 20190615_013000 TMP Z2 TMP Z2 ADPSFC CONUS BILIN 4 >273 >273 NA NA CTC"
            " 100 10 20 30 40\n",
            "mydesc",
        ),
    ],
)
def test_stat_file_old_header(tmp_path, header, line, expected_descr):
    """Files from older MET versions without DESC or units columns are read"""
    file_data = read_files(
        tmp_path,
        {
            "old.stat": header + line,
            # files with a header but no data lines are skipped
            "header_only.stat": header,
        },
    )

    stat = file_data.stat_data
    assert len(stat) == 1
    row = stat.iloc[0]
    assert row[CN.DESCR] == expected_descr
    assert row[CN.FCST_UNITS] == CN.NOTAV
    assert row[CN.OBS_UNITS] == CN.NOTAV
    # the columns after the missing ones are still in the right place
    assert row[CN.FCST_VAR] == "TMP"
    assert row[CN.FCST_LEV] == "Z2"
    assert row[CN.OBS_LEV] == "Z2"
    assert row[CN.FCST_THRESH] == ">273"
    assert row[CN.LINE_TYPE] == "CTC"
    assert [row[str(i)] for i in range(5)] == ["100", 10.0, "20", "30", "40"]


def test_percentile_thresholds(tmp_path):
    """A percentile value in parens is split out of fcst_thresh and obs_thresh"""
    file_data = read_files(
        tmp_path,
        {
            "perc.stat": STAT_HEADER
            + STAT_PREFIX + ">=SFP50(291.81) >=SOP50(293.05) NA NA CTC 100 10 20 30 40\n"
            + STAT_PREFIX + ">273 >273 NA NA CTC 100 1 2 3 94\n"
        },
    )

    stat = file_data.stat_data.sort_values(CN.LINE_NUM)
    assert stat[CN.FCST_THRESH].tolist() == [">=SFP50", ">273"]
    assert stat[CN.OBS_THRESH].tolist() == [">=SOP50", ">273"]
    assert stat[CN.FCST_PERC].tolist() == ["291.81", CN.MV_NOTAV]
    assert stat[CN.OBS_PERC].tolist() == ["293.05", CN.MV_NOTAV]


def test_invalid_and_unselected_line_types(tmp_path):
    """Lines with an unknown line type are dropped, as are line types not selected"""
    contents = (
        STAT_HEADER
        + STAT_PREFIX + "NA NA NA NA CTC 100 1 2 3 94\n"
        + STAT_PREFIX + "NA NA NA NA BOGUS 1 2 3\n"
        + STAT_PREFIX + "NA NA NA NA FHO 100 0.1 0.2 0.3\n"
    )
    mock_logger = MagicMock()
    file_data = read_files(tmp_path, {"lines.stat": contents}, logger=mock_logger)
    assert sorted(file_data.stat_data[CN.LINE_TYPE]) == ["CTC", "FHO"]
    assert "!!! Warning, invalid line_types:" in warnings_logged(mock_logger)

    # only load the line types listed in the XML
    data_dir = tmp_path / "data"
    xml_loadfile = XmlLoadFile(get_xml_test_file(tmp_path, str(data_dir), "point_stat"))
    xml_loadfile.read_xml()
    xml_loadfile.flags["line_type_load"] = True
    file_data = ReadDataFiles()
    file_data.read_data(xml_loadfile.flags, xml_loadfile.load_files, ["FHO"])
    assert file_data.stat_data[CN.LINE_TYPE].tolist() == ["FHO"]


def test_alpha_warning(tmp_path):
    """A warning is logged when a line type that has no ALPHA has an ALPHA value"""
    mock_logger = MagicMock()
    file_data = read_files(
        tmp_path,
        {"alpha.stat": STAT_HEADER + STAT_PREFIX + "NA NA NA 0.05 CTC 100 1 2 3 94\n"},
        logger=mock_logger,
    )
    assert file_data.stat_data[CN.ALPHA].tolist() == ["0.05"]
    assert any(
        w.startswith("!!! non-ALPHA line_type has ALPHA float value")
        for w in warnings_logged(mock_logger)
    )


def test_rps_missing_rps_comp(tmp_path):
    """RPS lines from older MET versions have no RPS_COMP, so it is set to 1 - RPS"""
    # RPS columns: TOTAL N_PROB RPS_REL RPS_RES RPS_UNC RPS RPSS RPSS_SMPL RPS_COMP
    # The second line is longer, so the shorter RPS line is padded with nulls.
    file_data = read_files(
        tmp_path,
        {
            "rps.stat": STAT_HEADER
            + STAT_PREFIX + "NA NA NA NA RPS 100 1 0.1 0.2 0.3 0.25 0.5 0.6\n"
            + STAT_PREFIX + "NA NA NA NA RPS 100 1 0.1 0.2 0.3 0.4 0.5 0.6 0.7 extra\n"
        },
    )
    stat = file_data.stat_data.sort_values(CN.LINE_NUM)
    assert stat["8"].astype(float).tolist() == [0.75, 0.7]


def test_empty_and_missing_files(tmp_path):
    """Empty and missing files are skipped and the other files are still read"""
    mock_logger = MagicMock()
    missing = str(tmp_path / "data" / "missing.stat")
    file_data = read_files(
        tmp_path,
        {
            "good.stat": STAT_HEADER + STAT_PREFIX + "NA NA NA NA CTC 100 1 2 3 94\n",
            "empty.stat": "",
        },
        logger=mock_logger,
        extra_files=[missing],
    )
    assert file_data.stat_data[CN.LINE_TYPE].tolist() == ["CTC"]
    warnings = warnings_logged(mock_logger)
    assert any("empty.stat has no columns" in w for w in warnings)
    assert f"!!! No file {missing}" in warnings


@pytest.mark.parametrize(
    "filename, lookup",
    [
        ("point_stat.stat", CN.STAT),
        ("gfs.vsdb", CN.VSDB_POINT_STAT),
        ("mode_cts.txt", CN.MODE_CTS),
        ("mode_obj.txt", CN.MODE_OBJ),
        ("mtd_2d.txt", CN.MTD_2D),
        ("mtd_3d_pair_cluster.txt", CN.MTD_3D_PC),
        ("mtd_3d_pair_simple.txt", CN.MTD_3D_PS),
        ("mtd_3d_single_cluster.txt", CN.MTD_3D_SC),
        ("mtd_3d_single_simple.txt", CN.MTD_3D_SS),
        ("tc_pairs.tcst", CN.TCST),
        ("readme.md", CN.NO_KEY),
    ],
)
def test_get_lookup(filename, lookup):
    """The file type is determined from the end of the filename"""
    assert ReadDataFiles.get_lookup(f"/some/dir/{filename.upper()}") == lookup


# A VSDB data line. VSDB files have no units, interp or alpha columns.
# A number after a slash in the model is the number of members/bins for some line types.
VSDB_LINE = "V01 GFS{n} 24 2014080200 ADPUPA G2/NHX {line_type} {var} P850 = {values}\n"


def vsdb_lines(lines):
    return "".join(
        VSDB_LINE.format(n=n, line_type=lt, var=var, values=values)
        for n, lt, var, values in lines
    )


def stat_values(row, count):
    """Values of the first count data columns of a row, as floats"""
    return [float(row[str(i)]) for i in range(count)]


def test_vsdb_line_types(tmp_path):
    """VSDB line types are converted to the equivalent MET .stat line types"""
    file_data = read_files(
        tmp_path,
        {
            "gfs.vsdb": vsdb_lines(
                [
                    # VL1L2 without f_speed_bar and o_speed_bar
                    ("", "VL1L2", "WIND", "100 1 2 3 4 5 6 7"),
                    ("", "VAL1L2", "WIND", "100 1 2 3 4 5 6 7"),
                    # HIST with 3 after the slash has 4 rank values, which are fractions
                    ("/3", "HIST", "T", "0.1 0.2 0.3 0.4"),
                    ("/3", "HIST", "T", "0.4 0.3 0.2 0.1"),
                    ("/3", "RELP", "T", "0.2 0.3 0.5"),
                    ("/3", "RELP", "T", "0.5 0.3 0.2"),
                    # ECON always has 18 values
                    ("/20", "ECON", "T", " ".join(f"{0.05 * i:.2f}" for i in range(18))),
                    ("", "FSS>1", "APCP", "100 0.2 0.5 0.1"),
                ]
            )
        },
        met_tool="vsdb",
    )
    stat = file_data.stat_data.set_index(CN.LINE_NUM)

    assert stat[CN.LINE_TYPE].tolist() == [
        "VL1L2", "VAL1L2", "RHIST", "RHIST", "RELP", "RELP", "ECLV", "NBRCNT"
    ]
    # the member count is removed from the model name
    assert set(stat[CN.MODEL]) == {"GFS"}

    # VL1L2: total and 7 values, then f_speed_bar and o_speed_bar are missing
    assert stat_values(stat.loc[1], 8) == [100, 1, 2, 3, 4, 5, 6, 7]

    # RHIST: total is 0, n_rank, then the ranks as percentages
    rhist = stat.loc[3]
    assert stat_values(rhist, 2) == [0, 4]
    assert [rhist[str(i)] for i in range(2, 6)] == pytest.approx([10, 20, 30, 40])

    # RELP: total is 0, n_ens, then the values
    relp = stat.loc[6]
    assert stat_values(relp, 2) == [0, 3]
    assert [float(relp[str(i)]) for i in range(2, 5)] == [0.5, 0.3, 0.2]

    # ECLV: total, baser, value_baser, n_pnt, then pairs of x_pnt_i and y_pnt_i where
    # x_pnt_i are fixed cost/loss ratios and y_pnt_i are the VSDB values
    eclv = stat.loc[7]
    assert float(eclv["3"]) == 18
    pairs = [(float(eclv[str(i)]), float(eclv[str(i + 1)])) for i in range(4, 40, 2)]
    assert [x for x, _ in pairs] == pytest.approx(CN.X_POINTS_ECON)
    assert [y for _, y in pairs] == pytest.approx([0.05 * i for i in range(18)])

    # FSS: the threshold is split out of the line type
    fss = stat.loc[8]
    assert fss[CN.FCST_THRESH] == ">1"
    assert fss[CN.OBS_THRESH] == ">1"
    assert stat_values(fss, 2) == [100, 0.2]


def test_vsdb_vl1l2_without_speed_columns(tmp_path):
    """f_speed_bar and o_speed_bar are added as not available for VSDB VL1L2 lines"""
    file_data = read_files(
        tmp_path,
        {"gfs.vsdb": vsdb_lines([("", "VL1L2", "WIND", "100 1 2 3 4 5 6 7")])},
        met_tool="vsdb",
    )
    row = file_data.stat_data.iloc[0]
    assert stat_values(row, 10) == [100, 1, 2, 3, 4, 5, 6, 7, -9999, -9999]


def test_vsdb_ensemble_line_types(tmp_path):
    """VSDB BSS, RMSE and RPS lines are converted to PSTD, CNT and ENSCNT"""
    file_data = read_files(
        tmp_path,
        {
            "gfs.vsdb": vsdb_lines(
                [
                    ("/20", "BSS", "T", "0.11 0.22 0.33 0.44 0.55 0.66"),
                    ("/20", "RMSE", "T", "1.1 2.2 3.3 4.4 5.5"),
                    ("/20", "RPS", "T", "0.1 0.2 0.3 0.4 0.5 0.6"),
                ]
            )
        },
        met_tool="vsdb",
    )
    stat = file_data.stat_data.set_index(CN.LINE_TYPE)
    assert sorted(stat.index) == ["CNT", "ENSCNT", "PSTD"]

    # BSS: values 3, 4, 5 are baser, baser_ncl, baser_ncu ... (see read_data_files.py)
    pstd = stat.loc["PSTD"]
    assert stat_values(pstd, 2) == [0, 0]
    assert [float(pstd[str(i)]) for i in (5, 6, 7, 9, 12, 15)] == [
        0.44, 0.55, 0.66, 0.11, 0.22, 0.33
    ]

    # RPS: the 6 values are every 5th column
    enscnt = stat.loc["ENSCNT"]
    assert [float(enscnt[str(i)]) for i in range(0, 26, 5)] == [
        0.1, 0.2, 0.3, 0.4, 0.5, 0.6
    ]


def test_vsdb_single_hist_line(tmp_path):
    file_data = read_files(
        tmp_path,
        {"gfs.vsdb": vsdb_lines([("/3", "HIST", "T", "0.1 0.2 0.3 0.4")])},
        met_tool="vsdb",
    )
    assert file_data.stat_data["1"].tolist() == [4]


def test_vsdb_hist_lines_with_different_ranks(tmp_path):
    file_data = read_files(
        tmp_path,
        {
            "gfs.vsdb": vsdb_lines(
                [
                    ("/3", "HIST", "T", "0.1 0.2 0.3 0.4"),
                    ("/1", "HIST", "T", "0.6 0.4"),
                ]
            )
        },
        met_tool="vsdb",
    )
    stat = file_data.stat_data.set_index(CN.LINE_NUM)
    assert [stat.loc[1][str(i)] for i in range(2, 6)] == pytest.approx([10, 20, 30, 40])
