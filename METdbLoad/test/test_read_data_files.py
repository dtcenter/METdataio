#!/usr/bin/env python3
"""Test reading data files."""

import pytest

from METdataio.METdbLoad.ush.read_data_files import ReadDataFiles
from METdataio.METdbLoad.test.utils import (
    POINT_STAT_DATA_DIR,
    MTD_DATA_DIR,
)


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
    assert len(XML_LOADFILE.load_files) == 2
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
