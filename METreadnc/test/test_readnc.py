
import os
import sys
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import xarray
import pytest

from METreadnc.util import read_netcdf as rn

# Top-level directory of the test input data, read from the METDATAIO_TEST_INPUT_DIR
# environment variable. Defaults to METdataio_Data in the top of the repository.
input_dir = os.path.abspath(os.environ.get(
    'METDATAIO_TEST_INPUT_DIR',
    os.path.join(os.path.dirname(__file__), '..', '..', 'METdataio_Data')
))

@pytest.mark.filterwarnings("ignore")
def test_xarray_one_file():
    # Test direct invocation with various input file types:
    # list, tuple, or single file (as a string)

    # Single input file
    file_reader = rn.ReadNetCDF()
    load_files = f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180201_000000.nc'
    netcdf_data_set = file_reader.read_into_xarray(load_files)
    assert isinstance(netcdf_data_set[0], xarray.Dataset)


def test_xarray_list_files():
    # List of input files
    file_reader = rn.ReadNetCDF()
    load_files = [f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180201_000000.nc',
                  f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180210_000000.nc',
                  f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180214_000000.nc',
                  f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180228_000000.nc']
    netcdf_data_set_list = file_reader.read_into_xarray(load_files)
    for cur in netcdf_data_set_list:
        assert isinstance(cur, xarray.Dataset)

# @pytest.mark.filterwarnings("ignore")
def test_xarray_tuple_files():
    # Test on data with only lat and level dimensions (no longitude)

    file_reader = rn.ReadNetCDF()
    load_files = [f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180201_000000.nc',
                  f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180210_000000.nc',
                  f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180214_000000.nc',
                  f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180228_000000.nc']
    netcdf_data_set_tup = file_reader.read_into_xarray(load_files)
    for cur in netcdf_data_set_tup:
        assert isinstance(cur, xarray.Dataset)

# @pytest.mark.filterwarnings("ignore")
def test_pandas_list():
    file_reader = rn.ReadNetCDF()
    load_files = [f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180201_000000.nc',
                  f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180210_000000.nc',
                  f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180214_000000.nc',
                  f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180228_000000.nc']
    netcdf_data_set_list = file_reader.read_into_pandas(load_files)

    assert isinstance(netcdf_data_set_list, pd.DataFrame)
    print(netcdf_data_set_list.columns)

@pytest.mark.filterwarnings("ignore")
def test_pandas_no_lon():
    # Verify that the resulting dataframe contains a level and
    # latitude column for this test data.
    file_reader = rn.ReadNetCDF()
    load_files = [f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180201_000000.nc',
                  f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180210_000000.nc',
                  f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180214_000000.nc',
                  f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180228_000000.nc']
    netcdf_data_set_list = file_reader.read_into_pandas(load_files)

    # if the dataframe is correct, then there should be lat and level columns
    expected_cols = ['pres', 'latitude', 'time', 'lat', 'u',
                     'T', 'lead_time']
    actual_cols = netcdf_data_set_list.columns.to_list()
    for col in actual_cols:
        assert col in expected_cols


def test_read_into_xarray_no_files():
    """

         Verify that an empty list of netCDF files is returned when an
         empty list of input files is passed into read_into_xarray()

    """
    load_files = []
    file_reader = rn.ReadNetCDF()
    netcdf_data_set_list = file_reader.read_into_xarray(load_files)

    assert (len(netcdf_data_set_list) == 0)


def test_read_into_pandas_no_files():
    """

         Verify that the ValueError is raised when an empty list of
          files is passed into the read_into_xarray() method.

    """
    load_files = []
    with pytest.raises(ValueError):
        file_reader = rn.ReadNetCDF()
        netcdf_data_set_list = file_reader.read_into_pandas(load_files)


def test_read_into_pandas_bogus_files():
    """

         Verify that the FileNotFoundError is raised when  list of
          bogus, non-existent files is passed into the read_into_pandas() method.

    """
    load_files = ['a','b','c']
    with pytest.raises(FileNotFoundError):
        file_reader = rn.ReadNetCDF()
        netcdf_data_set_list = file_reader.read_into_pandas(load_files)

    with pytest.raises(ValueError):
        file_reader = rn.ReadNetCDF()
        netcdf_data_set_list = file_reader.read_into_pandas(5)


def test_read_into_xarray_bogus_files():
            """

                 Verify that the ValueError is raised when an empty list of
                  files is passed into the read_into_xarray() method.

            """
            load_files = ['a', 'b', 'c']
            with pytest.raises(FileNotFoundError):
                file_reader = rn.ReadNetCDF()
                netcdf_data_set_list = file_reader.read_into_xarray(load_files)

            with pytest.raises(ValueError):
                file_reader = rn.ReadNetCDF()
                netcdf_data_set_list = file_reader.read_into_xarray(5)

LAT_LON_FILES = [
    'Z500_daily_19980216_NH.nc',
    'Z500_daily_20080106_NH.nc',
    'Z500_daily_20170224_NH.nc',
]


def test_read_yaml_config_env(monkeypatch):
    # test_readnc_env.yaml lists the files relative to the CALCPY_DATA env var
    monkeypatch.setenv('CALCPY_DATA', f'{input_dir}/lat_lon')
    file_reader = rn.ReadNetCDF()
    files = file_reader.readYAMLConfig(
        os.path.join(os.path.dirname(__file__), 'test_readnc_env.yaml'))
    assert files == [f'{input_dir}/lat_lon/{name}' for name in LAT_LON_FILES]


@pytest.mark.filterwarnings("ignore")
def test_pandas_one_file():
    file_reader = rn.ReadNetCDF()
    load_file = f'{input_dir}/lat_level/FCST_zonal_mean_U_T_20180201_000000.nc'
    df = file_reader.read_into_pandas(load_file)

    # one row for each combination of the dimensions
    data_set = xarray.open_dataset(load_file)
    assert len(df) == np.prod(list(data_set.sizes.values()))
    assert set(data_set.data_vars) <= set(df.columns)


@pytest.mark.filterwarnings("ignore")
def test_main(tmp_path, monkeypatch):
    config = tmp_path / 'config.yaml'
    config.write_text(
        'files:\n' + ''.join(f"  - '{input_dir}/lat_lon/{name}'\n" for name in LAT_LON_FILES))
    monkeypatch.setattr(sys, 'argv', ['read_netcdf.py', str(config)])

    read_into_xarray = rn.ReadNetCDF.read_into_xarray
    data_sets = []

    def keep_result(self, load_files):
        data_sets.extend(read_into_xarray(self, load_files))
        return data_sets

    monkeypatch.setattr(rn.ReadNetCDF, 'read_into_xarray', keep_result)
    rn.main()
    assert len(data_sets) == 3
    assert all(isinstance(ds, xarray.Dataset) for ds in data_sets)


def test_main_error(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['read_netcdf.py', str(tmp_path / 'config.yaml')])
    monkeypatch.setattr(rn.ReadNetCDF, 'readYAMLConfig', MagicMock(side_effect=RuntimeError))
    with pytest.raises(SystemExit, match='Error setting up read_netcdf'):
        rn.main()
