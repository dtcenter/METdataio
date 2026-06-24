
import os
import pandas as pd
import xarray
import pytest

from METreadnc.util import read_netcdf as rn

cwd = os.path.dirname(__file__)
@pytest.mark.filterwarnings("ignore")
def test_xarray_one_file():
    # Test direct invocation with various input file types:
    # list, tuple, or single file (as a string)

    # Single input file
    file_reader = rn.ReadNetCDF()
    load_files = f'{cwd}/data/lat_level/FCST_zonal_mean_U_T_20180201_000000.nc'
    netcdf_data_set = file_reader.read_into_xarray(load_files)
    assert isinstance(netcdf_data_set[0], xarray.Dataset)


def test_xarray_list_files():
    # List of input files
    file_reader = rn.ReadNetCDF()
    load_files = [f'{cwd}/data/lat_level/FCST_zonal_mean_U_T_20180201_000000.nc',
                  f'{cwd}/data/lat_level/FCST_zonal_mean_U_T_20180210_000000.nc',
                  f'{cwd}/data/lat_level/FCST_zonal_mean_U_T_20180214_000000.nc',
                  f'{cwd}/data/lat_level/FCST_zonal_mean_U_T_20180228_000000.nc']
    netcdf_data_set_list = file_reader.read_into_xarray(load_files)
    for cur in netcdf_data_set_list:
        assert isinstance(cur, xarray.Dataset)

# @pytest.mark.filterwarnings("ignore")
def test_xarray_tuple_files():
    # Test on data with only lat and level dimensions (no longitude)

    file_reader = rn.ReadNetCDF()
    load_files = [f'{cwd}/data/lat_level/FCST_zonal_mean_U_T_20180201_000000.nc',
                  f'{cwd}/data/lat_level/FCST_zonal_mean_U_T_20180210_000000.nc',
                  f'{cwd}/data/lat_level/FCST_zonal_mean_U_T_20180214_000000.nc',
                  f'{cwd}/data/lat_level/FCST_zonal_mean_U_T_20180228_000000.nc']
    netcdf_data_set_tup = file_reader.read_into_xarray(load_files)
    for cur in netcdf_data_set_tup:
        assert isinstance(cur, xarray.Dataset)

# @pytest.mark.filterwarnings("ignore")
def test_pandas_list():
    file_reader = rn.ReadNetCDF()
    load_files = [f'{cwd}/data/lat_level/FCST_zonal_mean_U_T_20180201_000000.nc',
                  f'{cwd}/data/lat_level/FCST_zonal_mean_U_T_20180210_000000.nc',
                  f'{cwd}/data/lat_level/FCST_zonal_mean_U_T_20180214_000000.nc',
                  f'{cwd}/data/lat_level/FCST_zonal_mean_U_T_20180228_000000.nc']
    netcdf_data_set_list = file_reader.read_into_pandas(load_files)

    assert isinstance(netcdf_data_set_list, pd.DataFrame)
    print(netcdf_data_set_list.columns)

@pytest.mark.filterwarnings("ignore")
def test_pandas_no_lon():
    # Verify that the resulting dataframe contains a level and
    # latitude column for this test data.
    file_reader = rn.ReadNetCDF()
    load_files = [f'{cwd}/data/lat_level/FCST_zonal_mean_U_T_20180201_000000.nc',
                  f'{cwd}/data/lat_level/FCST_zonal_mean_U_T_20180210_000000.nc',
                  f'{cwd}/data/lat_level/FCST_zonal_mean_U_T_20180214_000000.nc',
                  f'{cwd}/data/lat_level/FCST_zonal_mean_U_T_20180228_000000.nc']
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
        netcdf_data_set_list = file_reader.read_into_xarray(load_files)


        def test_read_into_xarray_bogus_files():
            """

                 Verify that the ValueError is raised when an empty list of
                  files is passed into the read_into_xarray() method.

            """
            load_files = ['a', 'b', 'c']
            with pytest.raises(FileNotFoundError):
                file_reader = rn.ReadNetCDF()
                netcdf_data_set_list = file_reader.read_into_xarray(load_files)