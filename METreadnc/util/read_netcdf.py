#!/usr/bin/env python3

"""
Program Name: read_netcdf.py
Contact(s): Hank Fisher 
Usage: Read netcdf files and load into pandas.
Input Files: netcdf files 
Copyright 2020 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

import sys
import os
import argparse
import pandas as pd
import xarray as xr
import yaml
# Setting PYTHONPATH to METcalcpy
# or pip install . in the directory METcalcpy makes this the better import
from metcalcpy.util.read_env_vars_in_config import parse_config


class ReadNetCDF:
    """! Class to read in netcdf files given in yaml config file
        Returns: Either a list of xarray DataSets or a pandas DataFrame
           N/A
    """

    def __init__(self):
        self.pandas_data = pd.DataFrame()
        self.xarray_data = []

    def readYAMLConfig(self, configFile):
        """ Returns a file or list of files

        Args:
            configFile: A YAML formatted config file

        Returns: 
            returns a list containing a single or multiple file names including path
        """

        # Retrieve the contents of a YAML custom config file to over-ride
        # or augment settings defined by the default config file.
        # Use a configure file parser that handles environment variables
        files_dict = parse_config(configFile)

        # parse_config returns a dictionary, read_data_files wants a list
        files = files_dict['files']
        return files

    def read_into_pandas(self, load_files) -> pd.DataFrame:
        """ Read in data files as a list specified in yaml config or
            invoke directly with input provided as a list, tuple, or a single file.

            Args:
                load_files: File or a list or tuple of files to read and convert to pandas.

            Returns: pandas dataframe
        """

        if isinstance(load_files, list) or isinstance(load_files, tuple):
            pd_list = []
            for file in load_files:
                file_data = xr.open_dataset(file)
                pd_list.append(file_data.to_dataframe().reset_index())
            df = pd.concat(pd_list)
        elif isinstance(load_files, str):
            # single file specified
            file_data = xr.open_dataset(load_files)
            df = file_data.to_dataframe().reset_index()
        return df

    def read_into_xarray(self, load_files) -> list:
        """ Read in data files as a list as specified in a yaml config or if invoking
            directly, accept list, tuple, or single file name.
            Returns: a list of xarry DataSets
        """

        if isinstance(load_files, list) or isinstance(load_files, tuple):
            for file in load_files:
                # open a list or tuple of files
                file_data = xr.open_dataset(file)
                self.xarray_data.append(file_data)
        elif isinstance(load_files, str):
            # single file specified
            file_data = xr.open_dataset(load_files)
            self.xarray_data.append(file_data)
        else:
            raise ValueError('Input file(s) not recognized. Files must be specified as a single filename, list of '
                             'filenames, or a tuple of filenames.')

        return self.xarray_data


def main():
    """
    Reads in a YAML config file that contains a list of netcdf files to be loaded into a
    list of xarray Datasets and/or a pandas DataFrame
    """

    try:
        file_reader = ReadNetCDF()

        # Reading in the configuration file
        parser = argparse.ArgumentParser(description='Read in config file')
        parser.add_argument('Path', metavar='yaml_config_file', type=str,
                            help='the full path to the YAML config file')
        args = parser.parse_args()
        specified_config_file = args.Path
        load_files = file_reader.readYAMLConfig(specified_config_file)

        # Pandas dataframes are much larger than xarrays
        # The read_into_pandas should be commented out if you are testing this
        # on very large files
        netcdf_data_frame = file_reader.read_into_pandas(load_files)

        netcdf_data_set = file_reader.read_into_xarray(load_files)

    except RuntimeError:
        print(
            "*** %s occurred setting up read_netcdf ***", sys.exc_info()[0])
        sys.exit("*** Error setting up read_netcdf")


if __name__ == "__main__":
    main()
