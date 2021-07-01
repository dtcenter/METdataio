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
import pandas as pd
import xarray as xr
import yaml
from METcalcpy.metcalcpy.util.read_env_vars_in_config import parse_config


class ReadNetCDF:
    """! Class to read in netcdf files given in yaml config file
        Returns: Either a list of xarray DataSets or a pandas DataFrame
           N/A
    """

    def __init__(self):
        self.pandas_data = pd.DataFrame()
        self.xarray_data = [] 

    def readYAMLConfig(self,configFile):
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

        #parse_config returns a dictionary, read_data_files wants a list
        files = files_dict['files']
        return files


    def read_into_pandas(self, load_files):
        """ Read in data files as given in yaml config.
            Returns: pandas dataframe
        """
        
        df = pd.DataFrame()
        for file in load_files:
            file_data = xr.open_dataset(file)
            df = df.append(file_data.to_dataframe(), sort=False)

        return df

    def read_into_xarray(self, load_files):
        """ Read in data files as given in yaml config.
            Returns: a list of xarry DataSets
        """
        
        df = pd.DataFrame()
        for file in load_files:
            file_data = xr.open_dataset(file)
            self.xarray_data.append(file_data)

        print(self.xarray_data)
        return self.xarray_data


def main():
    """
    Reads in a default config file that contains a list of netcdf files to be loaded into an xarray
    and a pandas DataFrame
    """

    file_reader = ReadNetCDF()

    #The advantage of using YAML is that you can use environment variables to
    #reference a path to the file
    yaml_config_file = "read_netcdf.yaml"
    load_files = file_reader.readYAMLConfig(yaml_config_file)

    netcdf_df = file_reader.read_files(load_files)
    print(netcdf_df)


if __name__ == "__main__":
    main()

