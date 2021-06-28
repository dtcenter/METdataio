#!/usr/bin/env python3

"""
Program Name: read_netcdf.py
Contact(s): Hank Fisher 
Usage: Read netcdf files and load into pandas.
Input Files: netcdf files 
Copyright 2020 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member

import sys
import os
from pathlib import Path
import logging
import time
from datetime import timedelta
from datetime import datetime
import numpy as np
import pandas as pd
import xarray as xr
import yaml
from METcalcpy.metcalcpy.util.read_env_vars_in_config import parse_config


class ReadNetCDF:
    """! Class to read in netcdf files iven in yaml config file
        Returns:
           N/A
    """

    def __init__(self):
        self.cache = {}
        self.nc_pandas_data = pd.DataFrame()
        self.data_files = pd.DataFrame()

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


    def read_files(self, load_files):
        """ Read in data files as given in yaml config.
            Returns: pandas dataframe
        """
        
        df = pd.DataFrame()
        for file in load_files:
            xarray_data = xr.open_dataset(file)
            df = df.append(xarray_data.to_dataframe(),ignore_index=True, sort=False)

def main():
    """
    Reads in a default config file that loads sample MET output data and puts it into a pandas
    DataFrame
    """

    file_reader = ReadNetCDF()

    #The advantage of using YAML is that you can use environment variables to
    #reference a path to the file
    yaml_config_file = "read_netcdf.yaml"
    load_files = file_reader.readYAMLConfig(yaml_config_file)

    file_reader.read_files(load_files)


if __name__ == "__main__":
    main()

