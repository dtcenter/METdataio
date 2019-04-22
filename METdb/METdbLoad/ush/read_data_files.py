#!/usr/bin/env python3

"""
Program Name: read_data_files.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Read data files given in load_spec XML file.
Parameters: N/A
Input Files: data files of type MET, VSDB, MODE, MTD
Output Files: N/A
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

import sys
from pathlib import Path
import pandas as pd

import METdb.METdbLoad.ush.constants as CN


class ReadDataFiles:
    """! Class to read in data files given in load_spec xml file
        Returns:
           N/A
    """

    def __init__(self):
        print("class")

    def read_data(self, load_files):
        """ Read in data files as given in load_spec xml file.
            Returns:
               N/A
        """

        # handle MET files, VSDB files, MODE files, and MTD files

        # speed up with dask delayed?

        one_file = pd.DataFrame()

        try:
            # Check to make sure files exist
            for filename in load_files:
                file_and_path = Path(filename)
                if file_and_path.is_file():
                    # check for blank files or, for MET, no data after header line files
                    # older MET files may be missing DESC
                    # handle variable number of fields
                    if filename.lower().endswith(".stat"):
                        one_file = pd.read_csv(filename, delim_whitespace=True, names=range(100))
                        print(one_file.iloc[1])
                        if CN.DESC not in one_file.iloc[1]:
                            print("older MET file - no DESC")
                    elif filename.lower().endswith(".vsdb"):
                        one_file = pd.read_csv(filename, delim_whitespace=True, names=range(100))
                        # make this data look like a Met file
                        print(one_file.iloc[0])
                    else:
                        print("this file type is not handled yet")

                    # keep track of files containing data for creating data_file records later

                    # after file is transformed, add this data to collection(s) of data

                    # re-initialize pandas dataframe before reading next file
                    if not one_file.empty:
                        one_file = one_file.iloc[0:0]

        except (RuntimeError, TypeError, NameError):
            print("***", sys.exc_info()[0], "in", "read_data", "***")
