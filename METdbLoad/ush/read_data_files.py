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

# pylint:disable=no-member
# constants exist in constants.py

import sys
from pathlib import Path
from datetime import timedelta
import pandas as pd

import constants as CN


class ReadDataFiles:
    """! Class to read in data files given in load_spec xml file
        Returns:
           N/A
    """

    def __init__(self):
        self.cache = {}

    def read_data(self, load_files):
        """ Read in data files as given in load_spec xml file.
            Returns:
               N/A
        """

        # handle MET files, VSDB files, MODE files, and MTD files

        # speed up with dask delayed?

        one_file = pd.DataFrame()
        all_stat = pd.DataFrame()

        try:
            # Check to make sure files exist
            for filename in load_files:
                file_and_path = Path(filename)
                if file_and_path.is_file():
                    # check for blank files or, for MET, no data after header line files
                    # older MET files may be missing DESC
                    # handle variable number of fields
                    if filename.lower().endswith(".stat"):
                        file_hdr = pd.read_csv(filename, delim_whitespace=True, names=range(100),nrows=1)
                        print(file_hdr.iloc[0])

                        if not file_hdr.iloc[0].str.contains(CN.DESC).any():
                            print("Old MET file - no DESC")
                            hdr_names = CN.SHORT_HEADER + CN.COL_NUMS
                            one_file = pd.read_csv(filename, delim_whitespace=True, names=hdr_names, skiprows=1,
                                                   parse_dates=[CN.FCST_VALID_BEG, CN.FCST_VALID_END,
                                                                CN.OBS_VALID_BEG,CN.OBS_VALID_END],
                                                   date_parser=self.cached_date_parser)

                            one_file.insert(2, CN.DESC, "NA")
                            one_file.insert(10, CN.FCST_UNITS, "NA")
                            one_file.insert(13, CN.OBS_UNITS, "NA")

                        elif not file_hdr.iloc[0].str.contains(CN.FCST_UNITS).any():
                            print("Older MET file - no FCST_UNITS")
                            hdr_names = CN.MID_HEADER + CN.COL_NUMS
                            one_file = pd.read_csv(filename, delim_whitespace=True, names=hdr_names, skiprows=1,
                                                   parse_dates=[CN.FCST_VALID_BEG, CN.FCST_VALID_END,
                                                                CN.OBS_VALID_BEG, CN.OBS_VALID_END],
                                                   date_parser=self.cached_date_parser)
                            one_file.insert(10, CN.FCST_UNITS, "NA")
                            one_file.insert(13, CN.OBS_UNITS, "NA")

                        else:
                            hdr_names = CN.LONG_HEADER + CN.COL_NUMS
                            one_file = pd.read_csv(filename, delim_whitespace=True, names=hdr_names, skiprows=1,
                                                   parse_dates=[CN.FCST_VALID_BEG, CN.FCST_VALID_END,
                                                                CN.OBS_VALID_BEG, CN.OBS_VALID_END],
                                                   date_parser=self.cached_date_parser)

                        print(one_file.iloc[0])
                    elif filename.lower().endswith(".vsdb"):
                        one_file = pd.read_csv(filename, delim_whitespace=True, names=range(100))
                        # make this data look like a Met file
                        print(one_file.iloc[0])
                    else:
                        print("this file type is not handled yet")

                    # keep track of files containing data for creating data_file records later

                    # after file is transformed, add this data to collection(s) of data

                    # re-initialize pandas dataframes before reading next file
                    if not one_file.empty:
                        all_stat = all_stat.append(one_file)
                        print("Size of", filename, one_file.size)
                        file_hdr = file_hdr.iloc[0:0]
                        one_file = one_file.iloc[0:0]

        except (RuntimeError, TypeError, NameError):
            print("***", sys.exc_info()[0], "in", "read_data", "***")

        print("Size of all files", all_stat.size)
        # todo: figure out how to calculate the fcst_init_beg column
        # all_stat[CN.FCST_INIT_BEG] = \
        #    all_stat[CN.FCST_VALID_BEG].sub(timedelta(seconds=all_stat[CN.FCST_LEAD].mul(.36)))
        print(all_stat.iloc[0])

    def cached_date_parser(self, s):
        # if date is repeated and already converted, return that value
        if s in self.cache:
            return self.cache[s]
        if (s.startswith('F') or s.startswith('O')):
            return pd.to_datetime('20000101_000000', format='%Y%m%d_%H%M%S')
        dt = pd.to_datetime(s, format='%Y%m%d_%H%M%S')
        self.cache[s] = dt
        return dt

