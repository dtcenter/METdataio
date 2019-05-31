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
import numpy as np
import pandas as pd

import constants as CN


class ReadDataFiles:
    """! Class to read in data files given in load_spec xml file
        Returns:
           N/A
    """

    def __init__(self):
        self.cache = {}

    def read_data(self, load_files, load_flags, line_types):
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
                        file_hdr = \
                            pd.read_csv(filename, delim_whitespace=True,
                                        names=range(CN.MAX_COL), nrows=1)
                        print(file_hdr.iloc[0])

                        if not file_hdr.iloc[0].str.contains(CN.DESC).any():
                            print("Old MET file - no DESC")
                            hdr_names = CN.SHORT_HEADER + CN.COL_NUMS
                            one_file = pd.read_csv(filename, delim_whitespace=True,
                                                   names=hdr_names, skiprows=1,
                                                   parse_dates=[CN.FCST_VALID_BEG,
                                                                CN.FCST_VALID_END,
                                                                CN.OBS_VALID_BEG,
                                                                CN.OBS_VALID_END],
                                                   date_parser=self.cached_date_parser,
                                                   keep_default_na=False, na_values='')

                            one_file.insert(2, CN.DESC, CN.NOTAV)
                            one_file.insert(10, CN.FCST_UNITS, CN.NOTAV)
                            one_file.insert(13, CN.OBS_UNITS, CN.NOTAV)

                        elif not file_hdr.iloc[0].str.contains(CN.FCST_UNITS).any():
                            print("Older MET file - no FCST_UNITS")
                            hdr_names = CN.MID_HEADER + CN.COL_NUMS
                            one_file = pd.read_csv(filename, delim_whitespace=True,
                                                   names=hdr_names, skiprows=1,
                                                   parse_dates=[CN.FCST_VALID_BEG,
                                                                CN.FCST_VALID_END,
                                                                CN.OBS_VALID_BEG,
                                                                CN.OBS_VALID_END],
                                                   date_parser=self.cached_date_parser,
                                                   keep_default_na=False, na_values='')
                            one_file.insert(10, CN.FCST_UNITS, CN.NOTAV)
                            one_file.insert(13, CN.OBS_UNITS, CN.NOTAV)

                        else:
                            hdr_names = CN.LONG_HEADER + CN.COL_NUMS
                            one_file = pd.read_csv(filename, delim_whitespace=True,
                                                   names=hdr_names, skiprows=1,
                                                   parse_dates=[CN.FCST_VALID_BEG,
                                                                CN.FCST_VALID_END,
                                                                CN.OBS_VALID_BEG,
                                                                CN.OBS_VALID_END],
                                                   date_parser=self.cached_date_parser,
                                                   keep_default_na=False, na_values='')

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
                        all_stat = all_stat.append(one_file, ignore_index=True)
                        print("Size of", filename, one_file.size)
                        file_hdr = file_hdr.iloc[0:0]
                        one_file = one_file.iloc[0:0]

        except (RuntimeError, TypeError, NameError, KeyError):
            print("***", sys.exc_info()[0], "in", "read_data", "***")

        print("Shape of all_stat before", all_stat.shape)
        print(all_stat.groupby(CN.LINE_TYPE).count())

        try:

            # remove any lines that have invalid line_types
            all_stat = all_stat.drop(all_stat[~all_stat.LINE_TYPE.isin(CN.UC_LINE_TYPES)].index)
            all_stat.reset_index(drop=True, inplace=True)

            # if user specified line types to load, delete the rest
            if load_flags["line_type_load"]:
                all_stat = all_stat.drop(all_stat[~all_stat.LINE_TYPE.isin(line_types)].index)
                all_stat.reset_index(drop=True, inplace=True)

            # if XML has flag to not load MPR records, delete them
            if not load_flags["load_mpr"]:
                all_stat = all_stat.drop(all_stat[all_stat.LINE_TYPE == CN.MPR.upper()].index)
                all_stat.reset_index(drop=True, inplace=True)

            # if XML has flag to not load ORANK records, delete them
            if not load_flags["load_orank"]:
                all_stat = all_stat.drop(all_stat[all_stat.LINE_TYPE == CN.ORANK.upper()].index)
                all_stat.reset_index(drop=True, inplace=True)

            # Copy forecast lead times, without trailing 0000 if they have them
            all_stat[CN.FCST_LEAD_HR] = \
                np.where(all_stat[CN.FCST_LEAD] > 9999,
                         all_stat[CN.FCST_LEAD] // 10000,
                         all_stat[CN.FCST_LEAD])

            # Calculate fcst_init_beg = fcst_valid_beg - fcst_lead hours
            all_stat.insert(6, CN.FCST_INIT_BEG, CN.NOTAV)
            all_stat[CN.FCST_INIT_BEG] = all_stat[CN.FCST_VALID_BEG] - \
                                         pd.to_timedelta(all_stat[CN.FCST_LEAD_HR], unit='h')
            print(all_stat.iloc[0])

            print("Shape of all_stat after", all_stat.shape)
            print(all_stat.groupby(CN.LINE_TYPE).count())

            print(all_stat.COV_THRESH.unique())
            print(all_stat.ALPHA.unique())
            print(all_stat.dtypes)

        except (RuntimeError, TypeError, NameError, KeyError):
            print("***", sys.exc_info()[0], "in", "read_data", "***")

    def cached_date_parser(self, date_str):
        """ if date is repeated and already converted, return that value.
            Returns:
               date in datetime format
        """
        # if date is repeated and already converted, return that value
        if date_str in self.cache:
            return self.cache[date_str]
        if (date_str.startswith('F') or date_str.startswith('O')):
            return pd.to_datetime('20000101_000000', format='%Y%m%d_%H%M%S')
        date_time = pd.to_datetime(date_str, format='%Y%m%d_%H%M%S')
        self.cache[date_str] = date_time
        return date_time
