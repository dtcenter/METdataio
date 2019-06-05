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
import logging
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
        self.stat_data = pd.DataFrame()

    def read_data(self, load_files, load_flags, line_types):
        """ Read in data files as given in load_spec xml file.
            Returns:
               N/A
        """

        logging.debug("--- Start read_data ---")

        # handle MET files, VSDB files, MODE files, and MTD files

        # speed up with dask delayed?

        one_file = pd.DataFrame()
        all_stat = pd.DataFrame()


        try:
            # Check to make sure files exist
            for filename in load_files:
                # Read in each file. Add columns if needed. Append to all_stat dataframe.
                file_and_path = Path(filename)
                if file_and_path.is_file():
                    # check for blank files or, for MET, no data after header line files
                    # handle variable number of fields
                    if filename.lower().endswith(".stat"):
                        # Get the first line of the .stat file that has the headers
                        file_hdr = pd.read_csv(filename, delim_whitespace=True,
                                               names=range(CN.MAX_COL), nrows=1)

                        # MET file has no headers and no test - it's empty
                        if file_hdr.empty:
                            logging.warning("Stat file %s is empty", filename)
                            continue

                        # Add a DESC column if the data file does not have one
                        if not file_hdr.iloc[0].str.contains(CN.DESC).any():
                            logging.debug("Old MET file - no DESC")
                            hdr_names = CN.SHORT_HEADER + CN.COL_NUMS
                            one_file = self.read_stat(filename, hdr_names)

                            # If the file has no DESC column, add UNITS as well
                            one_file.insert(2, CN.DESC, CN.NOTAV)
                            one_file.insert(10, CN.FCST_UNITS, CN.NOTAV)
                            one_file.insert(13, CN.OBS_UNITS, CN.NOTAV)

                        # If the file has a DESC column, but no UNITS columns
                        elif not file_hdr.iloc[0].str.contains(CN.FCST_UNITS).any():
                            logging.debug("Older MET file - no FCST_UNITS")
                            hdr_names = CN.MID_HEADER + CN.COL_NUMS
                            one_file = self.read_stat(filename, hdr_names)

                            one_file.insert(10, CN.FCST_UNITS, CN.NOTAV)
                            one_file.insert(13, CN.OBS_UNITS, CN.NOTAV)

                        else:
                            hdr_names = CN.LONG_HEADER + CN.COL_NUMS
                            one_file = self.read_stat(filename, hdr_names)

                    elif filename.lower().endswith(".vsdb"):
                        one_file = pd.read_csv(filename, delim_whitespace=True, names=range(100))
                        # make this data look like a Met file
                    else:
                        logging.warning("This file type is not handled yet")

                    # keep track of files containing data for creating data_file records later

                    # re-initialize pandas dataframes before reading next file
                    if not one_file.empty:
                        all_stat = all_stat.append(one_file, ignore_index=True)
                        logging.debug("Lines in %s: %s", filename, str(one_file.size))
                        file_hdr = file_hdr.iloc[0:0]
                        one_file = one_file.iloc[0:0]
                    else:
                        logging.warning("Empty file %s", filename)
                        continue
                else:
                    logging.warning("No file %s", filename)

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_data ***", sys.exc_info()[0])

        logging.debug("Shape of all_stat before: %s", str(all_stat.shape))

        try:

            # delete any lines that have invalid line_types
            invalid_line_indexes = all_stat[~all_stat.LINE_TYPE.isin(CN.LINE_TYPES)].index

            logging.warning("Warning, invalid line_types:")
            logging.warning("line types: %s", str(all_stat.iloc[invalid_line_indexes].LINE_TYPE))

            all_stat = all_stat.drop(invalid_line_indexes, axis=0)

            # if user specified line types to load, delete the rest
            if load_flags["line_type_load"]:
                all_stat = all_stat.drop(all_stat[~all_stat.LINE_TYPE.isin(line_types)].index)

            # if XML has flag to not load MPR records, delete them
            if not load_flags["load_mpr"]:
                all_stat = all_stat.drop(all_stat[all_stat.LINE_TYPE == CN.MPR].index)

            # if XML has flag to not load ORANK records, delete them
            if not load_flags["load_orank"]:
                all_stat = all_stat.drop(all_stat[all_stat.LINE_TYPE == CN.ORANK].index)

            # reset the index, in case any lines have been deleted
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

            logging.debug("Shape of all_stat after: %s", str(all_stat.shape))

            # print a warning message with data if value of alpha for an alpha line type is NA
            logging.warning("ALPHA line_type has ALPHA value of NA:\r\n %s",
                            str(all_stat[(all_stat.LINE_TYPE.isin(CN.ALPHA_LINE_TYPES)) &
                                         (all_stat.ALPHA == 'NA')].LINE_TYPE))

            # print a warning message with data if non-alpha line type has float value
            logging.warning("non-ALPHA line_type has ALPHA float value:\r\n %s",
                            str(all_stat[(~all_stat.LINE_TYPE.isin(CN.ALPHA_LINE_TYPES)) &
                                         (all_stat.ALPHA != 'NA')].LINE_TYPE))

            # Change ALL items in column ALPHA to -9999 if they are 'NA'
            all_stat.loc[all_stat.ALPHA == 'NA', CN.ALPHA] = -9999

            # Make ALPHA column into a decimal with no trailing zeroes after the decimal
            all_stat.ALPHA = all_stat.ALPHA.astype(float).map('{0:g}'.format)

            # Change ALL items in column COV_THRESH to '-9999' if they are 'NA'
            all_stat.loc[all_stat.COV_THRESH == 'NA', CN.COV_THRESH] = '-9999'

            # Change 'NA' values in column INTERP_PNTS to 0
            all_stat.loc[all_stat.INTERP_PNTS == 'NA', CN.INTERP_PNTS] = 0
            all_stat.INTERP_PNTS = all_stat.INTERP_PNTS.astype(int)

            logging.debug("Unique ALPHA values: %s", str(all_stat.ALPHA.unique()))
            logging.debug("Unique COV_THRESH values: %s", str(all_stat.COV_THRESH.unique()))
            logging.debug("Unique INTERP_PNTS: %s", str(all_stat.INTERP_PNTS.unique()))

            self.stat_data = all_stat

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_data ***", sys.exc_info()[0])

        logging.debug("--- End read_data ---")

    def read_stat(self, filename, hdr_names):
        """ Read in all of the lines except the header of a stat file.
            Returns:
               all the stat lines in a dataframe, with dates converted to datetime
        """
        return pd.read_csv(filename, delim_whitespace=True,
                           names=hdr_names, skiprows=1,
                           parse_dates=[CN.FCST_VALID_BEG,
                                        CN.FCST_VALID_END,
                                        CN.OBS_VALID_BEG,
                                        CN.OBS_VALID_END],
                           date_parser=self.cached_date_parser,
                           keep_default_na=False, na_values='')

    def cached_date_parser(self, date_str):
        """ if date is repeated and already converted, return that value.
            Returns:
               date in datetime format while reading in file
        """
        # if date is repeated and already converted, return that value
        if date_str in self.cache:
            return self.cache[date_str]
        if (date_str.startswith('F') or date_str.startswith('O')):
            return pd.to_datetime('20000101_000000', format='%Y%m%d_%H%M%S')
        date_time = pd.to_datetime(date_str, format='%Y%m%d_%H%M%S')
        self.cache[date_str] = date_time
        return date_time
