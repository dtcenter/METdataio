#!/usr/bin/env python3

"""
Program Name: read_data_files.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Read data files given in load_spec file.
Parameters: N/A
Input Files: data files of type MET, VSDB, MODE, MTD
Output Files: N/A
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member
# constants exist in constants.py

import sys
import os
from pathlib import Path
import logging
import time
from datetime import timedelta
from datetime import datetime
import numpy as np
import pandas as pd

import constants as CN


class ReadDataFiles:
    """! Class to read in data files given in load_spec file
        Returns:
           N/A
    """

    def __init__(self):
        self.cache = {}
        self.stat_data = pd.DataFrame()
        self.data_files = pd.DataFrame()


    def read_data(self, load_flags, load_files, line_types):
        """ Read in data files as given in load_spec file.
            Returns:
               N/A
        """

        logging.debug("[--- Start read_data ---]")

        read_time_start = time.perf_counter()

        # handle MET files, VSDB files, MODE files, and MTD files

        # speed up with dask delayed?

        one_file = pd.DataFrame()
        file_hdr = pd.DataFrame()
        all_stat = pd.DataFrame()
        list_frames = []

        try:

            # Put the list of files into a dataframe to collect info to write to database
            self.data_files[CN.FULL_FILE] = load_files
            # Won't know database key until we interact with the database, so no keys yet
            self.data_files[CN.DATA_FILE_ID] = CN.NO_KEY
            # Store the index in a column to make later merging with stat data easier
            self.data_files[CN.FILE_ROW] = self.data_files.index
            # Add the code that describes what kind of file this is - stat, vsdb, etc
            self.data_files[CN.DATA_FILE_LU_ID] = \
                np.vectorize(self.get_lookup)(self.data_files[CN.FULL_FILE])
            # Break the full file name into path and filename
            self.data_files[CN.FILEPATH] = self.data_files[CN.FULL_FILE].str.rpartition('/')[0]
            self.data_files[CN.FILENAME] = self.data_files[CN.FULL_FILE].str.rpartition('/')[2]
            # current date and time for load date
            self.data_files[CN.LOAD_DATE] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.data_files[CN.MOD_DATE] = None

            # Check to make sure files exist
            for row_num, filename in self.data_files.iterrows():
                # Read in each file. Add columns if needed. Append to all_stat dataframe.
                file_and_path = Path(filename[CN.FULL_FILE])

                if file_and_path.is_file():
                    # check for blank files or, for MET, no data after header line files
                    # handle variable number of fields
                    # get file info like size of file and last modified date of file
                    stat_info = os.stat(file_and_path)
                    # get last modified date of file in standard time format
                    mod_date = time.strftime('%Y-%m-%d %H:%M:%S',
                                             time.localtime(stat_info.st_mtime))
                    self.data_files.at[row_num, CN.MOD_DATE] = mod_date

                    # Process stat files
                    if filename[CN.DATA_FILE_LU_ID] == CN.STAT:
                        # Get the first line of the .stat file that has the headers
                        file_hdr = pd.read_csv(filename[CN.FULL_FILE], delim_whitespace=True,
                                               names=range(CN.MAX_COL), nrows=1)

                        # MET file has no headers or no text - it's empty
                        if file_hdr.empty or stat_info.st_size == 0:
                            logging.warning("!!! Stat file %s is empty", filename[CN.FULL_FILE])
                            continue

                        # Add a DESC column if the data file does not have one
                        if not file_hdr.iloc[0].str.contains(CN.UC_DESC).any():
                            logging.debug("Old MET file - no DESC")
                            hdr_names = CN.SHORT_HEADER + CN.COL_NUMS
                            one_file = self.read_stat(filename[CN.FULL_FILE], hdr_names)

                            # If the file has no DESC column, add UNITS as well
                            one_file.insert(2, CN.DESCR, CN.NOTAV)
                            one_file.insert(10, CN.FCST_UNITS, CN.NOTAV)
                            one_file.insert(13, CN.OBS_UNITS, CN.NOTAV)

                        # If the file has a DESC column, but no UNITS columns
                        elif not file_hdr.iloc[0].str.contains(CN.UC_FCST_UNITS).any():
                            logging.debug("Older MET file - no FCST_UNITS")
                            hdr_names = CN.MID_HEADER + CN.COL_NUMS
                            one_file = self.read_stat(filename[CN.FULL_FILE], hdr_names)

                            one_file.insert(10, CN.FCST_UNITS, CN.NOTAV)
                            one_file.insert(13, CN.OBS_UNITS, CN.NOTAV)

                        else:
                            hdr_names = CN.LONG_HEADER + CN.COL_NUMS
                            one_file = self.read_stat(filename[CN.FULL_FILE], hdr_names)

                        # add line numbers and count the header line, for stat files
                        one_file[CN.LINE_NUM] = one_file.index + 2

                    # Process vsdb files
                    elif filename[CN.DATA_FILE_LU_ID] == CN.VSDB_POINT_STAT:

                        # read each line in as 1 column so some fixes can be made
                        one_file = pd.read_csv(filename[CN.FULL_FILE], sep=CN.SEP, header=None)

                        # VSDB file is empty
                        if one_file.empty:
                            logging.warning("!!! Vsdb file %s is empty", filename[CN.FULL_FILE])
                            continue

                        # remove equal sign if present. also solves no space before =
                        one_file.iloc[:, 0] = one_file.iloc[:, 0].str.replace(CN.EQS, ' ')

                        # put a space in front of hyphen between numbers in case space is missing
                        # but FHO can have negative thresh - so fix with regex, only between numbers
                        one_file.iloc[:, 0] = \
                            one_file.iloc[:, 0].str.replace(r'(\d)-(\d)', r'\1 -\2')

                        # break fields out, separated by 1 or more spaces
                        one_file = one_file.iloc[:, 0].str.split(' +', expand=True)

                        # add column names
                        hdr_names = CN.VSDB_HEADER + CN.COL_NUMS
                        one_file.columns = hdr_names[:len(one_file.columns)]

                        # get thresh after > in line type
                        # FHO and FSS in the 6 column have thresh
                        one_file.insert(9, CN.FCST_THRESH, CN.NOTAV)

                        if one_file.iloc[:, 6].str.contains(CN.FHO).any():
                            one_file.loc[one_file.iloc[:, 6].str.contains(CN.FHO),
                                         CN.FCST_THRESH] = \
                                             one_file.loc[one_file.iloc[:, 6].str.contains(CN.FHO),
                                                          CN.LINE_TYPE].str.split(CN.FHO).str[1]
                            # change from the VSDB file type with thresh to Stat file type
                            one_file.loc[one_file.iloc[:, 6].str.contains(CN.FHO),
                                         CN.LINE_TYPE] = CN.CTC

                        if one_file.iloc[:, 6].str.contains(CN.FSS).any():
                            one_file.loc[one_file.iloc[:, 6].str.contains(CN.FSS),
                                         CN.FCST_THRESH] = \
                                             one_file.loc[one_file.iloc[:, 6].str.contains(CN.FSS),
                                                          CN.LINE_TYPE].str.split(CN.FSS).str[1]
                            # change from the VSDB file type with thresh to Stat file type
                            one_file.loc[one_file.iloc[:, 6].str.contains(CN.FSS),
                                         CN.LINE_TYPE] = CN.NBRCNT

                        # for RELI, get number after slash in model, add one, add text to be thresh
                        if one_file.iloc[:, 6].str.contains(CN.RELI).any():
                            one_file.loc[one_file.line_type == CN.RELI,
                                         CN.FCST_THRESH] = \
                                         '==1/' + \
                                         one_file.loc[one_file.line_type == CN.RELI,
                                                      CN.MODEL].str.split('/').str[1].astype(int).add(1).astype(str)

                        # do this after all files collected?
                        # change line types from VSDB to STAT
                        one_file.iloc[:, 6] = \
                            one_file.iloc[:, 6].replace(to_replace=CN.VSDB_LINE_TYPES,
                                                        value=CN.VSDB_TO_STAT_TYPES)

                        # make this VSDB data look like a Met file
                        # can/should this be done to all VSDB files at once?

                        # some need value after / split from model

                        # add description
                        one_file.insert(2, CN.DESCR, CN.NOTAV)
                        # reformat fcst_valid_beg
                        one_file.iloc[:, 4] = pd.to_datetime(one_file.iloc[:, 4], format='%Y%m%d%H')
                        # fcst_valid_end is the same as fcst_valid_beg
                        one_file.loc[:, CN.FCST_VALID_END] = one_file.iloc[:, 4]
                        # fcst_lead must be numeric for later calculations
                        one_file[CN.FCST_LEAD] = pd.to_numeric(one_file[CN.FCST_LEAD])
                        one_file[CN.OBS_LEAD] = 0
                        # copy obs values from fcst values
                        one_file.loc[:, CN.OBS_VALID_BEG] = one_file.iloc[:, 4]
                        one_file.loc[:, CN.OBS_VALID_END] = one_file.iloc[:, 4]
                        one_file.loc[:, CN.OBS_THRESH] = one_file.loc[:, CN.FCST_THRESH]
                        # add units
                        one_file.insert(11, CN.FCST_UNITS, CN.NOTAV)
                        one_file.loc[:, CN.OBS_VAR] = one_file.iloc[:, 8]
                        one_file.insert(12, CN.OBS_UNITS, CN.NOTAV)
                        one_file.loc[:, CN.OBS_LEV] = one_file.iloc[:, 9]
                        # add interp method and interp points with default values
                        one_file.insert(13, CN.INTERP_MTHD, CN.NOTAV)
                        one_file.insert(14, CN.INTERP_PNTS, "0")
                        one_file[CN.ALPHA] = -9999
                        one_file[CN.COV_THRESH] = -9999

                    else:
                        logging.warning("!!! This file type is not handled yet")

                    # re-initialize pandas dataframes before reading next file
                    if not one_file.empty:
                        # initially, match line data to the index of the file names
                        one_file[CN.FILE_ROW] = row_num
                        # keep the dataframes from each file in a list
                        list_frames.append(one_file)
                        logging.debug("Lines in %s: %s", filename[CN.FULL_FILE],
                                      str(len(one_file.index)))
                        one_file = one_file.iloc[0:0]
                        if not file_hdr.empty:
                            file_hdr = file_hdr.iloc[0:0]
                    else:
                        logging.warning("!!! Empty file %s", filename[CN.FULL_FILE])
                        continue
                else:
                    logging.warning("!!! No file %s", filename[CN.FULL_FILE])

            # concatenate all the dataframes - much faster than doing an append each time
            # added sort=False on 10/21/19 because that will be new default behavior
            all_stat = pd.concat(list_frames, ignore_index=True, sort=False)

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_data ***", sys.exc_info()[0])

        logging.debug("Shape of all_stat before: %s", str(all_stat.shape))

        try:
            # delete any lines that have invalid line_types
            invalid_line_indexes = all_stat[~all_stat.line_type.isin(CN.UC_LINE_TYPES)].index

            if not invalid_line_indexes.empty:

                logging.warning("!!! Warning, invalid line_types:")
                logging.warning("line types: %s",
                                str(all_stat.iloc[invalid_line_indexes].line_type))

                all_stat = all_stat.drop(invalid_line_indexes, axis=0)

            # if user specified line types to load, delete the rest
            if load_flags["line_type_load"]:
                all_stat = all_stat.drop(all_stat[~all_stat.line_type.isin(line_types)].index)

            # if load_spec has flag to not load MPR records, delete them
            if not load_flags["load_mpr"]:
                all_stat = all_stat.drop(all_stat[all_stat.line_type == CN.MPR].index)

            # if load_spec has flag to not load ORANK records, delete them
            if not load_flags["load_orank"]:
                all_stat = all_stat.drop(all_stat[all_stat.line_type == CN.ORANK].index)

            # reset the index, in case any lines have been deleted
            all_stat.reset_index(drop=True, inplace=True)

            # all lines from a file may have been deleted. if so, remove filename
            files_to_drop = ~self.data_files.index.isin(all_stat[CN.FILE_ROW])
            self.data_files = \
                self.data_files.drop(self.data_files[files_to_drop].index)

            self.data_files.reset_index(drop=True, inplace=True)

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

            # give a warning message with data if value of alpha for an alpha line type is NA
            alpha_lines = all_stat[(all_stat.line_type.isin(CN.ALPHA_LINE_TYPES)) &
                                   (all_stat.alpha == CN.NOTAV)].line_type
            if not alpha_lines.empty:
                logging.warning("!!! ALPHA line_type has ALPHA value of NA:\r\n %s",
                                str(alpha_lines))

            # give a warning message with data if non-alpha line type has float value
            non_alpha_lines = all_stat[(~all_stat.line_type.isin(CN.ALPHA_LINE_TYPES)) &
                                       (all_stat.alpha != CN.NOTAV)].line_type
            if not non_alpha_lines.empty:
                logging.warning("!!! non-ALPHA line_type has ALPHA float value:\r\n %s",
                                str(non_alpha_lines))

            # Change ALL items in column ALPHA to -9999 if they are 'NA'
            all_stat.loc[all_stat.alpha == CN.NOTAV, CN.ALPHA] = -9999

            # Make ALPHA column into a decimal with no trailing zeroes after the decimal
            all_stat.alpha = all_stat.alpha.astype(float).map('{0:g}'.format)

            # Change ALL items in column COV_THRESH to '-9999' if they are 'NA'
            all_stat.loc[all_stat.cov_thresh == CN.NOTAV, CN.COV_THRESH] = '-9999'

            # Change 'NA' values in column INTERP_PNTS to 0 if present
            if not all_stat.interp_pnts.dtypes == 'int':
                all_stat.loc[all_stat.interp_pnts == CN.NOTAV, CN.INTERP_PNTS] = 0
                all_stat.interp_pnts = all_stat.interp_pnts.astype(int)

            self.stat_data = all_stat

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_data ***", sys.exc_info()[0])

        read_time_end = time.perf_counter()
        read_time = timedelta(seconds=read_time_end - read_time_start)

        logging.info("    >>> Read time: %s", str(read_time))

        logging.debug("[--- End read_data ---]")

    @staticmethod
    def get_lookup(filename):
        """ Given the name of a file, determine its lookup type.
            Returns:
               lookup type, integer, based on data_file_lu table
        """
        lc_filename = filename.lower()
        lu_type = -1

        # Set lookup type from file extensions and the values in the data_file_lu table
        if lc_filename.endswith(".stat"):
            lu_type = CN.STAT
        elif lc_filename.endswith(".vsdb"):
            lu_type = CN.VSDB_POINT_STAT
        elif lc_filename.endswith("cts.txt"):
            lu_type = CN.MODE_CTS
        elif lc_filename.endswith("obj.txt"):
            lu_type = CN.MODE_OBJ
        elif lc_filename.endswith("2d.txt"):
            lu_type = CN.MTD_2D
        elif lc_filename.endswith("3d_pair_cluster.txt"):
            lu_type = CN.MTD_3D_PC
        elif lc_filename.endswith("3d_pair_simple.txt"):
            lu_type = CN.MTD_3D_PS
        elif lc_filename.endswith("3d_single_cluster.txt"):
            lu_type = CN.MTD_3D_SC
        elif lc_filename.endswith("3d_single_simple.txt"):
            lu_type = CN.MTD_3D_SS
        return lu_type

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
