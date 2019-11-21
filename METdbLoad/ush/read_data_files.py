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
        vsdb_file = pd.DataFrame()
        file_hdr = pd.DataFrame()
        all_stat = pd.DataFrame()
        all_vsdb = pd.DataFrame()
        list_frames = []
        list_vsdb = []

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

                    #
                    # Process stat files
                    #
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

                    #
                    # Process vsdb files
                    #
                    elif filename[CN.DATA_FILE_LU_ID] == CN.VSDB_POINT_STAT:

                        # read each line in as 1 column so some fixes can be made
                        vsdb_file = pd.read_csv(filename[CN.FULL_FILE], sep=CN.SEP, header=None)

                        # VSDB file is empty
                        if vsdb_file.empty:
                            logging.warning("!!! Vsdb file %s is empty", filename[CN.FULL_FILE])
                            continue

                        # remove equal sign if present. also solves no space before =
                        vsdb_file.iloc[:, 0] = vsdb_file.iloc[:, 0].str.replace(CN.EQS, ' ')

                        # put a space in front of hyphen between numbers in case space is missing
                        # but FHO can have negative thresh - so fix with regex, only between numbers
                        vsdb_file.iloc[:, 0] = \
                            vsdb_file.iloc[:, 0].str.replace(r'(\d)-(\d)', r'\1 -\2')

                        # break fields out, separated by 1 or more spaces
                        vsdb_file = vsdb_file.iloc[:, 0].str.split(' +', expand=True)

                        # add column names
                        hdr_names = CN.VSDB_HEADER + CN.COL_NUMS
                        vsdb_file.columns = hdr_names[:len(vsdb_file.columns)]

                        # add line numbers, starting at 1
                        vsdb_file[CN.LINE_NUM] = vsdb_file.index + 1

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
                    elif not vsdb_file.empty:
                        vsdb_file[CN.FILE_ROW] = row_num
                        list_vsdb.append(vsdb_file)
                        logging.debug("Lines in %s: %s", filename[CN.FULL_FILE],
                                      str(len(vsdb_file.index)))
                        vsdb_file = vsdb_file.iloc[0:0]
                    else:
                        logging.warning("!!! Empty file %s", filename[CN.FULL_FILE])
                        continue
                else:
                    logging.warning("!!! No file %s", filename[CN.FULL_FILE])

            # end for row_num, filename

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_data upper ***", sys.exc_info()[0])

        try:

            # concatenate all the dataframes - much faster than doing an append each time
            # added sort=False on 10/21/19 because that will be new default behavior
            if list_frames:
                all_stat = pd.concat(list_frames, ignore_index=True, sort=False)

                # These warnings and transforms only apply to stat files
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

                # Change ALL items in column ALPHA to '-9999' if they are 'NA'
                all_stat.loc[all_stat.alpha == CN.NOTAV, CN.ALPHA] = CN.MV_NOTAV

                # Make ALPHA column into a decimal with no trailing zeroes after the decimal
                all_stat.alpha = all_stat.alpha.astype(float).map('{0:g}'.format)

                # Change ALL items in column COV_THRESH to '-9999' if they are 'NA'
                all_stat.loc[all_stat.cov_thresh == CN.NOTAV, CN.COV_THRESH] = CN.MV_NOTAV

                # Change 'NA' values in column INTERP_PNTS to 0 if present
                if not all_stat.interp_pnts.dtypes == 'int':
                    all_stat.loc[all_stat.interp_pnts == CN.NOTAV, CN.INTERP_PNTS] = 0
                    all_stat.interp_pnts = all_stat.interp_pnts.astype(int)

            # collect vsdb files separately so additional transforms can be done
            if list_vsdb:
                all_vsdb = pd.concat(list_vsdb, ignore_index=True, sort=False)
                list_vsdb = []

                # Make VSDB files look like stat files
                # get thresh starting with > in line_type
                # FHO and FSS in the 6 column have thresh
                all_vsdb.insert(9, CN.FCST_THRESH, CN.NOTAV)

                if all_vsdb.line_type.str.startswith('F').any():
                    all_vsdb.loc[all_vsdb.line_type.str.startswith('F'),
                                 CN.FCST_THRESH] = \
                        all_vsdb.loc[all_vsdb.line_type.str.startswith('F'),
                                     CN.LINE_TYPE].str[3:]
                    # remove the thresh value from the line type
                    all_vsdb.loc[all_vsdb.line_type.str.startswith('F'),
                                 CN.LINE_TYPE] = \
                        all_vsdb.loc[all_vsdb.line_type.str.startswith('F'),
                                     CN.LINE_TYPE].str[0:3]

                # handle model names that contain a forward slash followed by a number
                if all_vsdb.model.str.contains(CN.FWD_SLASH).any():
                    all_vsdb.loc[:, CN.N_VAR] = 0
                    # save the value after the slash in model
                    all_vsdb.loc[all_vsdb.model.str.contains(CN.FWD_SLASH),
                                 CN.N_VAR] = \
                        all_vsdb.loc[all_vsdb.model.str.contains(CN.FWD_SLASH),
                                     CN.MODEL].str.split(CN.FWD_SLASH).str[1].astype(int)

                    # remove the slash and value from model
                    all_vsdb.loc[all_vsdb.model.str.contains(CN.FWD_SLASH),
                                 CN.MODEL] = \
                        all_vsdb.loc[all_vsdb.model.str.contains(CN.FWD_SLASH),
                                     CN.MODEL].str.split(CN.FWD_SLASH).str[0]

                    # for RELI/PCT, get number after slash in model, add one,
                    # prefix with string and put in thresh
                    if CN.RELI in all_vsdb.line_type.values:
                        all_vsdb.loc[all_vsdb.line_type ==
                                     CN.RELI,
                                     CN.N_VAR] = \
                            all_vsdb.loc[all_vsdb.line_type ==
                                         CN.RELI,
                                         CN.N_VAR] + 1
                        # RELI/PCT also uses this number in the threshold
                        all_vsdb.loc[all_vsdb.line_type ==
                                     CN.RELI,
                                     CN.FCST_THRESH] = \
                            '==1/' + \
                            all_vsdb.loc[all_vsdb.line_type ==
                                         CN.RELI,
                                         CN.N_VAR].astype(str)

                    # HIST/RHIST also adds one
                    if CN.HIST in all_vsdb.line_type.values:
                        all_vsdb.loc[all_vsdb.line_type ==
                                     CN.HIST,
                                     CN.N_VAR] = \
                            all_vsdb.loc[all_vsdb.line_type ==
                                         CN.HIST,
                                         CN.N_VAR] + 1

                    # ECON/ECLV use a default of 18
                    if CN.ECON in all_vsdb.line_type.values:
                        all_vsdb.loc[all_vsdb.line_type ==
                                     CN.ECON,
                                     CN.N_VAR] = 18

                # change from VSDB line types to STAT line types
                all_vsdb.line_type = \
                    all_vsdb.line_type.replace(to_replace=CN.OLD_VSDB_LINE_TYPES,
                                               value=CN.VSDB_TO_STAT_TYPES)

                # add columns to make these VSDB files look more like Met stat files

                # add description
                all_vsdb.insert(2, CN.DESCR, CN.NOTAV)
                # reformat fcst_valid_beg
                all_vsdb.fcst_valid_beg = pd.to_datetime(all_vsdb.fcst_valid_beg,
                                                         format='%Y%m%d%H')
                # fcst_valid_end is the same as fcst_valid_beg
                all_vsdb.loc[:, CN.FCST_VALID_END] = all_vsdb.fcst_valid_beg
                # fcst_lead must be numeric for later calculations
                all_vsdb.fcst_lead = pd.to_numeric(all_vsdb.fcst_lead)
                all_vsdb.insert(11, CN.OBS_LEAD, 0)
                # copy obs values from fcst values
                all_vsdb.loc[:, CN.OBS_VALID_BEG] = all_vsdb.fcst_valid_beg
                all_vsdb.loc[:, CN.OBS_VALID_END] = all_vsdb.fcst_valid_beg
                all_vsdb.loc[:, CN.OBS_VAR] = all_vsdb.fcst_var
                all_vsdb.loc[:, CN.OBS_LEV] = all_vsdb.fcst_lev
                all_vsdb.loc[:, CN.OBS_THRESH] = all_vsdb.fcst_thresh
                # add units
                all_vsdb.insert(12, CN.FCST_UNITS, CN.NOTAV)
                all_vsdb.insert(13, CN.OBS_UNITS, CN.NOTAV)
                # add interp method and interp points with default values
                all_vsdb.insert(14, CN.INTERP_MTHD, CN.NOTAV)
                all_vsdb.insert(15, CN.INTERP_PNTS, "0")
                # add alpha and cov_thresh
                all_vsdb.insert(16, CN.ALPHA, CN.NOTAV)
                all_vsdb.insert(17, CN.COV_THRESH, CN.NOTAV)
                # add total column with default of zero
                all_vsdb.insert(18, CN.TOTAL_LC, "0")

                all_vsdb[CN.COL_NA] = CN.MV_NOTAV
                all_vsdb[CN.COL_ZERO] = "0"

                # find all of the line types in the data
                vsdb_types = all_vsdb.line_type.unique()

                for vsdb_type in vsdb_types:
                    # get the line data of just this VSDB type and re-index
                    vsdb_data = all_vsdb[all_vsdb[CN.LINE_TYPE] == vsdb_type].copy()
                    vsdb_data.reset_index(drop=True, inplace=True)

                    if vsdb_type in (CN.SL1L2, CN.SAL1L2):
                        one_file = vsdb_data[CN.LONG_HEADER + CN.COL_NUMS[:7] +
                                             CN.COL_NAS[:89] + [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type in (CN.VL1L2, CN.GRAD):
                        one_file = vsdb_data[CN.LONG_HEADER + CN.COL_NUMS[:10] +
                                             CN.COL_NAS[:86] + [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type == CN.VAL1L2:
                        one_file = vsdb_data[CN.LONG_HEADER + CN.COL_NUMS[:8] +
                                             CN.COL_NAS[:88] + [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type in (CN.RHIST, CN.RELP, CN.PCT):
                        one_file = vsdb_data[CN.LONG_HEADER + [CN.TOTAL_LC, CN.N_VAR] +
                                             CN.COL_NAS[:94] + [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type == CN.ECLV:
                        one_file = vsdb_data[CN.LONG_HEADER + [CN.TOTAL_LC] +
                                             CN.COL_NAS[:2] + [CN.N_VAR] +
                                             CN.COL_NAS[:92] + [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type == CN.PSTD:
                        one_file = vsdb_data[CN.LONG_HEADER + [CN.TOTAL_LC] +
                                             [CN.COL_ZERO] + CN.COL_NAS[:3] +
                                             ['3', '4', '5'] + CN.COL_NAS[:1] +
                                             ['0'] + CN.COL_NAS[:2] +
                                             ['1'] + CN.COL_NAS[:2] +
                                             ['2'] + CN.COL_NAS[:1] +
                                             CN.COL_NAS[:79] + [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type == CN.CNT:
                        one_file = vsdb_data[CN.LONG_HEADER + [CN.TOTAL_LC] +
                                             CN.COL_NAS[:28] +
                                             [CN.COL_ZERO, CN.COL_ZERO, CN.COL_ZERO] +
                                             ['2'] + CN.COL_NAS[:4] +
                                             ['0'] + CN.COL_NAS[:7] +
                                             ['3'] + CN.COL_NAS[:8] +
                                             ['1'] + CN.COL_NAS[:23] +
                                             ['4'] + CN.COL_NAS[:17] + [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type == CN.ENSCNT:
                        one_file = vsdb_data[CN.LONG_HEADER + [CN.TOTAL_LC] +
                                             CN.COL_NAS[:4] +
                                             ['1'] + CN.COL_NAS[:4] +
                                             ['2'] + CN.COL_NAS[:4] +
                                             ['3'] + CN.COL_NAS[:4] +
                                             ['4'] + CN.COL_NAS[:4] +
                                             ['5'] + CN.COL_NAS[:70] + [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type == CN.CTC:
                        # column 0 is Total, 1 is F, 2 is H
                        # column 3 is O (Oh) - if None, set to 0 (Zero)
                        vsdb_data.loc[vsdb_data['3'].isnull(), '3'] = 0
                        # fy = Total * F
                        vsdb_data['4'] = vsdb_data['0'].astype(float) * vsdb_data['1'].astype(float)
                        # oy = Total * O
                        vsdb_data['5'] = vsdb_data['0'].astype(float) * vsdb_data['3'].astype(float)
                        # fy_oy = Total * H
                        vsdb_data['6'] = vsdb_data['0'].astype(float) * vsdb_data['2'].astype(float)
                        # fy_on = fy - fy_oy
                        vsdb_data['7'] = vsdb_data['4'].astype(float) - vsdb_data['6'].astype(float)
                        # fn_oy = oy - fy_oy
                        vsdb_data['8'] = vsdb_data['5'].astype(float) - vsdb_data['6'].astype(float)
                        # fn_on = Total - fy - oy + fy_oy
                        vsdb_data['9'] = (vsdb_data['0'].astype(float) -
                                          vsdb_data['4'].astype(float) -
                                          vsdb_data['5'].astype(float) +
                                          vsdb_data['6'].astype(float))
                        one_file = vsdb_data[CN.LONG_HEADER +
                                             ['0'] + ['6', '7', '8', '9'] +
                                             CN.COL_NAS[:91] + [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type == CN.NBRCNT:
                        # fss is calculated from the other columns
                        vsdb_data['4'] = (1 - vsdb_data['1'].astype(float) /
                                          vsdb_data['2'].astype(float) +
                                          vsdb_data['3'].astype(float))
                        one_file = vsdb_data[CN.LONG_HEADER +
                                             ['0', '1'] + CN.COL_NAS[:2] + ['4'] +
                                             CN.COL_NAS[:91] + [CN.LINE_NUM, CN.FILE_ROW]]

                    # rename columns
                    if not one_file.empty:
                        one_file.columns = CN.LONG_HEADER + \
                                           CN.COL_NUMS[:96] + [CN.LINE_NUM, CN.FILE_ROW]
                        list_vsdb.append(one_file)
                        one_file = one_file.iloc[0:0]

                # end for vsdb_type

                # Clear out all_vsdb, which we copied from above, line_type by line_type
                all_vsdb = pd.DataFrame()
                # combine stat and vsdb
                all_vsdb = pd.concat(list_vsdb, ignore_index=True, sort=False)
                all_stat = pd.concat([all_stat, all_vsdb], ignore_index=True, sort=False)

            logging.debug("Shape of all_stat before transforms: %s", str(all_stat.shape))

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_data middle ***", sys.exc_info()[0])

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

            logging.debug("Shape of all_stat after transforms: %s", str(all_stat.shape))

            self.stat_data = all_stat

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_data near end ***", sys.exc_info()[0])

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
        if date_str.startswith('F') or date_str.startswith('O'):
            return pd.to_datetime('20000101_000000', format='%Y%m%d_%H%M%S')
        date_time = pd.to_datetime(date_str, format='%Y%m%d_%H%M%S')
        self.cache[date_str] = date_time
        return date_time
