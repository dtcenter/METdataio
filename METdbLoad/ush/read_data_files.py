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
Copyright 2020 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
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
        self.mode_cts_data = pd.DataFrame()
        self.mode_obj_data = pd.DataFrame()
        self.tcst_data = pd.DataFrame()
        self.data_files = pd.DataFrame()
        self.mtd_2d_data = pd.DataFrame()
        self.mtd_3d_single_data = pd.DataFrame()
        self.mtd_3d_pair_data = pd.DataFrame()

    def read_data(self, load_flags, load_files, line_types):
        """ Read in data files as given in load_spec file.
            Returns:
               N/A
        """

        logging.debug("[--- Start read_data ---]")

        read_time_start = time.perf_counter()

        # handle MET files, VSDB files, MODE files, MTD files, TCST files

        # speed up with dask delayed?

        one_file = pd.DataFrame()
        vsdb_file = pd.DataFrame()
        mode_file = pd.DataFrame()
        tcst_file = pd.DataFrame()
        mtd_file = pd.DataFrame()
        file_hdr = pd.DataFrame()
        all_stat = pd.DataFrame()
        all_vsdb = pd.DataFrame()
        all_cts = pd.DataFrame()
        all_obj = pd.DataFrame()
        all_tcst = pd.DataFrame()
        all_2d = pd.DataFrame()
        all_single = pd.DataFrame()
        all_pair = pd.DataFrame()
        list_frames = []
        list_vsdb = []
        list_cts = []
        list_obj = []
        list_tcst = []
        list_2d = []
        list_single = []
        list_pair = []

        # keep track of each set of revisions
        rev_ctr = 0

        try:

            # Put the list of files into a dataframe to collect info to write to database
            self.data_files[CN.FULL_FILE] = load_files
            # Add the code that describes what kind of file this is - stat, vsdb, etc
            self.data_files[CN.DATA_FILE_LU_ID] = \
                np.vectorize(self.get_lookup)(self.data_files[CN.FULL_FILE])
                
            # Drop files that are not of a valid type
            self.data_files.drop(self.data_files[self.data_files[CN.DATA_FILE_LU_ID] ==
                                                 CN.NO_KEY].index, inplace=True)
            self.data_files.reset_index(drop=True, inplace=True)
            # Won't know database key until we interact with the database, so no keys yet
            self.data_files[CN.DATA_FILE_ID] = CN.NO_KEY
            # Store the index in a column to make later merging with stat data easier
            self.data_files[CN.FILE_ROW] = self.data_files.index
            # Break the full file name into path and filename
            self.data_files[CN.FILEPATH] = \
                self.data_files[CN.FULL_FILE].str.rpartition(CN.FWD_SLASH)[0]
            self.data_files[CN.FILENAME] = \
                self.data_files[CN.FULL_FILE].str.rpartition(CN.FWD_SLASH)[2]
            # current date and time for load date
            self.data_files[CN.LOAD_DATE] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.data_files[CN.MOD_DATE] = None

            # Check to make sure files exist
            for row in self.data_files.itertuples(name=None):

                row_num = row[0]
                filename = row[1]
                lu_id = row[2]
                filepath = row[5]

                # Read in each file. Add columns if needed. Append to all_stat dataframe.
                file_and_path = Path(filename)

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
                    if lu_id == CN.STAT:
                        # Get the first line of the .stat file that has the headers
                        file_hdr = pd.read_csv(filename, delim_whitespace=True,
                                               header=None, nrows=1)

                        # MET file has no headers or no text - it's empty
                        if file_hdr.empty or stat_info.st_size == 0:
                            logging.warning("!!! Stat file %s is empty", filename)
                            continue

                        # Add a DESC column if the data file does not have one
                        if not file_hdr.iloc[0].str.contains(CN.UC_DESC).any():
                            hdr_names = CN.SHORT_HEADER + CN.COL_NUMS
                            one_file = self.read_stat(filename, hdr_names)

                            # If the file has no DESC column, add UNITS as well
                            one_file.insert(2, CN.DESCR, CN.NOTAV)
                            one_file.insert(10, CN.FCST_UNITS, CN.NOTAV)
                            one_file.insert(13, CN.OBS_UNITS, CN.NOTAV)

                        # If the file has a DESC column, but no UNITS columns
                        elif not file_hdr.iloc[0].str.contains(CN.UC_FCST_UNITS).any():
                            hdr_names = CN.MID_HEADER + CN.COL_NUMS
                            one_file = self.read_stat(filename, hdr_names)

                            one_file.insert(10, CN.FCST_UNITS, CN.NOTAV)
                            one_file.insert(13, CN.OBS_UNITS, CN.NOTAV)

                        else:
                            hdr_names = CN.LONG_HEADER + CN.COL_NUMS
                            one_file = self.read_stat(filename, hdr_names)

                        # add line numbers and count the header line, for stat files
                        one_file[CN.LINE_NUM] = one_file.index + 2

                        # add columns for fcst_perc and obs_perc
                        # these can be in parens in fcst_thresh and obs_thresh in stat files
                        one_file[CN.FCST_PERC] = CN.MV_NOTAV
                        one_file[CN.OBS_PERC] = CN.MV_NOTAV

                    #
                    # Process vsdb files
                    #
                    elif lu_id == CN.VSDB_POINT_STAT:

                        # check whether vsdb file is empty
                        if stat_info.st_size == 0:
                            logging.warning("!!! Vsdb file %s is empty", filename)
                            continue

                        # read each line in as 1 column so some fixes can be made
                        vsdb_file = pd.read_csv(filename, sep=CN.SEP, header=None)

                        if vsdb_file.iloc[:, 0].str.contains('=').any():

                            # split vsdb data into 2 columns - before the =, and after
                            # this protects from changing weird variable names, and removes =
                            split_file = vsdb_file.iloc[:, 0].str.split('=', expand=True)

                            # put space in front of hyphen between numbers in case space is missing
                            # FHO can have negative thresh - fix with regex, only between numbers
                            split_file.iloc[:, 1] = \
                                split_file.iloc[:, 1].str.replace(r'(\d)-(\d)', r'\1 -\2',
                                                                  regex=True)

                            # merge the two halves together again
                            vsdb_file = split_file.iloc[:, 0] + ' ' + split_file.iloc[:, 1]

                        else:
                            vsdb_file = vsdb_file.iloc[:, 0]

                        # break fields out, separated by 1 or more spaces
                        vsdb_file = vsdb_file.str.split(' +', expand=True)

                        # add column names
                        hdr_names = CN.VSDB_HEADER + CN.COL_NUMS
                        vsdb_file.columns = hdr_names[:len(vsdb_file.columns)]

                        # add line numbers, starting at 1
                        vsdb_file.insert(9, CN.LINE_NUM, vsdb_file.index + 1)

                        # some line types need a piece of the path added to the model name
                        # if last part of path contains an underscore, save string after it.
                        # then add it to model name
                        last_slash = filepath.rfind(CN.FWD_SLASH)
                        last_und = filepath.rfind('_')
                        ens_value = ''
                        if last_und > last_slash:
                            ens_value = filepath[last_und:]
                        if not vsdb_file.loc[vsdb_file.line_type.isin(CN.ENS_VSDB_LINE_TYPES),
                                             CN.MODEL].empty:
                            vsdb_file.loc[vsdb_file.line_type.isin(CN.ENS_VSDB_LINE_TYPES),
                                          CN.MODEL] = \
                                vsdb_file.loc[vsdb_file.line_type.isin(CN.ENS_VSDB_LINE_TYPES),
                                              CN.MODEL].str.split(CN.FWD_SLASH).str[0] + \
                                ens_value + CN.FWD_SLASH + \
                                vsdb_file.loc[vsdb_file.line_type.isin(CN.ENS_VSDB_LINE_TYPES),
                                              CN.MODEL].str.split(CN.FWD_SLASH).str[1]

                    #
                    # Process mode files
                    #
                    elif lu_id in (CN.MODE_CTS, CN.MODE_OBJ):
                        
                        # Get the first line of the mode cts or obj file that has the headers
                        file_hdr = pd.read_csv(filename, delim_whitespace=True,
                                               nrows=1)

                        # MODE file has no headers or no text - it's empty
                        if file_hdr.empty or stat_info.st_size == 0:
                            logging.warning("!!! Mode file %s is empty", filename)
                            continue

                        # use lower case of headers in file as column names
                        hdr_names = file_hdr.columns.tolist()
                        hdr_names = [hdr.lower() for hdr in hdr_names]

                        # change field name after intensity_90 to be intensity_nn
                        if CN.INTENSITY_90 in hdr_names:
                            hdr_names[hdr_names.index(CN.INTENSITY_90) + 1] = CN.INTENSITY_NN

                        # read the file
                        mode_file = self.read_mode(filename, hdr_names)

                        # add line numbers and count the header line, for mode files
                        mode_file[CN.LINENUMBER] = mode_file.index + 2

                        # add other fields if not present in file
                        if CN.N_VALID not in hdr_names:
                            mode_file.insert(2, CN.N_VALID, CN.MV_NULL)
                        if CN.GRID_RES not in hdr_names:
                            mode_file.insert(3, CN.GRID_RES, CN.MV_NULL)
                        if CN.DESCR not in hdr_names:
                            mode_file.insert(4, CN.DESCR, CN.NOTAV)

                        if CN.ASPECT_DIFF not in hdr_names:
                            mode_file[CN.ASPECT_DIFF] = CN.MV_NOTAV

                        if CN.CURV_RATIO not in hdr_names:
                            mode_file[CN.CURV_RATIO] = CN.MV_NOTAV

                        # add units if input file does not have them
                        if CN.FCST_UNITS not in hdr_names:
                            mode_file.insert(16, CN.FCST_UNITS, CN.NOTAV)
                            mode_file.insert(19, CN.OBS_UNITS, CN.NOTAV)

                        # if FCST_LEAD is NA, set it to 0
                        if not mode_file.fcst_lead.dtypes == 'int':
                            mode_file.loc[mode_file.fcst_lead == CN.NOTAV, CN.FCST_LEAD] = 0

                        # initially, match line data to the index of the file names
                        mode_file[CN.FILE_ROW] = row_num

                        # determine which types of records are in the file
                        if lu_id == CN.MODE_CTS:
                            # mode_cts
                            list_cts.append(mode_file)
                        # both single and pair data can be in the same files
                        else:
                            list_obj.append(mode_file)
                    #
                    # Process TCST files
                    #
                    elif lu_id == CN.TCST:
                        # Get the first line of the .tcst file that has the headers
                        file_hdr = pd.read_csv(filename, delim_whitespace=True,
                                               header=None, nrows=1)
                        # TCST file has no headers or no text - it's empty
                        if file_hdr.empty or stat_info.st_size == 0:
                            logging.warning("!!! TCST file %s is empty", filename)
                            continue
                        # Add a DESC column if the data file does not have one
                        if not file_hdr.iloc[0].str.contains(CN.UC_DESC).any():
                            hdr_names = CN.SHORT_HEADER_TCST + CN.COL_NUMS
                            tcst_file = self.read_tcst(filename, hdr_names)
                            tcst_file.insert(3, CN.DESCR, CN.NOTAV)
                        else:
                            hdr_names = CN.LONG_HEADER_TCST + CN.COL_NUMS
                            tcst_file = self.read_tcst(filename, hdr_names)

                        # add line numbers and count the header line, for tcst files
                        tcst_file[CN.LINE_NUM] = tcst_file.index + 2

                        tcst_file = tcst_file.rename(columns={"init": "fcst_init",
                                                              "lead": "fcst_lead",
                                                              "valid": "fcst_valid"})

                    elif lu_id in CN.MTD_FILES:

                        # Get the first line of the MTD file that has the headers
                        file_hdr = pd.read_csv(filename, delim_whitespace=True,
                                               nrows=1)

                        # MTD file has no headers or no text - it's empty
                        if file_hdr.empty or stat_info.st_size == 0:
                            logging.warning("!!! MTD file %s is empty", filename)
                            continue

                        # use lower case of headers in file as column names
                        hdr_names = file_hdr.columns.tolist()
                        hdr_names = [hdr.lower() for hdr in hdr_names]

                        # MET output uses desc, mysql uses descr
                        hdr_names[2] = CN.DESCR

                        # read the MTD file the same way as a mode file
                        mtd_file = self.read_mode(filename, hdr_names)

                        # change field name after intensity_90 to be intensity_nn
                        if CN.INTENSITY_90 in mtd_file:
                            inten_col = mtd_file.columns.get_loc(CN.INTENSITY_90)
                            # if intensity_90 is the last column, add a column
                            if inten_col == len(mtd_file.columns) - 1:
                                mtd_file[CN.INTENSITY_NN] = CN.MV_NOTAV
                            else:
                                mtd_file = mtd_file.rename(columns={mtd_file.columns[inten_col + 1]:
                                                           CN.INTENSITY_NN})

                        # add a column for the revision_id
                        mtd_file[CN.REVISION_ID] = CN.MV_NULL

                        # add line numbers and count the header line, for MTD files
                        mtd_file[CN.LINENUMBER] = mtd_file.index + 2

                        # add other fields if not present in file
                        if CN.FCST_T_BEG not in hdr_names:
                            mtd_file.insert(8, CN.FCST_T_BEG, CN.MV_NULL)
                        if CN.FCST_T_END not in hdr_names:
                            mtd_file.insert(9, CN.FCST_T_END, CN.MV_NULL)
                        if CN.OBS_T_BEG not in hdr_names:
                            mtd_file.insert(12, CN.OBS_T_BEG, CN.MV_NULL)
                        if CN.OBS_T_END not in hdr_names:
                            mtd_file.insert(13, CN.OBS_T_END, CN.MV_NULL)

                        # add units if input file does not have them
                        if CN.FCST_UNITS not in hdr_names:
                            mtd_file.insert(17, CN.FCST_UNITS, CN.NOTAV)
                            mtd_file.insert(20, CN.OBS_UNITS, CN.NOTAV)

                        # if FCST_LEAD is NA, set it to 0 to do math
                        if not mtd_file.fcst_lead.dtypes == 'int':
                            mtd_file.loc[mtd_file.fcst_lead == CN.NOTAV, CN.FCST_LEAD] = 0

                        # Copy forecast lead times, without trailing 0000 if they have them
                        mtd_file[CN.FCST_LEAD_HR] = \
                            np.where(mtd_file[CN.FCST_LEAD] > 9999,
                                     mtd_file[CN.FCST_LEAD] // 10000,
                                     mtd_file[CN.FCST_LEAD])

                        # Calculate fcst_init = fcst_valid - fcst_lead hours
                        mtd_file.insert(5, CN.FCST_INIT, CN.NOTAV)
                        mtd_file[CN.FCST_INIT] = mtd_file[CN.FCST_VALID] - \
                            pd.to_timedelta(mtd_file[CN.FCST_LEAD_HR], unit='h')

                        # Where fcst_lead was set to zero for math, set it to -9999
                        if mtd_file[CN.FCST_LEAD].eq(0).any():
                            mtd_file.loc[mtd_file.fcst_lead == 0, CN.FCST_LEAD] = CN.MV_NOTAV

                        # if OBS_LEAD is NA, set it to -9999
                        if not mtd_file.obs_lead.dtypes == 'int':
                            mtd_file.loc[mtd_file.obs_lead == CN.NOTAV, CN.OBS_LEAD] = CN.MV_NOTAV

                        # initially, match line data to the index of the file names
                        mtd_file[CN.FILE_ROW] = row_num

                        # determine which types of records are in the file
                        if lu_id in (CN.MTD_3D_SS, CN.MTD_3D_SC):
                            # MTD single
                            list_single.append(mtd_file)
                        elif lu_id in (CN.MTD_3D_PS, CN.MTD_3D_PC):
                            # MTD pair
                            list_pair.append(mtd_file)
                        # MTD 2D
                        else:
                            # This is an MTD 2D Revision file if 10 columns each have a single value
                            mtd_rev = True
                            for mtd_col in CN.MTD_2D_REV_FIELDS:
                                if not (mtd_file[mtd_col] == mtd_file[mtd_col][0]).all():
                                    mtd_rev = False
                            if mtd_rev:
                                rev_lines = []
                                obj_id = 'new'
                                obj_ct = 1
                                # Create new rows by subtracting a previous row from a row by object
                                # Unique sequential id is assigned to items with the same object id
                                # Only object ids with more than 2 lines count and create lines
                                for row_num, mtd_row in mtd_file.iterrows():
                                    if mtd_row[CN.OBJECT_ID] == obj_id:
                                        obj_ct += 1
                                        if obj_ct == 3:
                                            rev_ctr += 1
                                        if obj_ct > 2:
                                            new_line = mtd_file.iloc[row_num - 1].to_dict()
                                            new_line[CN.FCST_VAR] = 'REV_' + new_line[CN.FCST_VAR]
                                            new_line[CN.OBS_VAR] = 'REV_' + new_line[CN.OBS_VAR]
                                            new_line[CN.AREA] -= mtd_file[CN.AREA][row_num - 2]
                                            new_line[CN.CENTROID_X] -= \
                                                mtd_file[CN.CENTROID_X][row_num - 2]
                                            new_line[CN.CENTROID_Y] -= \
                                                mtd_file[CN.CENTROID_Y][row_num - 2]
                                            new_line[CN.CENTROID_LAT] -= \
                                                mtd_file[CN.CENTROID_LAT][row_num - 2]
                                            new_line[CN.CENTROID_LON] -= \
                                                mtd_file[CN.CENTROID_LON][row_num - 2]
                                            new_line[CN.AXIS_ANG] = CN.MV_NOTAV
                                            new_line[CN.INTENSITY_10] -= \
                                                mtd_file[CN.INTENSITY_10][row_num - 2]
                                            new_line[CN.INTENSITY_25] -= \
                                                mtd_file[CN.INTENSITY_25][row_num - 2]
                                            new_line[CN.INTENSITY_50] -= \
                                                mtd_file[CN.INTENSITY_50][row_num - 2]
                                            new_line[CN.INTENSITY_75] -= \
                                                mtd_file[CN.INTENSITY_75][row_num - 2]
                                            new_line[CN.INTENSITY_90] -= \
                                                mtd_file[CN.INTENSITY_90][row_num - 2]
                                            new_line[CN.REVISION_ID] = rev_ctr
                                            new_line[CN.LINENUMBER] = 0
                                            rev_lines.append(new_line)
                                    else:
                                        obj_id = mtd_row[CN.OBJECT_ID]
                                        obj_ct = 1
                                rev_df = pd.DataFrame(rev_lines)
                                mtd_file = pd.concat([mtd_file, rev_df], ignore_index=True,
                                                     sort=False)
                                rev_df = rev_df.iloc[0:0]
                            # concat new rows with mtd_file
                            list_2d.append(mtd_file)

                    else:
                        logging.warning("!!! File type of %s not valid", filename)

                    # re-initialize pandas dataframes before reading next file
                    if not one_file.empty:
                        # initially, match line data to the index of the file names
                        one_file[CN.FILE_ROW] = row_num
                        # keep the dataframes from each file in a list
                        list_frames.append(one_file)
                        logging.debug("Lines in %s: %s", filename,
                                      str(len(one_file.index)))
                        one_file = one_file.iloc[0:0]
                        if not file_hdr.empty:
                            file_hdr = file_hdr.iloc[0:0]
                    elif not vsdb_file.empty:
                        vsdb_file.insert(10, CN.FILE_ROW, row_num)
                        list_vsdb.append(vsdb_file)
                        logging.debug("Lines in %s: %s", filename,
                                      str(len(vsdb_file.index)))
                        vsdb_file = vsdb_file.iloc[0:0]
                    elif not mode_file.empty:
                        logging.debug("Lines in %s: %s", filename,
                                      str(len(mode_file.index)))
                        mode_file = mode_file.iloc[0:0]
                        if not file_hdr.empty:
                            file_hdr = file_hdr.iloc[0:0]
                    elif not tcst_file.empty:
                        # initially, match line data to the index of the file names
                        tcst_file[CN.FILE_ROW] = row_num
                        # keep the dataframes from each file in a list
                        list_tcst.append(tcst_file)
                        logging.debug("Lines in %s: %s", filename,
                                      str(len(one_file.index)))
                        tcst_file = tcst_file.iloc[0:0]
                        if not file_hdr.empty:
                            file_hdr = file_hdr.iloc[0:0]
                    elif not mtd_file.empty:
                        logging.debug("Lines in %s: %s", filename,
                                      str(len(mtd_file.index)))
                        mtd_file = mtd_file.iloc[0:0]
                        if not file_hdr.empty:
                            file_hdr = file_hdr.iloc[0:0]
                    else:
                        logging.warning("!!! Empty file %s", filename)
                        continue
                else:
                    logging.warning("!!! No file %s", filename)
                    sys.exit("*** No file " + filename)

            # end for row

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_data upper ***", sys.exc_info()[0])

        try:

            # concatenate all the dataframes - much faster than doing an append each time
            # added sort=False on 10/21/19 because that will be new default behavior
            if list_frames:
                all_stat = pd.concat(list_frames, ignore_index=True, sort=False)
                list_frames = []

                # if a fcst percentage thresh is used, it is in parens in fcst_thresh
                if all_stat.fcst_thresh.str.contains(CN.L_PAREN, regex=False).any():
                    # save the value in parens
                    all_stat.loc[all_stat.fcst_thresh.str.contains(CN.L_PAREN, regex=False) &
                                 all_stat.fcst_thresh.str.contains(CN.R_PAREN, regex=False),
                                 CN.FCST_PERC] = \
                        all_stat.loc[all_stat.fcst_thresh.str.contains(CN.L_PAREN, regex=False) &
                                     all_stat.fcst_thresh.str.contains(CN.R_PAREN, regex=False),
                                     CN.FCST_THRESH].str.split(CN.L_PAREN).str[1]. \
                        str.split(CN.R_PAREN).str[0].astype(float)
                    # remove the percentage from fcst_thresh
                    all_stat.loc[all_stat.fcst_thresh.str.contains(CN.L_PAREN, regex=False) &
                                 all_stat.fcst_thresh.str.contains(CN.R_PAREN, regex=False),
                                 CN.FCST_THRESH] = \
                        all_stat.loc[all_stat.fcst_thresh.str.contains(CN.L_PAREN, regex=False) &
                                     all_stat.fcst_thresh.str.contains(CN.R_PAREN, regex=False),
                                     CN.FCST_THRESH].str.split(CN.L_PAREN).str[0]

                # if an obs percentage thresh is used, it is in parens in obs_thresh
                if all_stat.obs_thresh.str.contains(CN.L_PAREN, regex=False).any():
                    # save the value in parens
                    all_stat.loc[all_stat.obs_thresh.str.contains(CN.L_PAREN, regex=False) &
                                 all_stat.obs_thresh.str.contains(CN.R_PAREN, regex=False),
                                 CN.OBS_PERC] = \
                        all_stat.loc[all_stat.obs_thresh.str.contains(CN.L_PAREN, regex=False) &
                                     all_stat.obs_thresh.str.contains(CN.R_PAREN, regex=False),
                                     CN.OBS_THRESH].str.split(CN.L_PAREN).str[1]. \
                        str.split(CN.R_PAREN).str[0].astype(float)
                    all_stat.loc[all_stat.obs_thresh.str.contains(CN.L_PAREN, regex=False) &
                                 all_stat.obs_thresh.str.contains(CN.R_PAREN, regex=False),
                                 CN.OBS_THRESH] = \
                        all_stat.loc[all_stat.obs_thresh.str.contains(CN.L_PAREN, regex=False) &
                                     all_stat.obs_thresh.str.contains(CN.R_PAREN, regex=False),
                                     CN.OBS_THRESH].str.split(CN.L_PAREN).str[0]

                # These warnings and transforms only apply to stat files
                # Give a warning message with data if value of alpha for an alpha line type is NA
                # Do not check CNT and PSTD, even though they are alpha line types
                alpha_lines = all_stat[(all_stat.line_type.isin(CN.ALPHA_LINE_TYPES[:-2])) &
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

                # Change ALL items in column FCST_LEAD to 0 if they are 'NA'
                # Added for tc_gen files
                if not all_stat.fcst_lead.dtypes == 'int':
                    all_stat.loc[all_stat.fcst_lead == CN.NOTAV, CN.FCST_LEAD] = 0

                # Change ALL items in column OBS_LEAD to 0 if they are 'NA'
                if not all_stat.obs_lead.dtypes == 'int':
                    all_stat.loc[all_stat.obs_lead == CN.NOTAV, CN.OBS_LEAD] = 0

                # Change 'NA' values in column INTERP_PNTS to 0 if present
                if not all_stat.interp_pnts.dtypes == 'int':
                    all_stat.loc[all_stat.interp_pnts == CN.NOTAV, CN.INTERP_PNTS] = 0
                    all_stat.interp_pnts = all_stat.interp_pnts.astype(int)

                # PCT lines in stat files are short one row, subtract 1 from n_thresh
                if all_stat[CN.LINE_TYPE].eq(CN.PCT).any():
                    all_stat.loc[all_stat.line_type == CN.PCT, '1'] = \
                        all_stat.loc[all_stat.line_type == CN.PCT, '1'] - 1

                # RPS lines in stat files may be missing rps_comp
                # if rps_comp IS null and rps is NOT null,
                # set rps_comp to 1 minus rps
                if all_stat[CN.LINE_TYPE].eq(CN.RPS).any():
                    all_stat.loc[(all_stat.line_type == CN.RPS) &
                                 (all_stat['8'].isnull()) &
                                 (~all_stat['5'].isnull()), '8'] = \
                        1 - all_stat.loc[(all_stat.line_type == CN.RPS) &
                                         (all_stat['8'].isnull()) &
                                         (~all_stat['5'].isnull()), '5']

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_data if list_frames ***", sys.exc_info()[0])

        try:

            # concatenate all the dataframes - much faster than doing an append each time
            if list_tcst:
                all_tcst = pd.concat(list_tcst, ignore_index=True, sort=False)
                list_tcst = []

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_data if list_tcst ***", sys.exc_info()[0])

        try:

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
                    if all_vsdb.line_type.isin(CN.ENS_VSDB_LINE_TYPES).any():
                        all_vsdb.loc[all_vsdb.model.str.contains(CN.FWD_SLASH) &
                                     all_vsdb.line_type.isin(CN.ENS_VSDB_LINE_TYPES),
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
                all_vsdb.insert(15, CN.INTERP_PNTS, 0)
                # add alpha and cov_thresh
                all_vsdb.insert(16, CN.ALPHA, CN.MV_NOTAV)
                all_vsdb.insert(17, CN.COV_THRESH, CN.MV_NOTAV)
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
                        # some SL1L2 files do not have MAE
                        if '6' not in vsdb_data:
                            vsdb_data.insert(25, '6', CN.MV_NOTAV)
                        one_file = vsdb_data[CN.LONG_HEADER + CN.COL_NUMS[:7] +
                                             CN.COL_NAS[:94] + [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type in (CN.VL1L2, CN.GRAD):
                        # some VL1L2 files do not have f_speed_bar and o_speed_bar
                        if '8' not in vsdb_data:
                            vsdb_data.insert(25, '8', CN.MV_NOTAV)
                        if '9' not in vsdb_data:
                            vsdb_data.insert(25, '9', CN.MV_NOTAV)
                        one_file = vsdb_data[CN.LONG_HEADER + CN.COL_NUMS[:10] +
                                             CN.COL_NAS[:91] + [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type == CN.VAL1L2:
                        one_file = vsdb_data[CN.LONG_HEADER + CN.COL_NUMS[:8] +
                                             CN.COL_NAS[:93] + [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type == CN.RHIST:
                        # rhist ranks need to be multiplied by 100. First convert to float.
                        vsdb_data[CN.COL_NUMS[0:vsdb_data[CN.N_VAR][1]]] = \
                            vsdb_data[CN.COL_NUMS[0:vsdb_data[CN.N_VAR][1]]].astype(float)
                        vsdb_data[CN.COL_NUMS[0:vsdb_data[CN.N_VAR][1]]] *= 100
                        one_file = vsdb_data[CN.LONG_HEADER + [CN.TOTAL_LC, CN.N_VAR] +
                                             CN.COL_NUMS[0:vsdb_data[CN.N_VAR][1]] +
                                             CN.COL_NAS[:(99 - vsdb_data[CN.N_VAR][1])] +
                                             [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type == CN.PCT:
                        # the total needs to be float
                        vsdb_data[CN.TOTAL_LC] = vsdb_data[CN.TOTAL_LC].astype(float)
                        # the first set of n_var columns are oy_i, the second set are subtotals
                        # add n_var new columns after the first two sets, for calculated thresh_i
                        zero_col = vsdb_data.columns.get_loc('0')
                        mid_col = vsdb_data.columns.get_loc(CN.N_VAR)
                        vsdb_data = \
                            vsdb_data.reindex(columns=[*vsdb_data.columns.tolist()[0:mid_col],
                                                       *CN.COL_NUMS[mid_col - zero_col:-2],
                                                       *vsdb_data.columns.tolist()[mid_col:]],
                                              fill_value=0)
                        # all 3 sets of columns need to be float
                        vsdb_data[CN.COL_NUMS[0:-2]] = \
                            vsdb_data[CN.COL_NUMS[0:-2]].astype(float)
                        # the total in line_data_pct is the total of all of the subtotals
                        # calculated per row as there may be rows with different values of n_var
                        col_total = vsdb_data.columns.get_loc(CN.TOTAL_LC)
                        # calculate thresh and re-order values to be
                        # in sets of thresh_i, oy_i, and on_i (which is subtotal - oy_i)
                        for index, row in vsdb_data.iterrows():
                            var_values = []
                            n_var = row[CN.N_VAR]
                            col_start = zero_col + n_var
                            col_end = col_start + n_var
                            vsdb_data.iloc[index, col_total] = row[col_start:col_end].sum()
                            for i in range(n_var):
                                var_values = var_values + [i/(n_var - 1)]
                                var_values = var_values + [row[str(i)]]
                                var_values = var_values + [row[str(i+n_var)] - row[str(i)]]
                            df_values = pd.DataFrame([var_values])
                            # put calculated and re-ordered values back into vsdb_data
                            vsdb_data.iloc[index, zero_col:zero_col + (n_var * 3)] = \
                                df_values.iloc[0, 0:n_var * 3].values
                        one_file = vsdb_data[CN.LONG_HEADER + [CN.TOTAL_LC, CN.N_VAR] +
                                             CN.COL_NUMS[0:-2] +
                                             [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type == CN.RELP:
                        one_file = vsdb_data[CN.LONG_HEADER + [CN.TOTAL_LC, CN.N_VAR] +
                                             CN.COL_NUMS[0:vsdb_data[CN.N_VAR][1]] +
                                             CN.COL_NAS[:(99 - vsdb_data[CN.N_VAR][1])] +
                                             [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type == CN.ECLV:
                        n_var = vsdb_data.loc[0, CN.N_VAR]
                        # first data column
                        zero_col = vsdb_data.columns.get_loc('0')
                        # new last data column after data is doubled
                        last_col = zero_col + (n_var * 2) - 1
                        # current last data column
                        mid_col = zero_col + n_var - 1
                        # counter for constants that will be added to double the data
                        last_point = n_var - 1
                        n_var_col = vsdb_data.columns.get_loc(CN.N_VAR)
                        top_col = int(vsdb_data.columns[n_var_col - 1])
                        # add extra columns to vsdb_data if needed for doubling of columns
                        if top_col < last_col:
                            vsdb_data = \
                                vsdb_data.reindex(columns=[*vsdb_data.columns.tolist()[0:n_var_col],
                                                           *CN.COL_NUMS[top_col + 1: last_col + 1],
                                                           *vsdb_data.columns.tolist()[n_var_col:]],
                                                  fill_value=CN.MV_NOTAV)
                        for i in range(n_var):
                            vsdb_data.iloc[:, last_col] = vsdb_data.iloc[:, mid_col]
                            vsdb_data.iloc[:, last_col - 1] = CN.X_POINTS_ECON[last_point]
                            last_col = last_col - 2
                            mid_col = mid_col - 1
                            last_point = last_point - 1
                        one_file = vsdb_data[CN.LONG_HEADER + [CN.TOTAL_LC] +
                                             CN.COL_NAS[:2] + [CN.N_VAR] +
                                             CN.COL_NUMS[0:36] +
                                             CN.COL_NAS[:61] + [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type == CN.PSTD:
                        one_file = vsdb_data[CN.LONG_HEADER + [CN.TOTAL_LC] +
                                             [CN.COL_ZERO] + CN.COL_NAS[:3] +
                                             ['3', '4', '5'] + CN.COL_NAS[:1] +
                                             ['0'] + CN.COL_NAS[:2] +
                                             ['1'] + CN.COL_NAS[:2] +
                                             ['2'] + CN.COL_NAS[:1] +
                                             CN.COL_NAS[:84] + [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type == CN.CNT:
                        one_file = vsdb_data[CN.LONG_HEADER + [CN.TOTAL_LC] +
                                             CN.COL_NAS[:27] +
                                             [CN.COL_ZERO, CN.COL_ZERO, CN.COL_ZERO] +
                                             ['2'] + CN.COL_NAS[:4] +
                                             ['0'] + CN.COL_NAS[:7] +
                                             ['3'] + CN.COL_NAS[:8] +
                                             ['1'] + CN.COL_NAS[:23] +
                                             ['4'] + CN.COL_NAS[:23] + [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type == CN.ENSCNT:
                        one_file = vsdb_data[CN.LONG_HEADER + ['0'] +
                                             CN.COL_NAS[:4] +
                                             ['1'] + CN.COL_NAS[:4] +
                                             ['2'] + CN.COL_NAS[:4] +
                                             ['3'] + CN.COL_NAS[:4] +
                                             ['4'] + CN.COL_NAS[:4] +
                                             ['5'] + CN.COL_NAS[:75] + [CN.LINE_NUM, CN.FILE_ROW]]

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
                                             CN.COL_NAS[:96] + [CN.LINE_NUM, CN.FILE_ROW]]

                    elif vsdb_type == CN.NBRCNT:
                        # fss is calculated from the other columns
                        vsdb_data['4'] = (1 - vsdb_data['1'].astype(float) /
                                          vsdb_data['2'].astype(float) +
                                          vsdb_data['3'].astype(float))
                        one_file = vsdb_data[CN.LONG_HEADER +
                                             ['0', '1'] + CN.COL_NAS[:2] + ['4'] +
                                             CN.COL_NAS[:96] + [CN.LINE_NUM, CN.FILE_ROW]]

                    # rename columns
                    if not one_file.empty:
                        one_file.columns = CN.LONG_HEADER + \
                                           CN.COL_NUMS[:101] + [CN.LINE_NUM, CN.FILE_ROW]
                        list_vsdb.append(one_file)
                        one_file = one_file.iloc[0:0]
                        vsdb_data = vsdb_data.iloc[0:0]

                # end for vsdb_type

                # Clear out all_vsdb, which we copied from above, line_type by line_type
                all_vsdb = all_vsdb.iloc[0:0]
                # combine stat and vsdb
                all_vsdb = pd.concat(list_vsdb, ignore_index=True, sort=False)
                all_stat = pd.concat([all_stat, all_vsdb], ignore_index=True, sort=False)
                all_vsdb = all_vsdb.iloc[0:0]

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_data if list_vsdb ***", sys.exc_info()[0])

        try:
            if list_cts:
                all_cts = pd.concat(list_cts, ignore_index=True, sort=False)
                list_cts = []

                # Copy forecast lead times, without trailing 0000 if they have them
                all_cts[CN.FCST_LEAD_HR] = \
                    np.where(all_cts[CN.FCST_LEAD] > 9999,
                             all_cts[CN.FCST_LEAD] // 10000,
                             all_cts[CN.FCST_LEAD])

                # Calculate fcst_init = fcst_valid - fcst_lead hours
                all_cts.insert(8, CN.FCST_INIT, CN.NOTAV)
                all_cts[CN.FCST_INIT] = all_cts[CN.FCST_VALID] - \
                    pd.to_timedelta(all_cts[CN.FCST_LEAD_HR], unit='h')

                # line type of mode contingency table
                all_cts[CN.LINE_TYPE_LU_ID] = 19

                self.mode_cts_data = all_cts
                all_cts = all_cts.iloc[0:0]

            if list_obj:
                # gather all mode lines
                all_obj = pd.concat(list_obj, ignore_index=True, sort=False)
                list_obj = []

                # Copy forecast lead times, without trailing 0000 if they have them
                all_obj[CN.FCST_LEAD_HR] = \
                    np.where(all_obj[CN.FCST_LEAD] > 9999,
                             all_obj[CN.FCST_LEAD] // 10000,
                             all_obj[CN.FCST_LEAD])

                # Calculate fcst_init = fcst_valid - fcst_lead hours
                all_obj.insert(8, CN.FCST_INIT, CN.NOTAV)
                all_obj[CN.FCST_INIT] = all_obj[CN.FCST_VALID] - \
                    pd.to_timedelta(all_obj[CN.FCST_LEAD_HR], unit='h')

                # default to mode single
                all_obj[CN.LINE_TYPE_LU_ID] = 17

                # mark if it's a mode pair
                all_obj.loc[all_obj.object_id.str.contains('_'),
                            CN.LINE_TYPE_LU_ID] = 18

                self.mode_obj_data = all_obj
                all_obj = all_obj.iloc[0:0]

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_data if list_cts or list_obj ***",
                          sys.exc_info()[0])

        try:
            if not all_stat.empty:

                logging.debug("Shape of all_stat before transforms: %s", str(all_stat.shape))

                # delete any lines that have invalid line_types
                invalid_line_indexes = all_stat[~all_stat.line_type.isin(CN.UC_LINE_TYPES)].index

                if not invalid_line_indexes.empty:

                    logging.warning("!!! Warning, invalid line_types:")
                    logging.warning("line types: %s",
                                    str(all_stat.iloc[invalid_line_indexes].line_type))

                    all_stat.drop(invalid_line_indexes, axis=0, inplace=True)

                # if user specified line types to load, delete the rest
                if load_flags["line_type_load"]:
                    all_stat.drop(all_stat[~all_stat.line_type.isin(line_types)].index,
                                  inplace=True)

                # if load_spec has flag to not load MPR records, delete them
                if not load_flags["load_mpr"]:
                    all_stat.drop(all_stat[all_stat.line_type == CN.MPR].index, inplace=True)

                # if load_spec has flag to not load ORANK records, delete them
                if not load_flags["load_orank"]:
                    all_stat.drop(all_stat[all_stat.line_type == CN.ORANK].index, inplace=True)

                # reset the index, in case any lines have been deleted
                all_stat.reset_index(drop=True, inplace=True)

                # if all lines from a stat or vsdb file were deleted, remove filename
                files_to_drop = ~self.data_files.index.isin(all_stat[CN.FILE_ROW])
                files_stat = self.data_files[CN.DATA_FILE_LU_ID].isin([CN.VSDB_POINT_STAT,
                                                                       CN.STAT])
                self.data_files.drop(self.data_files[files_to_drop & files_stat].index,
                                     inplace=True)

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
                all_stat = all_stat.iloc[0:0]

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_data near end ***", sys.exc_info()[0])

        try:
            if not all_tcst.empty:

                logging.debug("Shape of all_tcst before transforms: %s", str(all_tcst.shape))

                # delete any lines that have invalid line_types
                invalid_line_indexes = \
                    all_tcst[~all_tcst.line_type.isin(CN.UC_LINE_TYPES_TCST)].index

                if not invalid_line_indexes.empty:

                    logging.warning("!!! Warning, invalid line_types:")
                    logging.warning("line types: %s",
                                    str(all_tcst.iloc[invalid_line_indexes].line_type))

                    all_tcst.drop(invalid_line_indexes, axis=0, inplace=True)

                # reset the index, in case any lines have been deleted
                all_tcst.reset_index(drop=True, inplace=True)

                # if all lines from a tcst file were deleted, remove filename
                files_to_drop = ~self.data_files.index.isin(all_tcst[CN.FILE_ROW])
                files_tcsp = self.data_files[CN.DATA_FILE_LU_ID] == CN.TCST
                self.data_files.drop(self.data_files[files_to_drop & files_tcsp].index,
                                     inplace=True)

                self.data_files.reset_index(drop=True, inplace=True)

                self.tcst_data = all_tcst
                all_tcst = all_tcst.iloc[0:0]

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_data near end ***", sys.exc_info()[0])

        try:
            if list_2d:
                all_2d = pd.concat(list_2d, ignore_index=True, sort=False)
                list_2d = []

                # line type of mtd 2d table
                all_2d[CN.LINE_TYPE_LU_ID] = 19

                self.mtd_2d_data = all_2d
                all_2d = all_2d.iloc[0:0]

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_data if list_2d ***",
                          sys.exc_info()[0])

        try:
            if list_single:
                all_single = pd.concat(list_single, ignore_index=True, sort=False)
                list_single = []

                # line type of mtd single table
                all_single[CN.LINE_TYPE_LU_ID] = 17

                self.mtd_3d_single_data = all_single
                all_single = all_single.iloc[0:0]

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_data if list_single ***",
                          sys.exc_info()[0])

        try:
            if list_pair:
                all_pair = pd.concat(list_pair, ignore_index=True, sort=False)
                list_pair = []

                # line type of mtd pair table
                all_pair[CN.LINE_TYPE_LU_ID] = 18

                self.mtd_3d_pair_data = all_pair
                all_pair = all_pair.iloc[0:0]

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_data if list_pair ***",
                          sys.exc_info()[0])

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
        # set the default to invalid file for later purging
        lu_type = CN.NO_KEY

        # Set lookup type from file extensions and the values in the data_file_lu table
        if lc_filename.endswith(".stat"):
            lu_type = CN.STAT
        elif lc_filename.endswith(".vsdb"):
            lu_type = CN.VSDB_POINT_STAT
        elif (lc_filename.endswith("cts.txt") and
              os.path.basename(lc_filename).startswith("mode")):
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
        elif lc_filename.endswith(".tcst"):
            lu_type = CN.TCST
        return lu_type

    def read_stat(self, filename, hdr_names):
        """ Read in all of the lines except the header of a stat file.
            Returns:
               all the stat lines in a dataframe, with dates converted to datetime
        """
        # switched to python engine for python 3.8 and pandas 1.4.2
        # switched back to c version for pandas 1.2.3
        # added the low_memory=False option when getting a DtypeWarning
        # to switch to python version: low_memory=False -> engine='python'
        return pd.read_csv(filename, delim_whitespace=True,
                           names=hdr_names, skiprows=1,
                           parse_dates=[CN.FCST_VALID_BEG,
                                        CN.FCST_VALID_END,
                                        CN.OBS_VALID_BEG,
                                        CN.OBS_VALID_END],
                           date_parser=self.cached_date_parser,
                           keep_default_na=False, na_values='', low_memory=False)

    def read_tcst(self, filename, hdr_names):
        """ Read in all of the lines except the header of a tcst file.
            Returns:
               all the tcst lines in a dataframe, with dates converted to datetime
        """
        # added the low_memory=False option when getting a DtypeWarning
        return pd.read_csv(filename, delim_whitespace=True,
                           names=hdr_names, skiprows=1,
                           parse_dates=[CN.INIT,
                                        CN.VALID],
                           date_parser=self.cached_date_parser,
                           keep_default_na=False, na_values='', low_memory=False)

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
        if date_str == CN.NOTAV:
            return CN.MV_NULL
        date_time = pd.to_datetime(date_str, format='%Y%m%d_%H%M%S', errors='ignore')
        self.cache[date_str] = date_time
        return date_time

    def read_mode(self, filename, hdr_names):
        """ Read in all of the lines except the header of a mode file.
            Returns:
               all the mode lines in a dataframe, with dates converted to datetime
        """
        # added the low_memory=False option when getting a DtypeWarning
        return pd.read_csv(filename, delim_whitespace=True,
                           names=hdr_names, skiprows=1,
                           parse_dates=[CN.FCST_VALID,
                                        CN.OBS_VALID],
                           date_parser=self.cached_date_parser,
                           keep_default_na=False, na_values='', low_memory=False)
