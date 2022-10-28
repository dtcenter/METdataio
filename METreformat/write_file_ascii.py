#!/usr/bin/env python3

"""
Program Name: write_file_ascii.py
Contact(s): Venita Hagerty, Minna Win
Abstract:
History Log:  Initial version
Usage: Write data_file records to an ASCII file.
Parameters: N/A
Input Files: transformed dataframe of MET and VSDB lines
Output Files: N/A
Copyright 2020 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member
# constants exist in constants.py

import sys
import logging
import time
from datetime import timedelta
import getpass
import pandas as pd

import constants as CN

from run_sql import RunSql


class WriteFileASCII:
    """ Class to write data_file records to an ASCII file
        Returns:
           N/A
    """

    def write_file_ascii(self, load_flags, data_files, stat_data, mode_cts_data,
                       mode_obj_data, tcst_data, mtd_2d_data, mtd_3d_single_data,
                       mtd_3d_pair_data, tmp_dir, local_infile):
        """ write data_file records to an ASCII file.
            Returns:
               N/A
        """

        logging.debug("[--- Start write_file_ascii ---]")

        write_time_start = time.perf_counter()

        try:

            # --------------------
            # Write Data Files
            # --------------------

            # get next valid data file id. data files start counting from 1
            next_file_id = self.sql_met.get_next_id(CN.DATA_FILE, CN.DATA_FILE_ID)
            if next_file_id == 0:
                next_file_id = 1

            id_ctr = 0
            list_dupes = []

            # write out records for data files, but first:
            # check for duplicates if flag on - delete if found
            for row_num, file_line in data_files.iterrows():
                # look for existing data file record
                sql_cur.execute(CN.Q_FILE, [file_line[CN.FILEPATH], file_line[CN.FILENAME]])
                result = sql_cur.fetchone()

                # If you find a match, check the force_dup_file tag/flag
                if sql_cur.rowcount > 0:
                    list_dupes = list_dupes + [file_line[CN.FILE_ROW]]
                    if not load_flags['force_dup_file']:
                        logging.warning("!!! Duplicate file %s without FORCE_DUP_FILE tag",
                                        file_line[CN.FULL_FILE])
                    else:
                        # With duplicate files allowed, save the existing id for the file
                        data_files.loc[data_files.index[row_num], CN.DATA_FILE_ID] = result[0]
                        logging.warning("Duplicate file %s already in data_file",
                                        file_line[CN.FULL_FILE])
                # Not a duplicate - give it a new id
                else:
                    data_files.loc[data_files.index[row_num], CN.DATA_FILE_ID] = \
                        id_ctr + next_file_id
                    id_ctr = id_ctr + 1

            # end for row_num, file_line

            if not load_flags['force_dup_file']:

                # delete line data rows that match index of duplicated file
                if not stat_data.empty and list_dupes:
                    if stat_data.file_row.isin(list_dupes).any():
                        stat_data.drop(stat_data[stat_data.file_row
                                                 .isin(list_dupes)].index,
                                       inplace=True)

                if not mode_cts_data.empty and list_dupes:
                    if mode_cts_data.file_row.isin(list_dupes).any():
                        mode_cts_data.drop(mode_cts_data[mode_cts_data.file_row
                                                         .isin(list_dupes)].index,
                                           inplace=True)

                if not mode_obj_data.empty and list_dupes:
                    if mode_obj_data.file_row.isin(list_dupes).any():
                        mode_obj_data.drop(mode_obj_data[mode_obj_data.file_row
                                                         .isin(list_dupes)].index,
                                           inplace=True)

                if not mtd_2d_data.empty and list_dupes:
                    if mtd_2d_data.file_row.isin(list_dupes).any():
                        mtd_2d_data.drop(mtd_2d_data[mtd_2d_data.file_row
                                                     .isin(list_dupes)].index,
                                         inplace=True)

                if not mtd_3d_single_data.empty and list_dupes:
                    if mtd_3d_single_data.file_row.isin(list_dupes).any():
                        mtd_3d_single_data.drop(mtd_3d_single_data[mtd_3d_single_data.file_row
                                                                   .isin(list_dupes)].index,
                                                inplace=True)

                if not mtd_3d_pair_data.empty and list_dupes:
                    if mtd_3d_pair_data.file_row.isin(list_dupes).any():
                        mtd_3d_pair_data.drop(mtd_3d_pair_data[mtd_3d_pair_data.file_row
                                                               .isin(list_dupes)].index,
                                              inplace=True)

            # delete duplicate file entries
            index_names = data_files[data_files.data_file_id == CN.NO_KEY].index
            data_files.drop(index_names, inplace=True)

            if not data_files.empty:

                # reset indexes in case any records were dropped
                stat_data.reset_index(drop=True, inplace=True)
                mode_cts_data.reset_index(drop=True, inplace=True)
                mode_obj_data.reset_index(drop=True, inplace=True)
                tcst_data.reset_index(drop=True, inplace=True)

                # Replace the temporary id value with the actual index in the line data
                for row_num, row in data_files.iterrows():
                    if not stat_data.empty:
                        stat_data.loc[stat_data[CN.FILE_ROW] == row[CN.FILE_ROW],
                                      CN.DATA_FILE_ID] = row[CN.DATA_FILE_ID]
                    if not mode_cts_data.empty:
                        mode_cts_data.loc[mode_cts_data[CN.FILE_ROW] == row[CN.FILE_ROW],
                                          CN.DATA_FILE_ID] = row[CN.DATA_FILE_ID]
                    if not mode_obj_data.empty:
                        mode_obj_data.loc[mode_obj_data[CN.FILE_ROW] == row[CN.FILE_ROW],
                                          CN.DATA_FILE_ID] = row[CN.DATA_FILE_ID]
                    if not tcst_data.empty:
                        tcst_data.loc[tcst_data[CN.FILE_ROW] == row[CN.FILE_ROW],
                                      CN.DATA_FILE_ID] = row[CN.DATA_FILE_ID]
                    if not mtd_2d_data.empty:
                        mtd_2d_data.loc[mtd_2d_data[CN.FILE_ROW] == row[CN.FILE_ROW],
                                        CN.DATA_FILE_ID] = row[CN.DATA_FILE_ID]
                    if not mtd_3d_single_data.empty:
                        mtd_3d_single_data.loc[mtd_3d_single_data[CN.FILE_ROW] == row[CN.FILE_ROW],
                                               CN.DATA_FILE_ID] = row[CN.DATA_FILE_ID]
                    if not mtd_3d_pair_data.empty:
                        mtd_3d_pair_data.loc[mtd_3d_pair_data[CN.FILE_ROW] == row[CN.FILE_ROW],
                                             CN.DATA_FILE_ID] = row[CN.DATA_FILE_ID]

                # get just the new data files
                new_files = data_files[data_files[CN.DATA_FILE_ID] >= next_file_id]

                # !!!REMOVE
                # write the new data files out to the sql database
                # if not new_files.empty:
                #     self.sql_met.write_to_sql(new_files, CN.DATA_FILE_FIELDS, CN.DATA_FILE,
                #                               CN.INS_DATA_FILES, tmp_dir, sql_cur, local_infile)

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_file_ascii ***", sys.exc_info()[0])

        write_time_end = time.perf_counter()
        write_time = timedelta(seconds=write_time_end - write_time_start)

        logging.info("    >>> Write time File: %s", str(write_time))

        logging.debug("[--- End write_file_ascii ---]")

        return data_files, stat_data, mode_cts_data, mode_obj_data, tcst_data, \
            mtd_2d_data, mtd_3d_single_data, mtd_3d_pair_data
