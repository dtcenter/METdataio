#!/usr/bin/env python3

"""
Program Name: write_stat_sql.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Write stat files (MET and VSDB) to a SQL database.
Parameters: N/A
Input Files: transformed dataframe of MET and VSDB lines
Output Files: N/A
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member
# constants exist in constants.py

import sys
import os
# from pathlib import Path
import logging
import time
from datetime import timedelta
import pymysql
import numpy as np   # without this, pylint throws a maximum recursion error
import pandas as pd

import constants as CN


class WriteStatSql:
    """ Class to write stat files (MET and VSDB) to a SQL database
        Returns:
           N/A
    """

    def __init__(self, connection):
        # Connect to the database
        self.conn = pymysql.connect(host=connection['db_host'],
                                    port=connection['db_port'],
                                    user=connection['db_user'],
                                    passwd=connection['db_password'],
                                    db=connection['db_name'],
                                    local_infile=True)

        self.cur = self.conn.cursor()

    def write_sql_data(self, load_flags, data_files, stat_data, group, description,
                       load_note, xml_str):
        """ write stat files (MET and VSDB) to a SQL database.
            Returns:
               N/A
        """

        logging.debug("[--- Start write_sql_data ---]")

        write_time_start = time.perf_counter()

        try:
            self.cur.execute("SHOW GLOBAL VARIABLES LIKE 'local_infile';")
            result = self.cur.fetchall()
            logging.debug("local_infile is %s", result[0][1])

            # write out records for data files, but first:
            # check for duplicates if flag on - delete if found
            for row_num, file_line in data_files.iterrows():
                # look for existing data file record
                self.cur.execute(CN.Q_FILE, [file_line[CN.FILEPATH], file_line[CN.FILENAME]])
                result = self.cur.fetchone()

                # If you find a match, check the force_dup_file tag/flag
                if self.cur.rowcount > 0:
                    if not load_flags['force_dup_file']:
                        # delete line data rows that match index of duplicated file
                        stat_data = stat_data.drop(stat_data[stat_data.file_row == \
                                                             file_line.file_row].index)
                        data_files = data_files.drop(row_num)
                        logging.warning("!!! Duplicate file %s without FORCE_DUP_FILE tag",
                                        file_line[CN.FULL_FILE])
                    else:
                        # if duplicate files allowed, save the existing id for the file
                        data_files.loc[data_files.index[row_num], CN.DATA_FILE_ID] = result[0]

            # reset the stat_data index in case any records were dropped
            stat_data.reset_index(drop=True, inplace=True)

            # data files start counting from 1
            next_file_id = self.get_next_id(CN.DATA_FILE, CN.DATA_FILE_ID)
            if next_file_id == 0:
                next_file_id = 1

            # For new files add the next id to the row number/index to make a new key
            data_files.loc[data_files.data_file_id == CN.NO_KEY, CN.DATA_FILE_ID] = \
                        data_files.index + next_file_id

            # Replace the temporary id value with the actual index in the stat line data
            for row_num, row in data_files.iterrows():
                stat_data.loc[stat_data[CN.FILE_ROW] == row[CN.FILE_ROW], CN.DATA_FILE_ID] = \
                    row[CN.DATA_FILE_ID]

            # write out the data files.
            if not data_files.empty:
                # later in development, may wish to delete this file to clean up after writing
                tmpfile = os.getenv('HOME') + '/METdbLoadFiles.csv'
                data_files[CN.DATA_FILE_FIELDS].to_csv(tmpfile, na_rep='-9999',
                                                       index=False, header=False, sep=CN.SEP)
                self.cur.execute(CN.L_TABLE.format(tmpfile, CN.DATA_FILE, CN.SEP))

            # find the unique headers for this current load job
            # for now, including VERSION to make pandas code easier - unlike MVLoad
            stat_headers = stat_data[CN.STAT_HEADER_KEYS].drop_duplicates()
            stat_headers.reset_index(drop=True, inplace=True)

            # At first, we do not know if the headers already exist, so we have no keys
            stat_headers[CN.STAT_HEADER_ID] = CN.NO_KEY

            # get the next valid header id. Set it to zero (first valid id) if no records yet
            next_header_id = self.get_next_id(CN.STAT_HEADER, CN.STAT_HEADER_ID)

            # if the flag is set to check for duplicate headers, get ids from existing headers
            if load_flags["stat_header_db_check"]:

                # For each header, query with unique fields to try to find a match in the database
                for row_num, data_line in stat_headers.iterrows():
                    self.cur.execute(CN.Q_HEADER, data_line.values[:-1].tolist())
                    result = self.cur.fetchone()

                    # If you find a match, put the key into the stat_headers dataframe
                    if self.cur.rowcount > 0:
                        stat_headers.loc[stat_headers.index[row_num], CN.STAT_HEADER_ID] = result[0]

            # For new headers add the next id to the row number/index to make a new key
            stat_headers.loc[stat_headers.stat_header_id == CN.NO_KEY, CN.STAT_HEADER_ID] = \
                        stat_headers.index + next_header_id

            # get just the new headers with their keys
            new_headers = stat_headers[stat_headers[CN.STAT_HEADER_ID] > (next_header_id - 1)]

            # Write any new headers out to a CSV file, and then load them into database
            if not new_headers.empty:
                # later in development, may wish to delete this file to clean up after writing
                tmpfile = os.getenv('HOME') + '/METdbLoadHeaders.csv'
                new_headers[CN.STAT_HEADER_FIELDS].to_csv(tmpfile, na_rep='-9999',
                                                          index=False, header=False, sep=CN.SEP)
                self.cur.execute(CN.L_TABLE.format(tmpfile, CN.STAT_HEADER, CN.SEP))

            # put the header ids back into the dataframe of all the line data
            stat_data = pd.merge(left=stat_data, right=stat_headers)

            # find all of the line types in the data
            line_types = stat_data.line_type.unique()

            # process one kind of line data at a time
            for line_type in line_types:

                # use the UC line type to index into the list of table names
                line_table = CN.LINE_TABLES[CN.UC_LINE_TYPES.index(line_type)]

                # Only variablw length lines have a line_data_id
                # more needs to be done on this
                if line_type in CN.VAR_LINE_TYPES:
                    # Get next valid line data id. Set it to zero (first valid id) if no records yet
                    next_line_id = \
                        self.get_next_id(line_table, CN.LINE_HEADER_ID)
                    logging.debug("next_line_id is %s", next_line_id)

                # get the line data of just this type and re-index
                line_data = stat_data[stat_data[CN.LINE_TYPE] == line_type]
                line_data.reset_index(drop=True, inplace=True)

                # write the lines out to a CSV file, and then load them into database
                # for debugging, unique. may want to reuse same name later - and delete
                tmpfile = os.getenv('HOME') + '/METdbLoadLines' + line_type + '.csv'
                line_data[CN.LINE_DATA_COLS[line_type]].to_csv(tmpfile, na_rep='-9999',
                                                               index=False, header=False,
                                                               sep=CN.SEP)
                self.cur.execute(CN.L_TABLE.format(tmpfile, line_table, CN.SEP))

            # insert or update the group and description fields in the metadata table
            if group != CN.DEFAULT_DATABASE_GROUP:
                self.cur.execute(CN.Q_METADATA)
                result = self.cur.fetchone()

                # If you find a match, update the category and description
                if self.cur.rowcount > 0:
                    if group != result[0] or description != result[1]:
                        self.cur.execute(CN.U_METADATA, [group, description])
                # otherwise, insert the category and description
                else:
                    self.cur.execute(CN.I_METADATA, [group, description])

            if load_flags['load_xml'] and not data_files.empty:
                update_date = data_files[CN.LOAD_DATE].iloc[0]
                next_instance_id = self.get_next_id(CN.INSTANCE_INFO, CN.INSTANCE_INFO_ID)
                self.cur.execute(CN.I_INSTANCE, [next_instance_id, 'mvuser', update_date,
                                                 load_note, xml_str])

            self.conn.commit()

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_sql_data ***", sys.exc_info()[0])

        finally:
            self.cur.close()
            self.conn.close()

        write_time_end = time.perf_counter()
        write_time = timedelta(seconds=write_time_end - write_time_start)

        logging.info("    >>> Write time: %s", str(write_time))

        logging.debug("[--- End write_sql_data ---]")

    def get_next_id(self, table, field):
        """ given a field for a table, find the max field value and return it plus one.
            Returns:
               next valid id to use in an id field in a table
        """
        # get the next valid id. Set it to zero (first valid id) if no records yet
        try:
            next_id = 0
            query_for_id = "SELECT MAX(" + field + ") from " + table
            self.cur.execute(query_for_id)
            result = self.cur.fetchone()
            if result[0] is not None:
                next_id = result[0] + 1
            return next_id

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_sql_data get_next_id ***", sys.exc_info()[0])
