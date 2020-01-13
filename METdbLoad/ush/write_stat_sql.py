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
import logging
import time
from datetime import timedelta
import getpass
import pymysql
import numpy as np
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

        # Default to False since it requires extra permission
        self.local_infile = False

    def write_sql_data(self, load_flags, data_files, stat_data, group, description,
                       load_note, xml_str):
        """ write stat files (MET and VSDB) to a SQL database.
            Returns:
               N/A
        """

        logging.debug("[--- Start write_sql_data ---]")

        write_time_start = time.perf_counter()

        try:
            # look at database to see whether we can use the local infile method
            self.cur.execute("SHOW GLOBAL VARIABLES LIKE 'local_infile';")
            result = self.cur.fetchall()
            self.local_infile = result[0][1]
            logging.debug("local_infile is %s", result[0][1])

            # --------------------
            # Write Data Files
            # --------------------

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
                        stat_data = stat_data.drop(stat_data[stat_data.file_row ==
                                                             file_line.file_row].index)
                        data_files = data_files.drop(row_num)
                        logging.warning("!!! Duplicate file %s without FORCE_DUP_FILE tag",
                                        file_line[CN.FULL_FILE])
                    else:
                        # With duplicate files allowed, save the existing id for the file
                        data_files.loc[data_files.index[row_num], CN.DATA_FILE_ID] = result[0]

            # end for row_num, file_line

            # reset the stat_data index in case any records were dropped
            stat_data.reset_index(drop=True, inplace=True)

            # get next valid data file id. data files start counting from 1
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

            # get just the new data files
            new_files = data_files[data_files[CN.DATA_FILE_ID] >= next_file_id]

            # write the new data files out to the sql database
            if not new_files.empty:
                self.write_to_sql(new_files, CN.DATA_FILE_FIELDS, CN.DATA_FILE, CN.INS_DATA_FILES)

            # --------------------
            # Write Stat Headers
            # --------------------

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
            logging.info("New headers: %s rows", str(len(new_headers.index)))

            # Write any new headers out to the sql database
            if not new_headers.empty:
                self.write_to_sql(new_headers, CN.STAT_HEADER_FIELDS, CN.STAT_HEADER, CN.INS_HEADER)

            # put the header ids back into the dataframe of all the line data
            stat_data = pd.merge(left=stat_data, right=stat_headers)

            # --------------------
            # Write Line Data
            # --------------------

            # find all of the line types in the data
            line_types = stat_data.line_type.unique()

            # process one kind of line data at a time
            for line_type in line_types:

                all_var = pd.DataFrame()

                # use the UC line type to index into the list of table names
                line_table = CN.LINE_TABLES[CN.UC_LINE_TYPES.index(line_type)]

                # get the line data of just this type and re-index
                line_data = stat_data[stat_data[CN.LINE_TYPE] == line_type].copy()
                line_data.reset_index(drop=True, inplace=True)
                logging.info("%s: %s rows", line_type, str(len(line_data.index)))

                # change all Not Available values to METviewer not available (-9999)
                line_data = line_data.replace('NA', CN.MV_NOTAV)

                # change float numbers to have limited digits after the decimal point
                line_data = np.round(line_data, decimals=7)

                # Only variable length lines have a line_data_id
                # more needs to be done on this
                if line_type in CN.VAR_LINE_TYPES:
                    # Get next valid line data id. Set it to zero (first valid id) if no records yet
                    next_line_id = \
                        self.get_next_id(line_table, CN.LINE_DATA_ID)
                    logging.debug("next_line_id is %s", next_line_id)

                    # try to keep order the same as MVLoad
                    line_data = line_data.sort_values(by=[CN.DATA_FILE_ID, CN.LINE_NUM])
                    line_data.reset_index(drop=True, inplace=True)

                    line_data[CN.LINE_DATA_ID] = line_data.index + next_line_id

                    # index of the first column of the repeating variables
                    var_index = line_data.columns.get_loc(CN.LINE_VAR_COUNTER[line_type]) + 1
                    # need this later for old RHIST
                    orig_index = var_index

                    # There are 10 extra variables after n_thresh in PSTD records
                    if line_type == CN.PSTD:
                        var_index = var_index + 10

                    for row_num, file_line in line_data.iterrows():
                        # how many sets of repeating variables
                        var_count = int(file_line[CN.LINE_VAR_COUNTER[line_type]])
                        # these two variable line types are one group short
                        if line_type in [CN.PJC, CN.PRC]:
                            var_count = var_count - 1
                        # older versions of RHIST have varying ECNT data in them
                        if line_type == CN.RHIST and file_line[CN.VERSION] in CN.RHIST_OLD:
                            var_count = int(file_line['3'])
                            var_index = orig_index + 2
                            if file_line[CN.VERSION] in CN.RHIST_5:
                                var_index = var_index + 1
                            if file_line[CN.VERSION] in CN.RHIST_6:
                                var_index = var_index + 2
                        # MCTC needs an i and a j counter
                        if line_type == CN.MCTC:
                            basic_count = var_count
                            var_count = var_count * var_count
                        # The number of variables in the repeats
                        var_repeats = CN.LINE_VAR_REPEATS[line_type]
                        # number of sets of variables times the number of variables in the sets
                        repeat_width = int(var_count * var_repeats)

                        # pull out just the repeating data
                        list_var_data = file_line.iloc[var_index:var_index + repeat_width]
                        # put it into the right number of rows and columns
                        var_data = \
                            pd.DataFrame(list_var_data.values.reshape(var_count, var_repeats))

                        # for older versions of RHIST, blank out repeating fields in line data
                        if line_type == CN.RHIST and file_line[CN.VERSION] in CN.RHIST_OLD:
                            line_data.iloc[row_num, var_index:var_index + repeat_width] = \
                                CN.MV_NOTAV

                        # add on the first two fields - line data id, and i value
                        var_data.insert(0, CN.LINE_DATA_ID, file_line[CN.LINE_DATA_ID])
                        var_data.insert(1, 'i_value', var_data.index + 1)

                        # MCTC has i and j counters where j increments faster
                        if line_type == CN.MCTC:
                            var_data.loc[:, 'i_value'] = \
                                np.repeat(np.array(range(1, basic_count + 1)), basic_count)
                            j_indices = np.resize(range(1, basic_count + 1), var_count)
                            var_data.insert(2, 'j_value', j_indices)

                        if line_type == CN.ORANK:
                            # move the values after the variable length data to the left
                            var_end = var_index + repeat_width
                            line_data.iloc[row_num, var_index:var_index + 7] = \
                                line_data.iloc[row_num, var_end:var_end + 7].values

                        # collect all of the variable data for a line type
                        all_var = all_var.append(var_data, ignore_index=True)

                    # end for row_num, file_line

                    # fill in the missing NA values that will otherwise write as zeroes
                    if line_type == CN.PSTD:
                        line_data.insert(25, 'briercl', CN.MV_NOTAV)
                        line_data.insert(26, 'briercl_ncl', CN.MV_NOTAV)
                        line_data.insert(27, 'briercl_ncu', CN.MV_NOTAV)
                        line_data.insert(28, 'bss', CN.MV_NOTAV)
                        line_data.insert(29, 'bss_smpl', CN.MV_NOTAV)
                        # add the missing 5 column names to the list of columns to write
                        CN.LINE_DATA_COLS[line_type] = CN.LINE_DATA_COLS[line_type][0:-5] + \
                                                       ['briercl', 'briercl_ncl', 'briercl_ncu',
                                                        'bss', 'bss_smpl']

                    if line_type == CN.RHIST:
                        # copy the RHIST columns and create ECNT lines from them
                        line_data2 = line_data[line_data[CN.VERSION].isin(CN.RHIST_OLD)].copy()
                        if not line_data2.empty:
                            line_data2.line_type = CN.ECNT

                            # put the fields in the correct order for ECNT
                            line_data2 = \
                                line_data2.rename(columns={'1':'2', '2':'4',
                                                           '3':'1', '4':'3',
                                                           '5':'7', '7':'5'})

                            # Write out the ECNT lines created from old RHIST lines
                            self.write_to_sql(line_data2, CN.LINE_DATA_COLS[CN.ECNT],
                                              CN.LINE_TABLES[CN.UC_LINE_TYPES.index(CN.ECNT)],
                                              CN.LINE_DATA_Q[CN.ECNT])

                            # copy the value of n_rank two columns earlier for old RHIST
                            line_data.loc[line_data[CN.VERSION].isin(CN.RHIST_OLD), '1'] = \
                                        line_data['3']

                # write the lines out to a CSV file, and then load them into database
                if not line_data.empty:
                    self.write_to_sql(line_data, CN.LINE_DATA_COLS[line_type], line_table,
                                      CN.LINE_DATA_Q[line_type])

                # if there are variable length records, write them out also
                if not all_var.empty:
                    all_var.columns = CN.LINE_DATA_VAR_FIELDS[line_type]
                    self.write_to_sql(all_var, CN.LINE_DATA_VAR_FIELDS[line_type],
                                      CN.LINE_DATA_VAR_TABLES[line_type],
                                      CN.LINE_DATA_VAR_Q[line_type])

            # end for line_type

            # --------------------
            # Write Metadata - group and description
            # --------------------

            # insert or update the group and description fields in the metadata table
            if group != CN.DEFAULT_DATABASE_GROUP:
                self.cur.execute(CN.Q_METADATA)
                result = self.cur.fetchone()

                # If you find a match, update the category and description
                if self.cur.rowcount > 0:
                    if group != result[0] or description != result[1]:
                        self.cur.execute(CN.UPD_METADATA, [group, description])
                # otherwise, insert the category and description
                else:
                    self.cur.execute(CN.INS_METADATA, [group, description])

            # --------------------
            # Write Instance Info
            # --------------------

            if load_flags['load_xml'] and not data_files.empty:
                update_date = data_files[CN.LOAD_DATE].iloc[0]
                next_instance_id = self.get_next_id(CN.INSTANCE_INFO, CN.INSTANCE_INFO_ID)
                self.cur.execute(CN.INS_INSTANCE, [next_instance_id, getpass.getuser(), update_date,
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

    def write_to_sql(self, raw_data, col_list, sql_table, sql_query):
        """ given a dataframe of raw_data with specific columns to write to a sql_table,
            write to a csv file and use local data infile for speed if allowed.
            otherwise, do an executemany to use a SQL insert statement to write data
        """

        try:
            if self.local_infile:
                # later in development, may wish to delete these files to clean up when done
                tmpfile = os.getenv('HOME') + '/METdbLoad_' + sql_table + '.csv'
                # write the data out to a csv file, use local data infile to load to database
                raw_data[col_list].to_csv(tmpfile, na_rep=CN.MV_NOTAV,
                                          index=False, header=False, sep=CN.SEP)
                self.cur.execute(CN.LD_TABLE.format(tmpfile, sql_table, CN.SEP))
            else:
                # fewer permissions required, but slower
                # Make sure there are no NaN values
                raw_data = raw_data.fillna(CN.MV_NOTAV)
                # make a copy of the dataframe that is a list of lists and write to database
                dfile = raw_data[col_list].values.tolist()
                # only line_data has timestamps in dataframe - change to datetime strings
                if 'line_data' in sql_query:
                    for line_num in range(len(dfile)):
                        dfile[line_num][4] = dfile[line_num][4].strftime("%Y-%m-%d %H:%M:%S")
                        dfile[line_num][5] = dfile[line_num][5].strftime("%Y-%m-%d %H:%M:%S")
                        dfile[line_num][6] = dfile[line_num][6].strftime("%Y-%m-%d %H:%M:%S")
                        dfile[line_num][8] = dfile[line_num][8].strftime("%Y-%m-%d %H:%M:%S")
                        dfile[line_num][9] = dfile[line_num][9].strftime("%Y-%m-%d %H:%M:%S")
                self.cur.executemany(sql_query, dfile)

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_stat_sql write_to_sql ***", sys.exc_info()[0])
