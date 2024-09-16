#!/usr/bin/env python3

"""
Program Name: write_tcst_sql.py
Contact(s): Tatiana Burek
Abstract:
History Log:  Initial version
Usage: Write tcst files to a SQL database.
Parameters: N/A
Input Files: transformed dataframe of tcst lines
Output Files: N/A
Copyright 2020 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member
# constants exist in constants.py


import sys
import time
from datetime import timedelta
import pandas as pd

import METdbLoad.ush.constants as CN

from METdbLoad.ush.run_sql import RunSql


class WriteTcstSql:
    """ Class to write tcst files  to a SQL database
            Returns:
               N/A
    """

    @staticmethod
    def write_tcst_data(load_flags, tcst_data, tmp_dir, sql_cur, local_infile, logger):
        """ write tcst files to a SQL database.
            Returns:
               N/A
        """

        logger.debug("[--- Start write_tcst_data ---]")

        write_time_start = time.perf_counter()

        try:

            sql_met = RunSql()

            # --------------------
            # Write Tcst Headers
            # --------------------

            # find the unique headers for this current load job
            # Do not include Version, as MVLoad does not
            tcst_headers = tcst_data[CN.TCST_HEADER_KEYS].copy()
            tcst_headers.drop_duplicates(CN.TCST_HEADER_KEYS[1:], keep='first', inplace=True)
            tcst_headers.reset_index(drop=True, inplace=True)

            # At first, we do not know if the headers already exist, so we have no keys
            tcst_headers[CN.TCST_HEADER_ID] = CN.NO_KEY

            # get the next valid tcst header id. Set it to zero (first valid id) if no records yet
            next_header_id = sql_met.get_next_id(CN.TCST_HEADER, CN.TCST_HEADER_ID, sql_cur, logger)

            # if the flag is set to check for duplicate headers, get ids from existing headers
            if load_flags["tcst_header_db_check"]:

                # For each header, query with unique fields to try to find a match in the database
                for row_num, data_line in tcst_headers.iterrows():
                    sql_cur.execute(CN.Q_HEADER_TCST, data_line.values[1:-1].tolist())
                    result = sql_cur.fetchone()

                    # If you find a match, put the key into the tcst_headers dataframe
                    if sql_cur.rowcount > 0:
                        tcst_headers.loc[tcst_headers.index[row_num], CN.TCST_HEADER_ID] = result[0]
                    else:
                        tcst_headers.loc[tcst_headers.index[row_num], CN.TCST_HEADER_ID] = \
                            row_num + next_header_id
            else:
                # When all new headers, add the next id to the row number/index to make a new key
                tcst_headers.loc[tcst_headers.tcst_header_id == CN.NO_KEY, CN.TCST_HEADER_ID] = \
                    tcst_headers.index + next_header_id

            # get just the new headers with their keys
            new_headers = tcst_headers[tcst_headers[CN.TCST_HEADER_ID] > (next_header_id - 1)]
            logger.info("New headers: %s rows", str(len(new_headers.index)))

            # Write any new headers out to the sql database
            if not new_headers.empty:
                sql_met.write_to_sql(new_headers, CN.TCST_HEADER_FIELDS, CN.TCST_HEADER,
                                     CN.INS_HEADER_TCST, tmp_dir, sql_cur, local_infile, logger)

            # put the header ids back into the dataframe of all the line data
            tcst_data = pd.merge(left=tcst_data, right=tcst_headers, on=CN.TCST_HEADER_KEYS[1:])
            # Merging with limited keys renames the version column, change it back
            if 'version_x' in tcst_data.columns:
                tcst_data = tcst_data.rename(columns={'version_x': CN.VERSION})
            # Clean out the headers working dataframes
            tcst_headers = tcst_headers.iloc[0:0]
            new_headers = new_headers.iloc[0:0]

        except (RuntimeError, TypeError, NameError, KeyError):
            logger.error("*** %s in write_tcst_data write tcst headers ***", sys.exc_info()[0])

        try:


            # --------------------
            # Write Line Data
            # --------------------

            # find all of the line types in the data
            line_types = tcst_data.line_type.unique()

            # process one kind of line data at a time
            for line_type in line_types:

                all_var = pd.DataFrame()
                list_var = []

                # use the UC line type to index into the list of table names
                line_table = CN.LINE_TABLES_TCST[CN.UC_LINE_TYPES_TCST.index(line_type)]

                # get the line data of just this type and re-index
                line_data = tcst_data[tcst_data[CN.LINE_TYPE] == line_type].copy()
                line_data.reset_index(drop=True, inplace=True)
                logger.info("%s: %s rows", line_type, str(len(line_data.index)))

                # change all Not Available numerical values to METviewer not available (-9999)
                # replace adepth and bdepth NA -> X
                if line_type == CN.TCMPR:
                    line_data['64'] = line_data['64'].replace(CN.NOTAV, 'X')
                    line_data['65'] = line_data['65'].replace(CN.NOTAV, 'X')

                # Change remaining NA values
                line_data = line_data.replace(CN.NOTAV, CN.MV_NOTAV)

                # Only variable length lines have a line_data_id
                if line_type in CN.VAR_LINE_TYPES_TCST:
                    # Get next valid line data id. Set it to zero (first valid id) if no records yet
                    next_line_id = \
                        sql_met.get_next_id(line_table, CN.LINE_DATA_ID, sql_cur, logger)
                    logger.debug("next_line_id is %s", next_line_id)

                    # try to keep order the same as MVLoad
                    line_data = line_data.sort_values(by=[CN.DATA_FILE_ID, CN.LINE_NUM],
                                                      ignore_index=True).copy()

                    line_data[CN.LINE_DATA_ID] = line_data.index + next_line_id

                    # index of the first column of the repeating variables
                    var_index = line_data.columns.get_loc(CN.LINE_VAR_COUNTER[line_type]) + 1

                    # process each variable line one at a time for different versions
                    for row_num, file_line in line_data.iterrows():
                        # how many sets of repeating variables
                        var_count = int(file_line[CN.LINE_VAR_COUNTER[line_type]])

                        # The number of variables in the repeats
                        var_repeats = CN.LINE_VAR_REPEATS[line_type]
                        # number of sets of variables times the number of variables in the sets
                        repeat_width = int(var_count * var_repeats)

                        # pull out just the repeating data
                        list_var_data = file_line.iloc[var_index:var_index + repeat_width]
                        # put it into the right number of rows and columns
                        var_data = \
                            pd.DataFrame(list_var_data.values.reshape(var_count, var_repeats))

                        # add on the first two fields - line data id, and i value
                        var_data.insert(0, CN.LINE_DATA_ID, file_line[CN.LINE_DATA_ID])
                        var_data.insert(1, 'i_value', var_data.index + 1)

                        # collect all of the variable data for a line type
                        list_var.append(var_data)

                    # end for row_num, file_line
                    if list_var:
                        all_var = pd.concat(list_var, ignore_index=True, sort=False)
                        list_var = []

                # write the lines out to a CSV file, and then load them into database
                if not line_data.empty:
                    sql_met.write_to_sql(line_data, CN.LINE_DATA_COLS_TCST[line_type], line_table,
                                         CN.LINE_DATA_Q[line_type], tmp_dir, sql_cur, local_infile, logger)
                    line_data = line_data.iloc[0:0]

                # if there are variable length records, write them out also
                if not all_var.empty:
                    all_var.columns = CN.LINE_DATA_VAR_FIELDS[line_type]
                    sql_met.write_to_sql(all_var, CN.LINE_DATA_VAR_FIELDS[line_type],
                                         CN.LINE_DATA_VAR_TABLES[line_type],
                                         CN.LINE_DATA_VAR_Q[line_type],
                                         tmp_dir, sql_cur, local_infile, logger)
                    all_var = all_var.iloc[0:0]

            # end for line_type

        except (RuntimeError, TypeError, NameError, KeyError):
            logger.error("*** %s in write_tcst_data write line data ***", sys.exc_info()[0])

        write_time_end = time.perf_counter()
        write_time = timedelta(seconds=write_time_end - write_time_start)

        logger.info("    >>> Write time Tcst: %s", str(write_time))

        logger.debug("[--- End write_tcst_data ---]")
