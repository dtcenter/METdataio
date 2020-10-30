#!/usr/bin/env python3

"""
Program Name: write_mtd_sql.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Write MTD files (2D, 3D single, and 3D pair) to a SQL database.
Parameters: N/A
Input Files: transformed dataframe of MTD lines
Output Files: N/A
Copyright 2020 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member
# constants exist in constants.py

import sys
import logging
import time
from datetime import timedelta
import pandas as pd

import constants as CN

from run_sql import RunSql


class WriteMtdSql:
    """ Class to write MTD files to a SQL database
        Returns:
           N/A
    """
    @staticmethod
    def write_mtd_data(load_flags, m_2d_data, m_3d_single_data, m_3d_pair_data,
                       sql_cur, local_infile):
        """ write mtd files to a SQL database.
            Returns:
               N/A
        """

        logging.debug("[--- Start write_mtd_sql ---]")

        write_time_start = time.perf_counter()

        try:

            all_pair = pd.DataFrame()

            sql_met = RunSql()

            # --------------------
            # Write MTD Headers
            # --------------------

            # get the unique MTD headers
            if not m_2d_data.empty:
                mtd_headers = m_2d_data[CN.MTD_HEADER_FIELDS[1:]]
            if not m_3d_single_data.empty:
                mtd_headers = mtd_headers.append(m_3d_single_data[CN.MTD_HEADER_FIELDS[1:]],
                                                 ignore_index=True)
            if not m_3d_pair_data.empty:
                mtd_headers = mtd_headers.append(m_3d_pair_data[CN.MTD_HEADER_FIELDS[1:]],
                                                 ignore_index=True)
            # restore to original order now all MTD headers are recombined
            mtd_headers = mtd_headers.sort_values(by=[CN.DATA_FILE_ID, CN.LINENUMBER])
            # get unique values, keeping the first of the duplicate records
            mtd_headers.drop_duplicates(CN.MTD_HEADER_KEYS, keep='first', inplace=True)
            mtd_headers.reset_index(drop=True, inplace=True)

            # At first, we do not know if the headers already exist, so we have no keys
            mtd_headers[CN.MTD_HEADER_ID] = CN.NO_KEY

            # get the next valid MTD header id. Set it to zero (first valid id) if no records yet
            next_header_id = sql_met.get_next_id(CN.MTD_HEADER, CN.MTD_HEADER_ID, sql_cur)

            # if the flag is set to check for duplicate headers, get ids from existing headers
            if load_flags["mtd_header_db_check"]:

                # For each header, query with unique fields to try to find a match in the database
                for row_num, data_line in mtd_headers.iterrows():
                    data_line[CN.FCST_VALID] = \
                        data_line[CN.FCST_VALID].strftime("%Y-%m-%d %H:%M:%S")
                    data_line[CN.FCST_INIT] = data_line[CN.FCST_INIT].strftime("%Y-%m-%d %H:%M:%S")
                    data_line[CN.OBS_VALID] = data_line[CN.OBS_VALID].strftime("%Y-%m-%d %H:%M:%S")
                    # when n_valid and grid_res are null, query needs 'is null'
                    if data_line[CN.N_VALID] == CN.MV_NULL and data_line[CN.GRID_RES] == CN.MV_NULL:
                        sql_cur.execute(CN.QN_MHEADER,
                                        [data_line[CN.VERSION],
                                         data_line[CN.MODEL]] + data_line.values[7:-1].tolist())
                    else:
                        sql_cur.execute(CN.Q_MHEADER, data_line.values[3:-1].tolist())
                    result = sql_cur.fetchone()

                    # If you find a match, put the key into the mtd_headers dataframe
                    if sql_cur.rowcount > 0:
                        mtd_headers.loc[mtd_headers.index[row_num], CN.MTD_HEADER_ID] = result[0]
                    # otherwise create the next id and put it in
                    else:
                        mtd_headers.loc[mtd_headers.index[row_num], CN.MTD_HEADER_ID] = \
                            row_num + next_header_id
            else:
                # When all new headers, add the next id to the row number/index to make a new key
                mtd_headers.loc[mtd_headers.mtd_header_id == CN.NO_KEY, CN.MTD_HEADER_ID] = \
                    mtd_headers.index + next_header_id

            # get just the new headers with their keys
            new_headers = mtd_headers[mtd_headers[CN.MTD_HEADER_ID] > (next_header_id - 1)]
            logging.info("New MTD headers: %s rows", str(len(new_headers.index)))

            # Write any new headers out to the sql database
            if not new_headers.empty:
                sql_met.write_to_sql(new_headers, CN.MTD_HEADER_FIELDS, CN.MTD_HEADER,
                                     CN.INS_MHEADER, sql_cur, local_infile)
                new_headers = new_headers.iloc[0:0]

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_mtd_sql ***", sys.exc_info()[0])

        write_time_end = time.perf_counter()
        write_time = timedelta(seconds=write_time_end - write_time_start)

        logging.info("    >>> Write time MTD: %s", str(write_time))

        logging.debug("[--- End write_mtd_sql ---]")
