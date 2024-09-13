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

import METdbLoad.ush.constants as CN

from METdbLoad.ush.run_sql import RunSql


class WriteMtdSql:
    """ Class to write MTD files to a SQL database
        Returns:
           N/A
    """
    @staticmethod
    def write_mtd_data(load_flags, m_2d_data, m_3d_single_data, m_3d_pair_data,
                       tmp_dir, sql_cur, local_infile):
        """ write mtd files to a SQL database.
            Returns:
               N/A
        """

        logging.debug("[--- Start write_mtd_sql ---]")

        write_time_start = time.perf_counter()

        try:

            sql_met = RunSql()

            mtd_headers = pd.DataFrame()
            new_headers = pd.DataFrame()

            # --------------------
            # Write MTD Headers
            # --------------------

            # get the unique MTD headers
            if not m_2d_data.empty:
                mtd_headers = m_2d_data[CN.MTD_HEADER_FIELDS[1:]]
            if not m_3d_single_data.empty:
                mtd_headers = pd.concat([mtd_headers,
                                        m_3d_single_data[CN.MTD_HEADER_FIELDS[1:]]],
                                        ignore_index=True, sort=False)
            if not m_3d_pair_data.empty:
                mtd_headers = pd.concat([mtd_headers,
                                        m_3d_pair_data[CN.MTD_HEADER_FIELDS[1:]]],
                                        ignore_index=True, sort=False)

            # get unique values, keeping the first of the duplicate records
            mtd_headers = mtd_headers.drop_duplicates(CN.MTD_2D_HEADER_KEYS, keep='first')
            mtd_headers.reset_index(drop=True, inplace=True)

            # make sure type of columns is consistent between headers and line data
            mtd_headers.fcst_lead = mtd_headers.fcst_lead.astype('int64')
            mtd_headers.obs_lead = mtd_headers.obs_lead.astype('int64')

            # At first, we do not know if the headers already exist, so we have no keys
            mtd_headers[CN.MTD_HEADER_ID] = CN.NO_KEY

            # get the next valid MTD header id. Set it to zero (first valid id) if no records yet
            next_header_id = sql_met.get_next_id(CN.MTD_HEADER, CN.MTD_HEADER_ID, sql_cur, logging)

            # if the flag is set to check for duplicate headers, get ids from existing headers
            if load_flags["mtd_header_db_check"]:

                # For each header, query with unique fields to try to find a match in the database
                for row_num, data_line in mtd_headers.iterrows():
                    data_line[CN.FCST_VALID] = \
                        data_line[CN.FCST_VALID].strftime("%Y-%m-%d %H:%M:%S")
                    data_line[CN.FCST_INIT] = data_line[CN.FCST_INIT].strftime("%Y-%m-%d %H:%M:%S")
                    if data_line[CN.OBS_VALID] != CN.MV_NULL:
                        data_line[CN.OBS_VALID] = \
                            data_line[CN.OBS_VALID].strftime("%Y-%m-%d %H:%M:%S")
                    if CN.MV_NULL not in data_line.values[4:-1].tolist():
                        sql_cur.execute(CN.Q_MTDHEADER, data_line.values[4:-1].tolist())
                    else:
                        sql_query = "SELECT mtd_header_id FROM mtd_header WHERE " + \
                                    "version=%s AND model=%s AND descr=%s AND fcst_lead=%s " + \
                                    "AND fcst_valid=%s AND fcst_init=%s AND obs_lead=%s "
                        data_values = data_line.values[4:11].tolist()
                        for mfield in CN.MTD_HEADER_KEYS[7:]:
                            if data_line[mfield] != CN.MV_NULL:
                                sql_query = sql_query + 'AND ' + mfield + '=%s '
                                data_values.append(data_line[mfield])
                            else:
                                sql_query = sql_query + 'AND ' + mfield + ' is NULL '
                        sql_cur.execute(sql_query, data_values)
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
            new_headers.obs_valid = pd.to_datetime(new_headers.obs_valid, errors='coerce')
            logging.info("New MTD headers: %s rows", str(len(new_headers.index)))

            # Write any new headers out to the sql database
            if not new_headers.empty:
                # If there are any 2D revision files
                if new_headers[CN.REVISION_ID].ne(CN.MV_NULL).any():
                    # numbered revision ids must have max revision id added to be unique
                    next_rev_id = sql_met.get_next_id(CN.MTD_HEADER, CN.REVISION_ID, sql_cur, logging)
                    new_headers.loc[new_headers.revision_id != CN.MV_NULL, CN.REVISION_ID] = \
                        new_headers.loc[new_headers.revision_id != CN.MV_NULL, CN.REVISION_ID] + \
                        next_rev_id
                new_headers.loc[new_headers.obs_valid.isnull(), CN.OBS_VALID] = CN.MV_NULL
                sql_met.write_to_sql(new_headers, CN.MTD_HEADER_FIELDS, CN.MTD_HEADER,
                                     CN.INS_MTDHEADER, tmp_dir, sql_cur, local_infile, logging)
                new_headers = new_headers.iloc[0:0]

            mtd_headers.obs_valid = pd.to_datetime(mtd_headers.obs_valid, errors='coerce')

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_mtd_sql write MTD headers ***", sys.exc_info()[0])

        try:
            # --------------------
            # Write Line Data
            # --------------------

            # write the lines out to a CSV file, and then load them into database
            if not m_2d_data.empty:
                # make sure type of columns is consistent between headers and line data
                m_2d_data.obs_valid = pd.to_datetime(m_2d_data.obs_valid,
                                                     errors='coerce')
                # put the header ids back into the dataframe
                m_2d_data = pd.merge(left=mtd_headers, right=m_2d_data, on=CN.MTD_2D_HEADER_KEYS)
                m_2d_data.loc[m_2d_data.obs_valid.isnull(), CN.OBS_VALID] = CN.MV_NULL
                
                # create defaults for flags
                m_2d_data[CN.SIMPLE_FLAG] = 1
                m_2d_data[CN.FCST_FLAG] = 0
                m_2d_data[CN.MATCHED_FLAG] = 0

                # Set simple flag to zero if object id starts with C
                if m_2d_data.object_id.str.startswith('C').any():
                    m_2d_data.loc[m_2d_data.object_id.str.startswith('C'),
                                  CN.SIMPLE_FLAG] = 0

                # Set fcst flag to 1 if object id contains an F
                if m_2d_data.object_id.str.contains('F').any():
                    m_2d_data.loc[m_2d_data.object_id.str.contains('F'),
                                  CN.FCST_FLAG] = 1

                # Set matched flag to 1 if object cat has neither underscore nor 000
                if ((~m_2d_data.object_cat.str.contains(CN.U_SCORE)).sum() > 0 and
                        (~m_2d_data.object_cat.str.contains(CN.T_ZERO)).sum() > 0):
                    m_2d_data.loc[~m_2d_data.object_cat.str.contains(CN.U_SCORE) &
                                  ~m_2d_data.object_cat.str.contains(CN.T_ZERO),
                                  CN.MATCHED_FLAG] = 1

                sql_met.write_to_sql(m_2d_data, CN.MTD_2D_OBJ_FIELDS, CN.MTD_2D_T,
                                     CN.INS_M2HEADER, tmp_dir, sql_cur, local_infile, logging)
                m_2d_data = m_2d_data.iloc[0:0]

            if not m_3d_single_data.empty:
                # make sure type of columns is consistent between headers and line data
                m_3d_single_data.fcst_lead = m_3d_single_data.fcst_lead.astype('int64')
                m_3d_single_data.obs_lead = m_3d_single_data.obs_lead.astype('int64')
                m_3d_single_data.obs_valid = pd.to_datetime(m_3d_single_data.obs_valid,
                                                            errors='coerce')

                # put the header ids back into the dataframe
                m_3d_single_data = pd.merge(left=mtd_headers, right=m_3d_single_data,
                                            on=CN.MTD_HEADER_KEYS)
                m_3d_single_data.loc[m_3d_single_data.obs_valid.isnull(), CN.OBS_VALID] = CN.MV_NULL

                # create defaults for flags
                m_3d_single_data[CN.SIMPLE_FLAG] = 1
                m_3d_single_data[CN.FCST_FLAG] = 0
                m_3d_single_data[CN.MATCHED_FLAG] = 0

                # Set simple flag to zero if object id starts with C
                if m_3d_single_data.object_id.str.startswith('C').any():
                    m_3d_single_data.loc[m_3d_single_data.object_id.str.startswith('C'),
                                         CN.SIMPLE_FLAG] = 0

                # Set fcst flag to 1 if object id contains an F
                if m_3d_single_data.object_id.str.contains('F').any():
                    m_3d_single_data.loc[m_3d_single_data.object_id.str.contains('F'),
                                         CN.FCST_FLAG] = 1

                # Set matched flag to 1 if object cat has neither underscore nor 000
                if (~m_3d_single_data.object_cat.str.contains(CN.U_SCORE)).sum() > 0:
                    if (~m_3d_single_data.object_cat.str.contains(CN.T_ZERO)).sum() > 0:
                        m_3d_single_data.loc[~m_3d_single_data.object_cat.str.contains(CN.U_SCORE) &
                                             ~m_3d_single_data.object_cat.str.contains(CN.T_ZERO),
                                             CN.MATCHED_FLAG] = 1

                sql_met.write_to_sql(m_3d_single_data, CN.MTD_3D_OBJ_SINGLE_FIELDS, CN.MTD_SINGLE_T,
                                     CN.INS_M3SHEADER, tmp_dir, sql_cur, local_infile, logging)
                m_3d_single_data = m_3d_single_data.iloc[0:0]

            if not m_3d_pair_data.empty:
                # make sure type of columns is consistent between headers and line data
                m_3d_pair_data.fcst_lead = m_3d_pair_data.fcst_lead.astype('int64')
                m_3d_pair_data.obs_lead = m_3d_pair_data.obs_lead.astype('int64')
                m_3d_pair_data.obs_valid = pd.to_datetime(m_3d_pair_data.obs_valid, 
                                                          errors='coerce')

                # put the header ids back into the dataframe
                m_3d_pair_data = pd.merge(left=mtd_headers, right=m_3d_pair_data,
                                          on=CN.MTD_HEADER_KEYS)
                m_3d_pair_data.loc[m_3d_pair_data.obs_valid.isnull(), CN.OBS_VALID] = CN.MV_NULL
                mtd_headers = mtd_headers.iloc[0:0]

                # create defaults for flags
                m_3d_pair_data[CN.SIMPLE_FLAG] = 1
                m_3d_pair_data[CN.MATCHED_FLAG] = 0

                # Set simple flag to zero if object id starts with C
                if m_3d_pair_data.object_id.str.startswith('C').any():
                    m_3d_pair_data.loc[m_3d_pair_data.object_id.str.startswith('C'),
                                       CN.SIMPLE_FLAG] = 0

                # Set matched flag to 1 if object cat has no 000
                if (~m_3d_pair_data.object_cat.str.contains(CN.T_ZERO)).sum() > 0:
                    m_3d_pair_data.loc[~m_3d_pair_data.object_cat.str.contains(CN.T_ZERO),
                                       CN.MATCHED_FLAG] = 1

                sql_met.write_to_sql(m_3d_pair_data, CN.MTD_3D_OBJ_PAIR_FIELDS, CN.MTD_PAIR_T,
                                     CN.INS_M3PHEADER, tmp_dir, sql_cur, local_infile, logging)
                m_3d_pair_data = m_3d_pair_data.iloc[0:0]

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_mtd_sql write line data ***", sys.exc_info()[0])

        write_time_end = time.perf_counter()
        write_time = timedelta(seconds=write_time_end - write_time_start)

        logging.info("    >>> Write time MTD: %s", str(write_time))

        logging.debug("[--- End write_mtd_sql ---]")
