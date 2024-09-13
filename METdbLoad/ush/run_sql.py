#!/usr/bin/env python3

"""
Program Name: run_sql.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Connect and disconnect to/from a SQL database.
Parameters: N/A
Input Files: connection data
Output Files: N/A
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member
# constants exist in constants.py

import sys
import os
import time
from datetime import timedelta
import pymysql

from METdbLoad.ush import constants as CN
from METdbLoad.ush.met_db_load import DEFAULT_LOGLEVEL
from METreformat.util import get_common_logger

class RunSql:
    """ Class to connect and disconnect to/from a SQL database
        Returns:
           N/A
    """

    def __init__(self, logger=None):
        # Default to False since it requires extra permission
        self.local_infile = False
        self.conn = None
        self.cur = None
        if logger is None:
            self.logger = get_common_logger(DEFAULT_LOGLEVEL, 'stdout')
        else:
            self.logger = logger

    def sql_on(self, connection):
        """ method to connect to a SQL database
            Returns:
               N/A
        """

        if 'db_local_infile' in connection.keys() and connection['db_local_infile'].lower() == 'false':
            local_infile = False
        else:
            # Default behaviour 
            local_infile = True

        try:
            if (not 'db_host' in connection) or (not 'db_user' in connection):
                self.logger.error("XML Load file does not have enough connection tags")
                sys.exit("*** Error when connecting to database")

            # Connect to the database using connection info from XML file
            self.conn = pymysql.connect(host=connection['db_host'],
                                        port=connection['db_port'],
                                        user=connection['db_user'],
                                        passwd=connection['db_password'],
                                        db=connection['db_database'],
                                        local_infile=local_infile)

        except pymysql.OperationalError as pop_err:
            self.logger.error("*** %s in run_sql ***", str(pop_err))
            sys.exit("*** Error when connecting to database")

        try:

            self.cur = self.conn.cursor()

        except (RuntimeError, TypeError, NameError, KeyError, AttributeError):
            self.logger.error("*** %s in run_sql ***", sys.exc_info()[0])
            sys.exit("*** Error when creating cursor")

        # look at database to see whether we can use the local infile method
        self.cur.execute("SHOW GLOBAL VARIABLES LIKE 'local_infile';")
        result = self.cur.fetchall()
        db_infile = result[0][1]

        # Check that both the connection and the database support local_infile
        if db_infile == 'ON' and self.conn._local_infile:
            self.local_infile = 'ON'
        else:
            self.local_infile = 'OFF'
        self.logger.debug("local_infile is %s", self.local_infile)


    @staticmethod
    def sql_off(conn, cur):
        """ method to commit data and disconnect from a SQL database
            Returns:
               N/A
        """

        conn.commit()

        cur.close()
        conn.close()

    @staticmethod
    def get_next_id(table, field, sql_cur, logger):
        """ given a field for a table, find the max field value and return it plus one.
            Returns:
               next valid id to use in an id field in a table
        """
        # get the next valid id. Set it to zero (first valid id) if no records yet
        try:
            next_id = 0
            query_for_id = "SELECT MAX(" + field + ") from " + table
            sql_cur.execute(query_for_id)
            result = sql_cur.fetchone()
            if result[0] is not None:
                next_id = result[0] + 1
            return next_id

        except (RuntimeError, TypeError, NameError, KeyError, AttributeError):
            logger.error("*** %s in write_sql_data get_next_id ***", sys.exc_info()[0])


    @staticmethod
    def get_file_name(data_file_id, sql_cur, logger):
        """ given a data_file_id, return the matching filename.
            Returns:
               filename for a data_file_id
        """
        # get the filename
        try:
            file_name = None
            query_for_name = "SELECT filename from data_file where data_file_id = " + str(data_file_id)
            sql_cur.execute(query_for_name)
            result = sql_cur.fetchone()
            if result[0] is not None:
                file_name = result[0]
            return file_name

        except (RuntimeError, TypeError, NameError, KeyError, AttributeError):
            logger.error("*** %s in write_sql_data get_file_name ***", sys.exc_info()[0])
            
    @staticmethod
    def write_to_sql(raw_data, col_list, sql_table, sql_query, tmp_dir, sql_cur, local_infile, logger):
        """ given a dataframe of raw_data with specific columns to write to a sql_table,
            write to a csv file and use local data infile for speed if allowed.
            otherwise, do an executemany to use a SQL insert statement to write data
        """

        try:
            if local_infile == 'ON':
                # later in development, may wish to delete these files to clean up when done
                tmpfile = tmp_dir + '/METdbLoad_' + sql_table + '.csv'
                # write the data out to a csv file, use local data infile to load to database
                raw_data[col_list].to_csv(tmpfile, na_rep=CN.MV_NOTAV,
                                          index=False, header=False, sep=CN.SEP)
                sql_cur.execute(CN.LD_TABLE.format(tmpfile, sql_table, CN.SEP))
                # delete the temporary CSV file
                os.remove(tmpfile)
            else:
                # fewer permissions required, but slower
                # Make sure there are no NaN values
                raw_data = raw_data.fillna(CN.MV_NOTAV)

                # only line_data has timestamps in dataframe - change to strings
                if 'line_data' in sql_table:
                    raw_data['fcst_valid_beg'] = raw_data['fcst_valid_beg'].astype(str)
                    raw_data['fcst_valid_end'] = raw_data['fcst_valid_end'].astype(str)
                    raw_data['fcst_init_beg'] = raw_data['fcst_init_beg'].astype(str)
                    raw_data['obs_valid_beg'] = raw_data['obs_valid_beg'].astype(str)
                    raw_data['obs_valid_end'] = raw_data['obs_valid_end'].astype(str)
                elif sql_table in (CN.MODE_HEADER, CN.MTD_HEADER):
                    raw_data['fcst_valid'] = raw_data['fcst_valid'].astype(str)
                    raw_data['fcst_init'] = raw_data['fcst_valid'].astype(str)
                    raw_data['obs_valid'] = raw_data['fcst_init'].astype(str)
                # make a copy of the dataframe that is a list of lists and write to database
                dfile = raw_data[col_list].values.tolist()
                sql_cur.executemany(sql_query, dfile)

        except (RuntimeError, TypeError, NameError, KeyError, AttributeError):
            logger.error("*** %s in run_sql write_to_sql ***", sys.exc_info()[0])

    @staticmethod
    def apply_indexes(drop, sql_cur, logger):
        """
        If user sets tag apply_indexes to true, try to create all indexes
        If user sets tag drop_indexes to true, try to drop all indexes
        """
        logger.debug("[--- Start apply_indexes ---]")

        apply_time_start = time.perf_counter()

        try:
            if drop:
                sql_array = CN.DROP_INDEXES_QUERIES
                logger.info("--- *** --- Dropping Indexes --- *** ---")
            else:
                sql_array = CN.CREATE_INDEXES_QUERIES
                logger.info("--- *** --- Loading Indexes --- *** ---")

            for sql_cmd in sql_array:
                sql_cur.execute(sql_cmd)

        except (pymysql.OperationalError, pymysql.InternalError):
            if drop:
                logger.error("*** Index to drop does not exist in run_sql apply_indexes ***")
            else:
                logger.error("*** Index to add already exists in run_sql apply_indexes ***")

        apply_time_end = time.perf_counter()
        apply_time = timedelta(seconds=apply_time_end - apply_time_start)

        logger.info("    >>> Apply time: %s", str(apply_time))

        logger.debug("[--- End apply_indexes ---]")
