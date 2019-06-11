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
# from pathlib import Path
import logging
# import numpy as np
# import pandas as pd
# import pymysql
from pymysql import connect

import constants as CN


class WriteStatSql:
    """ Class to write stat files (MET and VSDB) to a SQL database
        Returns:
           N/A
    """

    def __init__(self):
        logging.debug("Initializing WriteStatSql")

    def write_sql_data(self, connection, stat_data):
        """ write stat files (MET and VSDB) to a SQL database.
            Returns:
               N/A
        """

        logging.debug("--- Start write_sql_data ---")

        try:
            # Connect to the database
            self.conn = connect(host=connection['db_host'],
                                port=connection['db_port'],
                                user=connection['db_user'],
                                passwd=connection['db_password'],
                                db=connection['db_name'])

            cur = self.conn.cursor()

            cur.execute("SHOW GLOBAL VARIABLES LIKE 'local_infile';")
            result = cur.fetchall()
            logging.debug("local_infile is %s", result[0][1])

            stat_headers = stat_data[CN.STAT_HEADER_KEYS].drop_duplicates()
            stat_headers.reset_index(drop=True, inplace=True)

            line_types = stat_data.LINE_TYPE.unique()

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_sql_data ***", sys.exc_info()[0])

        logging.debug("--- End write_sql_data ---")
