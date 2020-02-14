#!/usr/bin/env python3

"""
Program Name: write_mode_sql.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Write mode files (cts and object) to a SQL database.
Parameters: N/A
Input Files: transformed dataframe of mode cts and object lines
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


class WriteModeSql:
    """ Class to write mode files (cts and object) to a SQL database
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

    def write_mode_data(self, load_flags, data_files, cts_data, obj_data, group, description,
                        load_note, xml_str):
        """ write mode files (cts and object) to a SQL database.
            Returns:
               N/A
        """

        logging.debug("[--- Start write_mode_sql ---]")

        write_time_start = time.perf_counter()

        try:
            # look at database to see whether we can use the local infile method
            self.cur.execute("SHOW GLOBAL VARIABLES LIKE 'local_infile';")
            result = self.cur.fetchall()
            self.local_infile = result[0][1]
            logging.debug("local_infile is %s", result[0][1])

            self.conn.commit()

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_mode_sql ***", sys.exc_info()[0])

        finally:
            self.cur.close()
            self.conn.close()

        write_time_end = time.perf_counter()
        write_time = timedelta(seconds=write_time_end - write_time_start)

        logging.info("    >>> Write time: %s", str(write_time))

        logging.debug("[--- End write_mode_sql ---]")
