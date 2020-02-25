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
import logging
import time
from datetime import timedelta
import numpy as np
import pandas as pd

import constants as CN

from run_sql import RunSql

class WriteModeSql:
    """ Class to write mode files (cts and object) to a SQL database
        Returns:
           N/A
    """
    @staticmethod
    def write_mode_data(load_flags, cts_data, obj_data, sql_cur, local_infile):
        """ write mode files (cts and object) to a SQL database.
            Returns:
               N/A
        """

        logging.debug("[--- Start write_mode_sql ---]")

        write_time_start = time.perf_counter()

        try:

            sql_met = RunSql()

            # get the unique ode headers from cts_data and obj_data
            mode_headers = cts_data[CN.MODE_HEADER_KEYS].drop_duplicates()
            mode_headers = mode_headers.append(obj_data[CN.MODE_HEADER_KEYS].drop_duplicates(), ignore_index=True)
            mode_headers.drop_duplicates()
            mode_headers.reset_index(drop=True, inplace=True)

            # At first, we do not know if the headers already exist, so we have no keys
            mode_headers[CN.MODE_HEADER_ID] = CN.NO_KEY

            # get the next valid mode id. Set it to zero (first valid id) if no records yet
            next_header_id = sql_met.get_next_id(CN.MODE_HEADER, CN.MODE_HEADER_ID, sql_cur)

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_mode_sql ***", sys.exc_info()[0])


        write_time_end = time.perf_counter()
        write_time = timedelta(seconds=write_time_end - write_time_start)

        logging.info("    >>> Write time: %s", str(write_time))

        logging.debug("[--- End write_mode_sql ---]")
