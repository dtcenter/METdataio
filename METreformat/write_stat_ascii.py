#!/usr/bin/env python3

"""
Program Name: write_stat_ascii.py
Contact(s):  Minna Win
Abstract:
History Log:  Initial version
Usage: Write stat files (MET) to an ASCII file.
Parameters: N/A
Input Files: transformed dataframe of MET lines
Output Files: N/A
Copyright 2022 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member
# constants exist in constants.py

import sys
import logging
import time
from datetime import timedelta
import numpy as np
import pandas as pd

import constants as cn


class WriteStatAscii:
    """ Class to write MET .stat files to an ASCII file
        Returns:
           N/A
    """

    def write_stat_ascii(self, load_flags, stat_data):
        """ write stat files (MET and VSDB) to an ASCII file.
            Returns:
               N/A

            Args:
                @param load_flags: flag values set in the XML spec file, indicating which items
                            are to be loaded
                @ param stat_data: pandas dataframe corresponding to the MET stat input file

            Returns:  None, write an output ASCII file associated with the original MET .stat file with statistics
                      information aggregated into these four columns: stat_name, stat_value, stat_ncl, stat_ncu,
                      stat_bcl, and stat_bcu (the stat_xyz are not available in all line types,
                      these will have values of NA)

        """

        logging.debug("[--- Start write_stat_data ---]")

        write_time_start = time.perf_counter()

        try:


            # --------------------
            # Write Stat Headers
            # --------------------

            # Create a generic set of headers, the common headers for all stat files
            # (columns 1-14, then create headers for the maximum number of allowable
            # MET stat headers).
            common_stat_headers = cn.LONG_HEADER
            line_types = list(stat_data['line_type'])
            unique_line_types = set(line_types)


            # Subset the stat_data dataframe by line types. Use a dictionary for keeping track of the
            # subsetted data based on the line type.  Keep track of the total number of rows to verify that
            # we haven't lost any rows in the process.
            # expected_num_rows = stat_data.shape[0]
            # num_rows = 0
            # df_by_line_type = {}
            # for idx, cur_line_type in enumerate(unique_line_types):
            #     df_by_line_type[cur_line_type] = (stat_data[stat_data['line_type'] == cur_line_type])
            #     num_rows = num_rows + df_by_line_type[cur_line_type].shape[0]

            # Verify that we didn't lose any rows of data while we were sub-setting based on the line types.
            # if num_rows != expected_num_rows:
            #     raise RuntimeError(
            #         "Total number of rows in subsetted data do not total the number of rows in the original"
            #         "data frame.")

            # For each line type, extract the statistics information and save it in a new dataframe
            expanded_headers = common_stat_headers + ['stat_name', 'stat_value', 'stat_bcl', 'stat_bcu', 'stat_ncl,'
                                                      'stat_ncu']
            all_var = pd.DataFrame(columns=expanded_headers)

            for idx, cur_line_type in enumerate(unique_line_types):
                if cur_line_type == cn.FHO:
                    fho_var = self.process_by_stat_linetype(cur_line_type, stat_data, all_var)

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_stat_ascii ***", sys.exc_info()[0])

        write_time_end = time.perf_counter()
        write_time = timedelta(seconds=write_time_end - write_time_start)

        logging.info("    >>> Write time Stat: %s", str(write_time))

        logging.debug("[--- End write_stat_data ---]")

    def process_by_stat_linetype(self, linetype, stat_data, linetype_data):
        '''
           For a given linetype, extract the relevant statistics information into the
           stat_name, stat_value, stat_bcl, stat_bcu, stat_ncl, and stat_ncu columns,
           along with the original data in the all_vars dataframe.

        Args:
            @param linetype: The linetype of interest (i.e. CNT, CTS, FHO, etc.)
            @param stat_data: The original MET data read in from the .stat file. Empty columns from the original .stat
                              file are named with the string representation of the numbers 1-n.
            @param linetype_data: The dataframe that will contain the data for the line type of interest,
                                  with the aggregated statistics information in the stat_name, stat_value,
                                  stat_ncl, stat_ncu, stat_bcu, and stat_bcl columns.

            @return: linetype_data
        '''

        # FHO forecast, hit rate, observation rate
        if linetype == cn.FHO:
            print(f"Currently processing FHO")
            # Extract the stat_names and stat_values for this line type:
            # F_RATE, H_RATE, O_RATE (these will be the stat name).  There are no corresponding xyz_bcl, xyz_bcu,
            # xyz_ncl, and xyz_ncu values where xyz = stat name

            #
            # Subset the stat_data dataframe into a smaller data frame containing only the FHO line type with all its
            # columns.
            #

            # Omit the fcst_init_beg column (column 6, zero-based count) that was added by the
            # read_data_files.py module since this isn't one of the columns in the FHO line type.
            #
            no_fcst_init_beg_column = np.append(np.arange(0,6), np.arange(7,25))

            # Also ignore the 'total' column (column 25, zero-based counting)
            fho_columns_to_use = np.append(no_fcst_init_beg_column, np.arange(26,29)).tolist()
            fho_df = stat_data[stat_data['line_type'] == linetype].iloc[:, fho_columns_to_use]

            # Add the stat columns for the FHO line type
            fho_columns = cn.LC_COMMON_STAT_HEADER + ['f_rate', 'h_rate', 'o_rate']
            fho_df.columns = fho_columns

            fho_df.to_csv("/Volumes/d1/minnawin/feature_121_met_reformatter/fho_df.csv")

            # Create another index column to preserve the original index values from the stat_data datframe
            idx = list(fho_df.index)
            fho_df.insert(loc=0, column='Idx', value=idx)

            # Use pandas 'melt' to reshape the data frame from wide to long shape (i.e. collecting the f_rate, h_rate,
            # and o_rate values and putting them under the column 'stat_value' corresponding to the 'stat_name' column
            # containing the names F_RATE, H_RATE, and O_RATE

            # columns that we don't want to change
            columns_to_use = fho_df.columns[0:-3]
            fho_copy = fho_df.copy(deep=True)
            linetype_data = pd.melt(fho_copy, id_vars=list(columns_to_use), var_name='stat_name', value_name='stat_value')

            # FHO line type doesn't have the bcl and bcu stat values set these to NA
            na_column = ['NA' for na_column in range(0, linetype_data.shape[0])]

            linetype_data['stat_ncl'] = na_column
            linetype_data['stat_ncu'] = na_column
            linetype_data['stat_bcl'] = na_column
            linetype_data['stat_bcu'] = na_column



        # CTC Contingency Table Counts

        # CTS Contingency Table Statistics

        # CNT Continuous Statistics

        # MCTC Multi-category Contingency Table Count

        # MCTS Multi-category Contingency Table Statistics

        # PCT Contingency Table Counts for Probabilistic forecasts

        # PSTD Contingency Table Statistics for Probabilistic forecasts

        # PJC Joint and Conditional factorization for Probabilistic forecasts

        # PRC Receiver Operating Characteristic for Probabilistic forecasts

        # ECLV Economic Cost/Loss Relative Value

        # SL1L2 Scalar Partial sums

        # SAL1L2 Scalar Anomaly Partial Sums

        # VL1L2 Vector Partial Sums

        # VAL1L2 Vector Anomaly Partial Sums

        # VCNT Vector Continuous Statistics

        # MPR Matched Pair

        # SEEPS_MPR Stable Equitable Error in Probability space of Matched Pair

        # SEEPS Stable Equitable Error in Probability Space



        return linetype_data

