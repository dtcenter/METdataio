#!/usr/bin/env python3

"""
Program Name: write_stat_ascii.py
Contact(s):  Minna Win
Abstract:
History Log:  Initial version (supports CNT, CTC, CTS, MCTC, and SL1L2 line types for
point_stat and grid_stat .stat files)
Usage: Write MET stat files (.stat) to an ASCII file with additional columns of
information.
Parameters: Requires a yaml configuration file
Input Files: transformed dataframe of MET lines
Output Files: A text file containing reformatted data
Copyright 2022 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado,
NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member
# constants exist in constants.py
import gc
import sys
import os
import logging
import time
import pathlib
from typing import List, Set
import numpy as np
import pandas as pd
import yaml
import re

import constants as cn
from METdbLoad.ush.read_load_xml import XmlLoadFile
from METdbLoad.ush.read_data_files import ReadDataFiles
import util

logger = logging.getLogger(__name__)

class WriteStatAscii:
    """ Class to write MET .stat files to an ASCII file that contains the reformatted input data

        Returns:
           a Pandas dataframe and creates an ascii file with reformatted data.
    """

    def __init__(self, parms):

        # Set up logging
        loglevel_setting = str(parms['log_level']).upper()
        log_levels = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO,
                      'WARNING': logging.WARNING, 'ERROR': logging.ERROR,
                      'CRITICAL': logging.CRITICAL}
        log_level = log_levels[loglevel_setting]
        log_directory = parms['log_directory']
        # Create log directory if it doesn't already exist.
        log_filename = (str(parms['log_filename'])).upper()
        if not os.path.exists(log_directory) and log_filename != 'STDOUT':
            os.mkdir(parms['log_directory'])

        full_log_filename = os.path.join(log_directory, log_filename)
        logger.setLevel(log_level)

        if log_filename.lower() == 'stdout':
            logging.basicConfig(level=log_level,
                                format='%(asctime)s||User:%('
                                       'user)s||%(funcName)s|| [%(levelname)s]: %('
                                       'message)s',
                                datefmt='%Y-%m-%d %H:%M:%S',
                                stream=sys.stdout)
        else:
            logging.basicConfig(level=log_level,
                                format=' %(asctime)s || %(module)s || User:%(user)s || %(levelname)s : '
                                       '%(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S',
                                filename = full_log_filename,
                                filemode = 'w')


    def write_stat_ascii(self, stat_data: pd.DataFrame, parms: dict) -> pd.DataFrame:
        """ For line types: FHO, CTC, CTS, SL1L2, ECNT, MCTS, and VCNT reformat the MET stat files (.stat) to another
            ASCII file with stat_name, stat_value,
            stat_bcl, stat_bcu, stat_ncl, and stat_ncu columns, converting the
            original data file from wide form to long form.

            For line types such as PCT: specific reformatting is required, based on the type of plot that is utilizing
            that data.


            Args:
                @param stat_data: pandas dataframe corresponding to the MET stat
                input file generated from the METdbLoad file reader
                @param parms:  The yaml configuration object (dictionary) containing
                the settings for output dir, output file

            Returns:
                  reformatted_df: pandas dataframe with original data reformatted into
                               'long' form, or the form required for specific plot types (e.g.
                               histograms, ROC, etc.)

                      Additionally, write an output ASCII file associated with the
                      original MET .stat file with statistics information aggregated
                      into these six columns: stat_name,
                      stat_value, stat_ncl, stat_ncu,
                      stat_bcl, and stat_bcu (the stat_xyz are not available in all
                      line types, these will have values of NA)

        """

        write_time_start: float = time.perf_counter()

        try:

            # -----------------------------------
            # Subset data to requested line type
            # ----------------------------------
            supported_linetypes = [cn.FHO, cn.CNT, cn.VCNT, cn.CTC,
                                   cn.CTS, cn.MCTS, cn.SL1L2, cn.ECNT, cn.PCT,
                                   cn.RHIST]

            # Different formats based on the line types. Most METplotpy plots accept the long format where
            # all stats are under the stat_name and stat_value columns and the confidence limits under the
            # stat_bcl/bcu, stat_ncl/ncu columns.  Other plots, like the histogram plots (rank, relative, probability)
            # and ROC diagrams require specific formatting.

            working_df = stat_data.copy(deep=True)
            linetype_requested = str( parms['line_type']).upper()
            if linetype_requested in supported_linetypes:
                working_df = working_df.loc[working_df['line_type'] == linetype_requested]
            else:
                logging.ERROR("Requested line type is currently not supported for reformatting")
                raise ValueError("Requested line type ", linetype_requested,  " is currently not supported for reformatting")

            # --------------------
            # Write Stat Headers
            # --------------------

            # Create a generic set of headers, the common headers for all stat files
            # (columns 1-14, then create headers for the maximum number of allowable
            # MET stat headers, for line types where this is known).
            # The FCST_INIT_BEG header is added in after the FCST_VALID_END
            # column, resulting in one additional column to the common header.


            # ------------------------------
            # Extract statistics information
            # ------------------------------
            # Based on the line type, extract the statistics information and save it in a
            # dataframe.

            # Setting to indicate whether .stat files were processed with MET stat-analysis (True)
            # or directly from the MET point-stat, grid-stat, or ensemble-stat tool (False)
            is_aggregated = parms['input_stats_aggregated']
            
            # Replace any nan records with 'NA'.  These nan values were set by the
            # METdbLoad read_data_files module.
            working_df = working_df.fillna('NA')
            begin_reformat = time.perf_counter()

            try:
                reformatted_df = self.process_by_stat_linetype(linetype_requested, working_df, is_aggregated)
            except NotImplementedError:
                sys.exit('NotImplementedError')


            end_reformat = time.perf_counter()
            reformat_time = end_reformat - begin_reformat
            msg = 'Reformatting took: ' + str(reformat_time) + ' seconds'
            logger.info(msg)

            # Write out to the tab-separated text file
            output_file = os.path.join(parms['output_dir'], parms['output_filename'])
            _: pd.DataFrame = reformatted_df.to_csv(output_file, index=None, sep='\t',
                                                  mode='a')

        except (RuntimeError, TypeError, NameError, KeyError, NotImplementedError):
            logging.error("*** %s in write_stat_ascii ***", sys.exc_info()[0])

        write_time_end: float = time.perf_counter()
        write_time = write_time_end - write_time_start

        logger.info("Total time to reformat and write ASCII: %s seconds", str(write_time))
        logger.debug("End write_stat_data ")

        return reformatted_df

    def process_by_stat_linetype(self, linetype: str, stat_data: pd.DataFrame, is_aggregated=True):
        """
        
           For MET .stat output, extract the relevant statistics information into the
           necessary format based on whether the data is already aggregated (via MET stat-analysis) or
           if the data is un-aggregated and requires the METcalcpy agg_stat module for performing the aggregation
           statistics calculations.  **NOTE** Support for reformatting into agg_stat's required input format is currently
           available for the *ECNT* linetype.  This support will be extended to the other supported linetypes. 
           

        Args:
            @param linetype: The linetype of interest (i.e. CNT, CTS, FHO, etc.)
            @param stat_data: The original MET data read in from the .stat file, containing only the requested linetype
                              rows.
            Empty columns from the original .stat
                              file are named with the string representation of the
                              numbers 1-n.
            @param is_aggregated: Default=True.
                                  Boolean to indicate whether input .stat files already have aggregated statistics
                                  computed (i.e. output from MET stat-analysis tool). True if MET stat-analysis was used,
                                  False if .stat files are directly from MET point-stat, grid-stat, or
                                  ensemble-stat tool.

            @return: linetype_data 

            If input .stat data is aggregated (via MET stat-analysis):
                The dataframe that is reshaped (from wide to
                long), now including the stat_name,
                stat_value, stat_bcl, stat_bcu, stat_ncl, and stat_ncu columns. If the
                requested linetype does not exist or isn't supported, return None.
            otherwise (.stat data NOT aggregated-.stat files direct output from MET point-stat, grid-stat, or ensemble-stat)
                Or, if data requires aggregation via METcalcpy agg_stat.py, then the input will be
                reformatted with columns corresponding to the linetype's statistics names.
        """

        # FHO forecast, hit rate, observation rate
        if linetype == cn.FHO:
            if is_aggregated:
                linetype_data: pd.DataFrame = self.process_fho(stat_data)
            else:
                linetype_data: pd.DataFrame = self.process_fho_for_agg(stat_data)

        # CNT Continuous Statistics
        elif linetype == cn.CNT:
            if is_aggregated:
                linetype_data: pd.DataFrame = self.process_cnt(stat_data)
            else:
                linetype_data: pd.DataFrame = self.process_cnt_for_agg(stat_data)

        # VCNT Continuous Statistics
        elif linetype == cn.VCNT:
            if is_aggregated:
                linetype_data: pd.DataFrame = self.process_vcnt(stat_data)
            else:
                linetype_data: pd.DataFrame = self.process_vcnt_for_agg(stat_data)

        # CTC Contingency Table Counts
        elif linetype == cn.CTC:
            if is_aggregated:
                linetype_data: pd.DataFrame = self.process_ctc(stat_data)
            else:
                linetype_data: pd.DataFrame = self.process_ctc_for_agg(stat_data)

        # CTS Contingency Table Statistics
        elif linetype == cn.CTS:
            if is_aggregated:
                linetype_data: pd.DataFrame = self.process_cts(stat_data)
            else:
                linetype_data: pd.DataFrame = self.process_cts_for_agg(stat_data)

        # MCTS Contingency Table Statistics
        elif linetype == cn.MCTS:
            if is_aggregated:
                linetype_data: pd.DataFrame = self.process_mcts(stat_data)
            else:
                linetype_data: pd.DataFrame = self.process_mcts_for_agg(stat_data)

        # SL1L2 Scalar Partial sums
        elif linetype == cn.SL1L2:
            if is_aggregated:
                linetype_data: pd.DataFrame = self.process_sl1l2(stat_data)
            else:
                linetype_data: pd.DataFrame = self.process_sl1l2_for_agg(stat_data)

        # VL1L2 Scalar Partial sums
        elif linetype == cn.VL1L2:
            if is_aggregated:
                linetype_data: pd.DataFrame = self.process_vl1l2(stat_data)
            else:
                linetype_data: pd.DataFrame = self.process_vl1l2_for_agg(stat_data)

        # ECNT Ensemble Continuous statistics
        elif linetype == cn.ECNT:
            if is_aggregated:
                linetype_data: pd.DataFrame = self.process_ecnt(stat_data)
            else:
                linetype_data: pd.DataFrame = self.process_ecnt_for_agg(stat_data)

        # PCT
        elif linetype == cn.PCT:
            if is_aggregated:
                linetype_data: pd.DataFrame = self.process_pct(stat_data)
            else:
                linetype_data: pd.DataFrame = self.process_pct_for_agg(stat_data)

        # RHIST (ranked histogram)
        elif linetype == cn.RHIST:
            if is_aggregated:
                linetype_data: pd.DataFrame = self.process_rhist(stat_data)
            else:
                linetype_data: pd.DataFrame = self.process_rhist_for_agg(stat_data)
        else:
            return None

        return linetype_data

    def process_pct(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        """
            Retrieve the PCT linetype data (Contingency count for probabilistic data) and reshape it
            (wide to long format) to enable METplotpy to ingest the data and generate ROC diagram plots.
            Take into account that this line type consists of a variable number
            of fields/columns that appear after the N_THRESH column (i.e. THRESH_i, OY_i, ON_i).

            Arguments:
            @param stat_data: Input data from MET .stat output represented as a dataframe.

            Returns:
            linetype_data: the input dataframe reformatted into long format

        """

        # Determine the columns for this line type
        linetype: str = cn.PCT


        #
        # Subset the input dataframe to include only the PCT columns and label the remaining
        # "unlabelled" (i.e. labelled with numbers after data is read in by METdbLoad)
        # columns/headers.
        #

        # Subset the dataframe to only the PCT line type rows
        # stat_data_copy: pd.DataFrame = stat_data.copy(deep=True)
        # Do not assume that the input data contains only the PCT lines.
        stat_data_copy = stat_data.loc[stat_data['line_type'] == cn.PCT]

        # Number of columns after the N_THRESH column (i.e. THRESH_i, RANK_i, BIN_i, etc.)
        num_repeating_col_labels: int = int(cn.LINE_VAR_REPEATS[linetype])

        #  Retrieve the value for N_THRESH, the number of thresholds
        num_thresh: int = int(stat_data_copy.iloc[0][cn.NUM_STATIC_PCT_COLS])

        # Add 1 for THRESH_N, the last threshold value column
        total_number_variable_columns = num_thresh * num_repeating_col_labels + 1

        # Add 1 for the TOTAL column to get the total number of columns for this line type
        total_number_relevant_columns = cn.NUM_STATIC_PCT_COLS + total_number_variable_columns + 1

        # Get a list of names of the columns that correspond to the PCT linetype for this data
        only_relevant_columns = stat_data_copy.columns.tolist()[0:total_number_relevant_columns]

        filtered_df = stat_data_copy[only_relevant_columns]
        headers = filtered_df.columns

        # Identify the common headers to be used in indexing the dataframe.
        common_list = headers[0:cn.NUM_STATIC_PCT_COLS].to_list()
        common_list[cn.NUM_STATIC_PCT_COLS - 1] = 'total'

        working_df = filtered_df.copy(deep=True)

        # Remove the stat_data_copy dataframe, it is no longer needed.
        del stat_data_copy
        gc.collect()

        # Replace the first two numbered labels (following the LINETYPE column) with the TOTAL and N_THRESH labels
        working_df.rename(columns={'0':'total', cn.LINE_VAR_COUNTER[cn.PCT]:'n_thresh'}, inplace=True)

        # Relabel the remaining numbered column headers
        last_column_name = len(working_df.columns) - cn.NUM_STATIC_PCT_COLS

        # The THRESH_n column is the last column
        thresh_n_col_name = 'thresh_' + str(num_thresh + 1)
        working_df.rename(columns={str(last_column_name):thresh_n_col_name}, inplace=True)

        # Relabel the repeating columns (THRESH_i, OY_i, ON_i)
        # column names are numbered '1','2','3',...,etc. Give them descriptive labels: thresh_1, oy_1, on_1, etc.
        ith_value_label = []
        column_name_value = int(cn.LINE_VAR_COUNTER[cn.PCT]) + 1

        for i in range(int(cn.LINE_VAR_COUNTER[cn.PCT]), int(num_thresh) + 1):
            for column in cn.LC_PCT_VARIABLE_HEADERS:
                column_name = str(column_name_value)
                column_label = "{label}_{idx}".format(label=column, idx=i)
                working_df.rename(columns={column_name:column_label}, inplace=True)
                column_name_value += 1

            # Add a list used to facilitate creating the value_i column when reformatting.
            ith_value_label.append("{label}_{idx}".format(label="value", idx=i))

        # Create a dataframe consisting only of the value_1, ..., value_n values and their corresponding index values
        # and concat to the working_df.
        num_rows = working_df.shape[0]

        # Create a dictionary of values corresponding to each value_1, value_2, etc 'key'
        value_i_dict = {}

        # Create a dataframe with the same number of rows
        # as the working_df dataframe to enable concatenation.

        for label in ith_value_label:
            values_list = []
            match = re.match(r'(value_)(\d+)', label)
            ith_value = int(match.group(2))

            for i in range(1, num_rows + 1):
                values_list.append(ith_value)

            value_i_dict[label] = values_list

        # Create the dataframe of the value_i values.
        value_df = pd.DataFrame.from_dict(value_i_dict)

        # Reindex working_df to match the value_df index. This ensures correct concatenation of
        # the working_df with the value_df
        working_df_reindexed = working_df.reset_index(drop=False)
        working_df_reindexed = pd.concat([working_df_reindexed, value_df], axis=1)

        # Clean up working_df dataframe, it is no longer needed
        del working_df
        del value_df
        gc.collect()

        # Now reformat the columns so all thresh_1, thresh_2, ..., etc values go under the thresh_i column,
        # the oy_1, oy_2, ..., etc. values go under the oy_i column, and the on_1, on_2, ...,etc. values
        # go under the on_i column.  The corresponding threshold level/number/index for the thresh_i, oy_i and on_i
        # columns goes under the i_value column (ie threshold 1,..., n).

        # Work on a copy of the working_df to avoid working on a fragmented dataframe (i.e. avoid the
        # PerformanceWarning).
        working_copy_df = working_df_reindexed.copy(deep=True)

        # Clean up
        del working_df_reindexed
        gc.collect()

        thresh_cols = []
        oy_cols = []
        on_cols = []
        i_value = []
        working_headers = working_copy_df.columns.to_list()
        remaining_columns = working_headers[cn.NUM_STATIC_PCT_COLS:]
        for cur in remaining_columns:
            match_thresh = re.match(r'(thresh_)(\d+)', cur)
            match_oy = re.match(r'(oy_)(\d+)', cur)
            match_on = re.match(r'(on_)(\d+)', cur)
            match_val = re.match(r'(value_)(\d+)', cur)
            if match_thresh:
                thresh_cols.append(cur)
            elif match_oy:
                oy_cols.append(cur)
            elif match_on:
                on_cols.append(cur)
            elif match_val:
                i_value.append(cur)

        # The last threshold value, thresh_n isn't used, remove it from the list
        thresh_cols = thresh_cols[:-1]

        # Now apply melt to get the thresh_i, oy_i, on_i columns, and i_value column
        # include the n_thresh column for indexing.
        common_list.append('n_thresh')
        df_thresh  = working_copy_df.melt(id_vars=common_list, value_vars=thresh_cols, var_name='thresh', value_name='thresh_i')
        df_oy = working_copy_df.melt(id_vars=common_list, value_vars=oy_cols, var_name='oy', value_name='oy_i')
        df_on = working_copy_df.melt(id_vars=common_list, value_vars=on_cols, var_name='on', value_name='on_i')
        df_values = working_copy_df.melt(id_vars=common_list, value_vars=i_value, var_name='values', value_name='i_value')

        # Drop the unused var_names in the melted dataframes
        df_thresh.drop('thresh', axis=1, inplace=True)
        df_oy.drop('oy', axis=1, inplace=True)
        df_on.drop('on', axis=1, inplace=True)
        df_values.drop('values', axis=1, inplace=True)

        # Reindex to use the common columns before concatenating the melted dataframes to avoid duplication of
        # common columns.
        df_thresh_reindex = df_thresh.set_index(common_list, drop=True, append=False, inplace=False)
        df_oy_reindex = df_oy.set_index(common_list, drop=True, append=False, inplace=False)
        df_on_reindex = df_on.set_index(common_list, drop=True, append=False, inplace=False)
        df_values_reindex = df_values.set_index(common_list, drop=True, append=False, inplace=False)
        reformatted_df = pd.concat([df_thresh_reindex, df_oy_reindex, df_on_reindex, df_values_reindex], axis=1)

        # clean up
        del working_copy_df
        gc.collect()

        # reset the index so all columns are same level
        reformatted_df = reformatted_df.reset_index(drop=False)

        # Convert the n_thresh values to integers
        reformatted_df = reformatted_df.astype({"n_thresh":np.int16})

        return reformatted_df

    def process_pct_for_agg(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError

    def process_rhist(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        """
            Retrieve the RHIST linetype data (Ranked histogram, from the MET ensemble-stat tool) and reshape it
            (wide to long format) to enable METplotpy to ingest the data and generate ranked histogram plots.
            Take into account that this line type consists of a variable number
            of RANK_i columns that appear after the N_RANK column (i.e. RANK_1, RANK_2,..., RANK_n where n= the number
            of possible ranks).

            Arguments:
            @param stat_data: Input data from MET .stat output represented as a dataframe.

            Returns:
            linetype_data: the input dataframe reformatted into long format

        """

        # Determine the columns for this line type
        linetype: str = cn.RHIST


        #
        # Subset the input dataframe to include only the RHIST columns and label the remaining
        # "unlabelled" (i.e. labelled with numbers after data is read in by METdbLoad)
        # columns/headers.
        #

        # Subset the dataframe to only the RHIST line type rows
        # stat_data_copy: pd.DataFrame = stat_data.copy(deep=True)
        # Do not assume that the input data contains only the RHIST lines.
        stat_data_copy = stat_data.loc[stat_data['line_type'] == cn.RHIST]

        # Number of columns after the N_RANK column (RANK_1, ..., RANK_n)
        num_repeating_col_labels: int = int(cn.LINE_VAR_REPEATS[linetype])

        #  Retrieve the value for N_RANK, the number of possible ranks
        num_rank:int = int(stat_data_copy.iloc[0][cn.NUM_STATIC_RHIST_COLS])

        # Add 1 for the TOTAL column to get the total number of columns for this line type
        total_number_relevant_columns = cn.NUM_STATIC_RHIST_COLS + num_rank * num_repeating_col_labels + 1

        # Get a list of names of the columns that correspond to the RHIST linetype for this data
        only_relevant_columns = stat_data_copy.columns.tolist()[0:total_number_relevant_columns]

        filtered_df = stat_data_copy[only_relevant_columns]
        headers = filtered_df.columns

        # Identify the common headers to be used in indexing the dataframe.
        common_list = headers[0:cn.NUM_STATIC_RHIST_COLS].to_list()
        common_list[cn.NUM_STATIC_RHIST_COLS - 1] = 'total'

        working_df = filtered_df.copy(deep=True)

        # Remove the stat_data_copy dataframe, it is no longer needed.
        del stat_data_copy
        gc.collect()

        # Replace the first two numbered labels (following the LINETYPE column) with the TOTAL and N_RANK labels
        working_df.rename(columns={'0':'total', cn.LINE_VAR_COUNTER[cn.RHIST]:'n_rank'}, inplace=True)

        # Relabel the remaining numbered column headers
        last_column_name = len(working_df.columns) - cn.NUM_STATIC_RHIST_COLS

        # Relabel the repeating columns (RANK_1, ..., RANK_n)
        # column names are numbered '1','2','3',...,etc. by METdbLoad.
        # Give them descriptive labels: rank_1, rank_2, etc.
        ith_value_label = []
        column_name_value = int(cn.LINE_VAR_COUNTER[cn.RHIST]) + 1

        for i in range(int(cn.LINE_VAR_COUNTER[cn.RHIST]), int(num_rank) + 1):
            for column in cn.LC_RHIST_VARIABLE_HEADERS:
                column_name = str(column_name_value)
                column_label = "{label}_{idx}".format(label=column, idx=i)
                working_df.rename(columns={column_name:column_label}, inplace=True)
                column_name_value += 1

            # Add a list used to facilitate creating the value_i column when reformatting.
            ith_value_label.append("{label}_{idx}".format(label="value", idx=i))

        # Create a dataframe consisting only of the value_1, ..., value_n values and their corresponding index values
        # and concat to the working_df.
        num_rows = working_df.shape[0]

        # Create a dictionary of values corresponding to each value_1, value_2, etc. 'key'
        value_i_dict = {}

        # Create a dataframe with the same number of rows
        # as the working_df dataframe to enable concatenation.

        for label in ith_value_label:
            values_list = []
            match = re.match(r'(value_)(\d+)', label)
            ith_value = int(match.group(2))

            for i in range(1, num_rows + 1):
                values_list.append(ith_value)

            value_i_dict[label] = values_list

        # Create the dataframe of the value_i values.
        value_df = pd.DataFrame.from_dict(value_i_dict)

        # Reindex working_df to match the value_df index. This ensures correct concatenation of
        # the working_df with the value_df
        working_df_reindexed = working_df.reset_index(drop=False)
        working_df_reindexed = pd.concat([working_df_reindexed, value_df], axis=1)

        # Clean up working_df dataframe, it is no longer needed
        del working_df
        del value_df
        gc.collect()

        # Now reformat the columns so all rank_1, rank_2, ..., etc values go under the rank_i column,
        # The corresponding rank level/number/index for the rank_i
        # columns goes under the i_value column

        # Work on a copy of the working_df to avoid working on a fragmented dataframe (i.e. avoid the
        # PerformanceWarning).
        working_copy_df = working_df_reindexed.copy(deep=True)

        # Clean up
        del working_df_reindexed
        gc.collect()

        rank_cols = []
        i_value = []
        working_headers = working_copy_df.columns.to_list()
        remaining_columns = working_headers[cn.NUM_STATIC_RHIST_COLS:]
        for cur in remaining_columns:
            match_rank = re.match(r'(rank_)(\d+)', cur)
            match_val = re.match(r'(value_)(\d+)', cur)
            if match_rank:
                rank_cols.append(cur)
            elif match_val:
                i_value.append(cur)

        # Now apply melt to get the rank_i and i_value columns
        # include the n_rank column for indexing.
        common_list.append('n_rank')
        df_rank  = working_copy_df.melt(id_vars=common_list, value_vars=rank_cols, var_name='rank', value_name='rank_i')
        df_values = working_copy_df.melt(id_vars=common_list, value_vars=i_value, var_name='values', value_name='i_value')

        # Drop the unused var_names in the melted dataframes
        df_rank.drop('rank', axis=1, inplace=True)
        df_values.drop('values', axis=1, inplace=True)

        # Reindex to use the common columns before concatenating the melted dataframes to avoid duplication of
        # common columns.
        df_rank_reindex = df_rank.set_index(common_list, drop=True, append=False, inplace=False)
        df_values_reindex = df_values.set_index(common_list, drop=True, append=False, inplace=False)
        reformatted_df = pd.concat([df_rank_reindex, df_values_reindex], axis=1)

        # clean up
        del working_copy_df
        gc.collect()

        # reset the index so all columns are same level
        reformatted_df = reformatted_df.reset_index(drop=False)

        # Convert the n_rank values to integers
        reformatted_df = reformatted_df.astype({"n_rank":np.int16})

        return reformatted_df

    def process_rhist_for_agg(self, stat_data: pd.DataFrame) -> pd.DataFrame:

        raise NotImplementedError

    def process_fho(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        """
             Retrieve the FHO line type data and reshape it to replace the original
             columns (based on column number) into
             stat_name, stat_value, stat_bcl, stat_bcu, stat_ncu, and stat_ncl

             Arguments:
             @param stat_data: The dataframe containing the data from
             the MET .stat file.

             Returns:
             linetype_data:  The dataframe with the reshaped data for the FHO line type
         """

        # Extract the stat_names and stat_values for this line type:
        # F_RATE, H_RATE, O_RATE (these will be the stat name).  There are no
        # corresponding xyz_bcl, xyz_bcu,
        # xyz_ncl, and xyz_ncu values where xyz = stat name

        #
        # Subset the stat_data dataframe into a smaller data frame containing only
        # the FHO line type with all its
        # columns.
        #

        # Relevant columns for the FHO line type
        linetype: str = cn.FHO
        end = cn.NUM_STAT_FHO_COLS
        fho_columns_to_use: List[str] =\
            np.arange(0, end).tolist()

        # Subset original dataframe to another dataframe consisting of only the FHO
        # line type.
        fho_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                               fho_columns_to_use]

        # Add the stat columns header names for the FHO line type
        fho_columns: List[str] = cn.FHO_FULL_HEADER
        fho_df.columns: List[str] = fho_columns

        # Create another index column to preserve the index values from the stat_data
        # dataframe (ie the dataframe
        # containing the original data from the MET output file).
        idx = list(fho_df.index)

        # Work on a copy of the fho_df dataframe to avoid a possible PerformanceWarning
        # message due to a fragmented dataframe.
        fho_df_copy = fho_df.copy()
        fho_df_copy.insert(loc=0, column='Idx', value=idx)

        # Use pandas 'melt' to reshape the data frame from wide to long shape (i.e.
        # collecting the f_rate, h_rate,
        # and o_rate values and putting them under the column 'stat_value'
        # corresponding to the 'stat_name' column
        # containing the names F_RATE, H_RATE, and O_RATE

        # columns that we don't want to change (the last three columns are the stat
        # columns of interest,
        # we want to capture that information into the stat_name and stat_values
        # columns)
        columns_to_use: List[str] = fho_df_copy.columns[0:-3].tolist()
        fho_copy: pd.DataFrame = fho_df_copy.copy(deep=True)
        linetype_data: pd.DataFrame = pd.melt(fho_copy, id_vars=columns_to_use,
                                              var_name='stat_name',
                                              value_name='stat_value')

        # FHO line type doesn't have the bcl and bcu stat values; set these to NA
        na_column: List[str] = ['NA' for _ in range(0, linetype_data.shape[0])]

        linetype_data['stat_ncl']: pd.Series = na_column
        linetype_data['stat_ncu']: pd.Series = na_column
        linetype_data['stat_bcl']: pd.Series = na_column
        linetype_data['stat_bcu']: pd.Series = na_column

        return linetype_data

    def process_fho_for_agg(self, stat_data: pd.DataFrame) -> pd.DataFrame:

        raise NotImplementedError

    def process_cnt(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        """
           Reshape the data from the original MET output file (stat_data) into new
           statistics columns:
           stat_name, stat_value, stat_ncl, stat_ncu, stat_bcl, and stat_bcu
           specifically for the CNT line type data.

           Arguments:
           @param stat_data: the dataframe containing data from the MET .stat
           file.

           Returns:
           linetype_data: the reshaped pandas dataframe with statistics and
           confidence level data reorganized into the
                          stat_name, stat_value, stat_ncl, stat_ncu, stat_bcl,
                          and stat_bcu columns.

        """

        # Relevant columns for the CNT line type
        linetype: str = cn.CNT
        end = cn.NUM_STAT_CNT_COLS
        cnt_columns_to_use: List[str] =\
            np.arange(0, end).tolist()

        # Subset original dataframe to one containing only the CNT data
        cnt_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                               cnt_columns_to_use]

        # Add the stat columns for the CNT line type
        cnt_columns: List[str] = cn.FULL_CNT_HEADER
        cnt_df.columns: List[str] = cnt_columns

        # Create another index column to preserve the index values from the stat_data
        # dataframe (ie the dataframe
        # containing the original data from the MET output file).
        idx = list(cnt_df.index)

        # Work on a copy of the cnt_df dataframe to avoid a possible PerformanceWarning
        # message due to a fragmented dataframe.
        cnt_df_copy = cnt_df.copy()
        cnt_df_copy.insert(loc=0, column='Idx', value=idx)

        # Use the pd.wide_to_long() to collect the statistics and confidence level
        # data into the appropriate columns.
        # Rename the <stat_group>_BCL|BCU|NCL|NCU to BCL|BCU|NCL|NCU_<stat_group> in
        # order to
        # use pd.wide_to_long().

        # Rename confidence level column header names so the BCL, BCU, NCL, and NCU
        # are appended with the statistic name
        # (i.e. from FBAR_BCU to BCU_FBAR to be able to use the pandas wide_to_long).
        confidence_level_columns_renamed: List[str] = (
            self.rename_confidence_level_columns(cnt_df_copy.columns.tolist()))
        cnt_df_copy.columns: List[str] = confidence_level_columns_renamed

        # Rename the statistics columns (ie. FBAR, MAE, FSTDEV, etc. to STAT_FBAR,
        # STAT_MAE, etc.)
        stat_confidence_level_columns_renamed = self.rename_statistics_columns(
            cnt_df_copy, cn.CNT_STATISTICS_HEADERS)
        cnt_df_copy.columns = stat_confidence_level_columns_renamed

        # Get the name of the columns to be used for indexing, this will also
        # preserve the ordering of columns from the
        # original data.
        indexing_columns = ['Idx'] + cn.LC_COMMON_STAT_HEADER + ['total']

        wide_to_long_df: pd.DataFrame = pd.wide_to_long(cnt_df_copy,
                                                        stubnames=['STAT', 'NCL', 'NCU',
                                                                   'BCL', 'BCU'],
                                                        i=indexing_columns,
                                                        j='stat_name',
                                                        sep='_',
                                                        suffix='.+'
                                                        ).sort_values('Idx')

        # Rename the BCL, BCU, NCL, NCU, and STAT columns to stat_bcl, stat_bcu,
        # stat_ncl, stat_ncu, and stat_value
        # respectively.
        wide_to_long_df = wide_to_long_df.reset_index()

        renamed_wide_to_long_df: pd.DataFrame = wide_to_long_df.rename(
            columns={'BCL': 'stat_bcl', 'BCU': 'stat_bcu', 'NCL': 'stat_ncl',
                     'NCU': 'stat_ncu', 'STAT': 'stat_value'})

        # Some statistics in the CNT line type only have confidence level confidence
        # limits (ie. no normal confidence limits).
        # Set any nan stat_ncl and stat_ncu records to 'NA'
        linetype_data: pd.DataFrame = renamed_wide_to_long_df.fillna('NA')

        return linetype_data

    def process_cnt_for_agg(self, stat_data: pd.DataFrame) -> pd.DataFrame:

        raise NotImplementedError

    def process_vcnt(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        """
           Reshape the data from the original MET output file (stat_data) into new
           statistics columns:
           stat_name, stat_value, stat_ncl, stat_ncu, stat_bcl, and stat_bcu
           specifically for the VCNT line type data.

           Arguments:
           @param stat_data: the dataframe containing all the VCNT data from the MET .stat
           file.

           Returns:
           linetype_data: the reshaped pandas dataframe with statistics and
           confidence level data reorganized into the
                          stat_name, stat_value, stat_ncl, stat_ncu, stat_bcl,
                          and stat_bcu columns.

        """

        # Relevant columns for the VCNT line type
        linetype: str = cn.VCNT
        end = cn.NUM_STAT_VCNT_COLS
        vcnt_columns_to_use: List[str] = \
            np.arange(0, end).tolist()

        # Subset original dataframe to one containing only the VCNT data
        vcnt_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                               vcnt_columns_to_use]

        # Add the stat columns for the CNT line type
        vcnt_columns: List[str] = cn.FULL_VCNT_HEADER
        vcnt_df.columns: List[str] = vcnt_columns

        # Create another index column to preserve the index values from the stat_data
        # dataframe (ie the dataframe
        # containing the original data from the MET output file).
        idx = list(vcnt_df.index)

        # Work on a copy of the cnt_df dataframe to avoid a possible PerformanceWarning
        # message due to a fragmented dataframe.
        vcnt_df_copy = vcnt_df.copy()
        vcnt_df_copy.insert(loc=0, column='Idx', value=idx)

        # Use the pd.wide_to_long() to collect the statistics and confidence level
        # data into the appropriate columns.
        # Rename the <stat_group>_BCL|BCU|NCL|NCU to BCL|BCU|NCL|NCU_<stat_group> in
        # order to
        # use pd.wide_to_long().

        # Rename confidence level column header names so the BCL, BCU, NCL, and NCU
        # are appended with the statistic name
        # (i.e. from FBAR_BCU to BCU_FBAR to be able to use the pandas wide_to_long).
        confidence_level_columns_renamed: List[str] = (
            self.rename_confidence_level_columns(vcnt_df_copy.columns.tolist()))
        vcnt_df_copy.columns: List[str] = confidence_level_columns_renamed

        # Rename the statistics columns (ie. FBAR, MAE, FSTDEV, etc. to STAT_FBAR,
        # STAT_MAE, etc.)
        stat_confidence_level_columns_renamed = self.rename_statistics_columns(
            vcnt_df_copy, cn.VCNT_STATISTICS_HEADERS)
        vcnt_df_copy.columns = stat_confidence_level_columns_renamed

        # Get the name of the columns to be used for indexing, this will also
        # preserve the ordering of columns from the
        # original data.
        indexing_columns = ['Idx'] + cn.LC_COMMON_STAT_HEADER + ['total']

        wide_to_long_df: pd.DataFrame = pd.wide_to_long(vcnt_df_copy,
                                                        stubnames=['STAT', 'NCL', 'NCU',
                                                                   'BCL', 'BCU'],
                                                        i=indexing_columns,
                                                        j='stat_name',
                                                        sep='_',
                                                        suffix='.+'
                                                        ).sort_values('Idx')

        # Rename the BCL, BCU, NCL, NCU, and STAT columns to stat_bcl, stat_bcu,
        # stat_ncl, stat_ncu, and stat_value
        # respectively.
        wide_to_long_df = wide_to_long_df.reset_index()

        renamed_wide_to_long_df: pd.DataFrame = wide_to_long_df.rename(
            columns={'BCL': 'stat_bcl', 'BCU': 'stat_bcu', 'NCL': 'stat_ncl',
                     'NCU': 'stat_ncu', 'STAT': 'stat_value'})

        # Some statistics in the CNT line type only have confidence level confidence
        # limits (ie. no normal confidence limits).
        # Set any nan stat_ncl and stat_ncu records to 'NA'
        linetype_data: pd.DataFrame = renamed_wide_to_long_df.fillna('NA')

        return linetype_data

    def process_vcnt_for_agg(self, stat_data: pd.DataFrame) -> pd.DataFrame:

        raise NotImplementedError

    def process_ctc(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        """
             Reshape the data from the original MET output file (stat_data) into new
             statistics columns:
             stat_name, stat_value specifically for the CTC line type data.

             Arguments:
             @param stat_data: the dataframe containing all the data from the MET
             .stat file.

             Returns:
                 linetype_data: the reshaped pandas dataframe with statistics data
                 reorganized into the stat_name and
                                stat_value, stat_ncl, stat_ncu, stat_bcl,
                                and stat_bcu columns.

        """

        # Relevant columns for the CTC line type
        linetype: str = cn.CTC
        end = cn.NUM_STAT_CTC_COLS
        ctc_columns_to_use: List[str] =\
            np.arange(0, end).tolist()

        # Subset original dataframe to one containing only the CTC data
        ctc_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                               ctc_columns_to_use]

        # Add the stat columns header names for the CTC line type
        ctc_columns: List[str] = cn.CTC_HEADERS
        ctc_df.columns: List[str] = ctc_columns

        # Create another index column to preserve the index values from the stat_data
        # dataframe (ie the dataframe
        # containing the original data from the MET output file).
        idx = list(ctc_df.index)

        # Work on a copy of the ctc_df dataframe to avoid a possible PerformanceWarning
        # message due to a fragmented dataframe.
        ctc_df_copy = ctc_df.copy()
        ctc_df_copy.insert(loc=0, column='Idx', value=idx)

        # Now apply melt to get the stat_name and stat_values from the statistics

        # Columns we don't want to stack (i.e. treat these columns as a multi index)
        id_vars_list = ['Idx'] + cn.LC_COMMON_STAT_HEADER + ['total']
        linetype_data = ctc_df_copy.melt(id_vars=id_vars_list,
                                         value_vars=cn.CTC_STATISTICS_HEADERS,
                                         var_name='stat_name',
                                         value_name='stat_value').sort_values('Idx')

        # CTC line type doesn't have the ncl, ncu, bcl and bcu stat values set these
        # to NA
        na_column: List[str] = ['NA' for _ in range(0, linetype_data.shape[0])]

        linetype_data['stat_ncl']: pd.Series = na_column
        linetype_data['stat_ncu']: pd.Series = na_column
        linetype_data['stat_bcl']: pd.Series = na_column
        linetype_data['stat_bcu']: pd.Series = na_column

        return linetype_data

    def process_ctc_for_agg(self, stat_data: pd.DataFrame) -> pd.DataFrame:

        raise NotImplementedError


    def process_cts(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        """
             Reshape the data from the original MET output file (stat_data) into new
             statistics columns:
             stat_name, stat_value specifically for the CTS (Contingency Table
             Statistic) line type data.

             Arguments:
             @param stat_data: the dataframe containing all the data from the MET
             .stat file.

             Returns:
                 linetype_data: the reshaped pandas dataframe with statistics data
                 reorganized into the stat_name and
                                stat_value, stat_ncl, stat_ncu, stat_bcl,
                                and stat_bcu columns.

        """

        # Relevant columns for the CTS line type
        linetype: str = cn.CTS
        end = cn.NUM_STAT_CTS_COLS
        cts_columns_to_use: List[str] =\
            np.arange(0, end).tolist()

        # Subset original dataframe to one containing only the CTS data
        cts_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                               cts_columns_to_use]

        # Add all the columns header names for the CTS line type
        cts_columns: List[str] = cn.CTS_SPECIFIC_HEADERS
        cts_df.columns: List[str] = cts_columns

        # Create another index column to preserve the index values from the stat_data
        # dataframe (ie the dataframe
        # containing the original data from the MET output file).
        idx = list(cts_df.index)

        # Work on a copy of the cts_df dataframe to avoid a possible PerformanceWarning
        # message due to a fragmented dataframe.
        cts_df_copy = cts_df.copy()
        cts_df_copy.insert(loc=0, column='Idx', value=idx)

        # Use the pd.wide_to_long() to collect the statistics and confidence level
        # data into the appropriate columns.
        # Rename the <stat_group>_BCL|BCU|NCL|NCU to BCL|BCU|NCL|NCU_<stat_group> in
        # order to
        # use pd.wide_to_long().

        # Rename confidence level column header names so the BCL, BCU, NCL, and NCU
        # are appended with the statistic name
        # (i.e. from FBAR_BCU to BCU_FBAR to be able to use the pandas wide_to_long).
        confidence_level_columns_renamed: List[str] = (
            self.rename_confidence_level_columns(cts_df_copy.columns.tolist()))
        cts_df_copy.columns: List[str] = confidence_level_columns_renamed

        # Rename the statistics columns (ie. FBAR, MAE, FSTDEV, etc. to STAT_FBAR,
        # STAT_MAE, etc.)
        stat_confidence_level_columns_renamed = self.rename_statistics_columns(
            cts_df_copy, cn.CTS_STATS_ONLY_HEADERS)
        cts_df_copy.columns = stat_confidence_level_columns_renamed

        # Get the name of the columns to be used for indexing, this will also
        # preserve the ordering of columns from the
        # original data.
        indexing_columns = ['Idx'] + cn.LC_COMMON_STAT_HEADER + ['total']

        wide_to_long_df: pd.DataFrame = pd.wide_to_long(cts_df_copy,
                                                        stubnames=['STAT', 'NCL', 'NCU',
                                                                   'BCL', 'BCU'],
                                                        i=indexing_columns,
                                                        j='stat_name',
                                                        sep='_',
                                                        suffix='.+'
                                                        ).sort_values('Idx')

        # Rename the BCL, BCU, NCL, NCU, and STAT columns to stat_bcl, stat_bcu,
        # stat_ncl, stat_ncu, and stat_value
        # respectively.
        wide_to_long_df = wide_to_long_df.reset_index()

        renamed_wide_to_long_df = wide_to_long_df.rename(
            columns={'BCL': 'stat_bcl', 'BCU': 'stat_bcu', 'NCL': 'stat_ncl',
                     'NCU': 'stat_ncu', 'STAT': 'stat_value'})

        # Some statistics in the CTS line type only have confidence level confidence
        # limits (ie. no normal confidence limits).
        # Set any nan stat_ncl and stat_ncu records to 'NA'
        linetype_data: pd.DataFrame = renamed_wide_to_long_df.fillna('NA')

        return linetype_data

    def process_cts_for_agg(self, stat_data: pd.DataFrame) -> pd.DataFrame:

        raise NotImplementedError


    def process_mcts(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        """
             Reshape the data from the original MET output file (stat_data) into new
             statistics columns:
             stat_name, stat_value specifically for the MCTS (Multi-
             category Contingency Table Statistics) line type data
             .

             Arguments:
             @param stat_data: the dataframe containing all the data from the MET
             .stat file.

             Returns:
                 linetype_data: the reshaped pandas dataframe with statistics data
                 reorganized into the stat_name and
                                stat_value, stat_ncl, stat_ncu, stat_bcl,
                                and stat_bcu columns.

        """

        # Relevant columns for the MCTS line type
        linetype: str = cn.MCTS
        end = cn.NUM_STAT_MCTS_COLS
        mcts_columns_to_use: List[str] =\
            np.arange(0, end).tolist()

        # Subset original dataframe to one containing only the CTS data
        mcts_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                               mcts_columns_to_use]

        # Add all the columns header names for the MCTS line type
        mcts_columns: List[str] = cn.MCTS_SPECIFIC_HEADERS
        mcts_df.columns: List[str] = mcts_columns

        # Create another index column to preserve the index values from the stat_data
        # dataframe (ie the dataframe
        # containing the original data from the MET output file).
        idx = list(mcts_df.index)

        # Work on a copy of the cts_df dataframe to avoid a possible PerformanceWarning
        # message due to a fragmented dataframe.
        mcts_df_copy = mcts_df.copy()
        mcts_df_copy.insert(loc=0, column='Idx', value=idx)

        # Use the pd.wide_to_long() to collect the statistics and confidence level
        # data into the appropriate columns.
        # Rename the <stat_group>_BCL|BCU|NCL|NCU to BCL|BCU|NCL|NCU_<stat_group> in
        # order to
        # use pd.wide_to_long().

        # Rename confidence level column header names so the BCL, BCU, NCL, and NCU
        # are appended with the statistic name
        # (i.e. from FBAR_BCU to BCU_FBAR to be able to use the pandas wide_to_long).
        confidence_level_columns_renamed: List[str] = (
            self.rename_confidence_level_columns(mcts_df_copy.columns.tolist()))
        mcts_df_copy.columns: List[str] = confidence_level_columns_renamed

        # Rename the statistics columns (ie. ACC, HK, HSS, etc. to STAT_ACCR,
        # STAT_HK, etc.)
        stat_confidence_level_columns_renamed = self.rename_statistics_columns(
            mcts_df_copy, cn.MCTS_STATS_ONLY_HEADERS)
        mcts_df_copy.columns = stat_confidence_level_columns_renamed

        # Get the name of the columns to be used for indexing, this will also
        # preserve the ordering of columns from the
        # original data.
        indexing_columns = ['Idx'] + cn.LC_COMMON_STAT_HEADER + ['total']

        wide_to_long_df: pd.DataFrame = pd.wide_to_long(mcts_df_copy,
                                                        stubnames=['STAT', 'NCL', 'NCU',
                                                                   'BCL', 'BCU'],
                                                        i=indexing_columns,
                                                        j='stat_name',
                                                        sep='_',
                                                        suffix='.+'
                                                        ).sort_values('Idx')

        # Rename the BCL, BCU, NCL, NCU, and STAT columns to stat_bcl, stat_bcu,
        # stat_ncl, stat_ncu, and stat_value
        # respectively.
        wide_to_long_df = wide_to_long_df.reset_index()

        renamed_wide_to_long_df = wide_to_long_df.rename(
            columns={'BCL': 'stat_bcl', 'BCU': 'stat_bcu', 'NCL': 'stat_ncl',
                     'NCU': 'stat_ncu', 'STAT': 'stat_value'})

        # Some statistics in the MCTS line type only have confidence
        # limits (such as HK, HK_BCL, HK_BCU).
        # Set any nan stat_ncl and stat_ncu records to 'NA'
        linetype_data: pd.DataFrame = renamed_wide_to_long_df.fillna('NA')

        return linetype_data

    def process_mcts_for_agg(self, stat_data: pd.DataFrame) -> pd.DataFrame:

        raise NotImplementedError

    def process_sl1l2(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        """
             Reshape the data from the original MET output file (stat_data) into new
             statistics columns:
             stat_name, stat_value specifically for the SL1L2 line type data.

             Arguments:
             @param stat_data: the dataframe containing all the data from the MET
             .stat file.

             Returns:
                 linetype_data: the reshaped pandas dataframe with statistics data
                 reorganized into the stat_name and
                                stat_value columns.

        """

        # Relevant columns for the SL1L2 line type
        linetype: str = cn.SL1L2
        end = cn.NUM_STAT_SL1L2_COLS
        sl1l2_columns_to_use: List[str] = (
            np.arange(0, end).tolist())

        # Subset original dataframe to one containing only the Sl1L2 data
        sl1l2_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                                 sl1l2_columns_to_use]

        # Add the stat columns header names for the SL1L2 line type
        sl1l2_columns: List[str] = cn.SL1L2_HEADERS
        sl1l2_df.columns: List[str] = sl1l2_columns

        # Create another index column to preserve the index values from the stat_data
        # dataframe (ie the dataframe
        # containing the original data from the MET output file).
        idx = list(sl1l2_df.index)

        # Work on a copy of thesl1l2_df dataframe to avoid a possible PerformanceWarning
        # message due to a fragmented dataframe.
        sl1l2_df_copy = sl1l2_df.copy()
        sl1l2_df_copy.insert(loc=0, column='Idx', value=idx)

        # Now apply melt to get the stat_name and stat_values from the statistics

        # Columns we don't want to stack (i.e. treat these columns as a multi index)
        id_vars_list = ['Idx'] + cn.LC_COMMON_STAT_HEADER + ['total']
        reshaped = sl1l2_df_copy.melt(id_vars=id_vars_list,
                                      value_vars=cn.SL1L2_STATISTICS_HEADERS,
                                      var_name='stat_name',
                                      value_name='stat_value').sort_values('Idx')

        # SL1L2 line type doesn't have the bcl and bcu stat values set these to NA
        na_column: List[str] = ['NA' for _ in range(0, reshaped.shape[0])]

        reshaped['stat_ncl']: pd.Series = na_column
        reshaped['stat_ncu']: pd.Series = na_column
        reshaped['stat_bcl']: pd.Series = na_column
        reshaped['stat_bcu']: pd.Series = na_column

        return reshaped

    def process_sl1l2_for_agg(self, stat_data: pd.DataFrame) -> pd.DataFrame:

        raise NotImplementedError


    def process_vl1l2(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        """
             Reshape the data from the original MET output file (stat_data) into new
             statistics columns:
             stat_name, stat_value specifically for the VL1L2 line type data.

             Arguments:
             @param stat_data: the dataframe containing all the data from the MET
             .stat file.

             Returns:
                 linetype_data: the reshaped pandas dataframe with statistics data
                 reorganized into the stat_name and
                                stat_value columns.

        """

        # Relevant columns for the VL1L2 line type
        linetype: str = cn.VL1L2
        end = cn.NUM_STAT_VL1L2_COLS
        vl1l2_columns_to_use: List[str] = (
            np.arange(0, end).tolist())

        # Subset original dataframe to one containing only the Sl1L2 data
        vl1l2_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                                 vl1l2_columns_to_use]

        # Add the stat columns header names for the SL1L2 line type
        vl1l2_columns: List[str] = cn.VL1L2_HEADERS
        vl1l2_df.columns: List[str] = vl1l2_columns

        # Create another index column to preserve the index values from the stat_data
        # dataframe (ie the dataframe
        # containing the original data from the MET output file).
        idx = list(vl1l2_df.index)

        # Work on a copy of thesl1l2_df dataframe to avoid a possible PerformanceWarning
        # message due to a fragmented dataframe.
        vl1l2_df_copy = vl1l2_df.copy()
        vl1l2_df_copy.insert(loc=0, column='Idx', value=idx)

        # Now apply melt to get the stat_name and stat_values from the statistics

        # Columns we don't want to stack (i.e. treat these columns as a multi index)
        id_vars_list = ['Idx'] + cn.LC_COMMON_STAT_HEADER + ['total']
        reshaped = vl1l2_df_copy.melt(id_vars=id_vars_list,
                                      value_vars=cn.VL1L2_STATISTICS_HEADERS,
                                      var_name='stat_name',
                                      value_name='stat_value').sort_values('Idx')

        # VL1L2 line type doesn't have the bcl and bcu stat values set these to NA
        na_column: List[str] = ['NA' for _ in range(0, reshaped.shape[0])]

        reshaped['stat_ncl']: pd.Series = na_column
        reshaped['stat_ncu']: pd.Series = na_column
        reshaped['stat_bcl']: pd.Series = na_column
        reshaped['stat_bcu']: pd.Series = na_column

        return reshaped

    def process_vl1l2_for_agg(self, stat_data: pd.DataFrame) -> pd.DataFrame:

       raise NotImplementedError

    def process_ecnt(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        """
             Reshape the data from the original MET output file (stat_data) into new
             statistics columns:
             stat_name, stat_value specifically for the ECNT line type data.

             Arguments:
             @param stat_data: the dataframe containing all the data from the MET
             .stat file.

             Returns:
                 linetype_data: the reshaped pandas dataframe with statistics data
                 reorganized into the stat_name and
                                stat_value columns.

        """

        # Relevant columns for the ECNT line type
        linetype: str = cn.ECNT
        end = cn.NUM_STAT_ECNT_COLS
        ecnt_columns_to_use: List[str] = (
            np.arange(0, end).tolist())

        # Subset original dataframe to one containing only the ECNT data
        ecnt_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                                ecnt_columns_to_use]

        # Add the stat columns header names for the ECNT line type
        ecnt_columns: List[str] = cn.ECNT_HEADERS
        ecnt_df.columns: List[str] = ecnt_columns

        # Create another index column to preserve the index values from the stat_data
        # dataframe (ie the dataframe
        # containing the original data from the MET output file).
        idx = list(ecnt_df.index)

        # Work on a copy of the ecnt_df dataframe to avoid a possible
        # PerformanceWarning
        # message due to a fragmented dataframe.
        ecnt_df_copy = ecnt_df.copy()
        ecnt_df_copy.insert(loc=0, column='Idx', value=idx)

        # Now apply melt to get the stat_name and stat_values from the statistics

        # Columns we don't want to stack (i.e. treat these columns as a multi index)
        id_vars_list = ['Idx'] + cn.LC_COMMON_STAT_HEADER + ['total']
        reshaped = ecnt_df_copy.melt(id_vars=id_vars_list,
                                      value_vars=cn.ECNT_STATISTICS_HEADERS,
                                      var_name='stat_name',
                                      value_name='stat_value').sort_values('Idx')

        # ECNT line type doesn't have the bcl and bcu stat values set these to NA
        na_column: List[str] = ['NA' for _ in range(0, reshaped.shape[0])]

        reshaped['stat_ncl']: pd.Series = na_column
        reshaped['stat_ncu']: pd.Series = na_column
        reshaped['stat_bcl']: pd.Series = na_column
        reshaped['stat_bcu']: pd.Series = na_column

        return reshaped


    def process_ecnt_for_agg(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        """
             Reformatting for using METcalcpy agg_stat. For input data that does NOT
             have aggregation statistics and confidence values calculated from the
             MET ensemble-stat tool.

             Reformat the data from the original MET output file (stat_data) into
             statistics columns corresponding to the statistics name for the MET ECNT
             linetype, as defined in constants.py in the METdbLoad module:

                 'n_ens', 'crps', 'crpss', 'ign', 'me', 'rmse', 'spread',
                 'me_oerr', 'rmse_oerr', 'spread_oerr', 'spread_plus_oerr',
                 'crpscl', 'crps_emp', 'crpscl_emp', 'crpss_emp',
                 'crps_emp_fair', 'spread_md', 'mae', 'mae_oerr',
                 'bias_ratio', 'n_ge_obs', 'me_ge_obs',
                 'n_lt_obs', 'me_lt_obs'

             In addition, create a stat_name column with  ECNT_<stat> (where stat is the name of the stat
             This format is *required* for using the METcalcpy agg_stat.py module to calculate aggregation
             statistics.

             Arguments:
             @param stat_data: the dataframe containing all the data from the MET
             .stat file.

             Returns:
                 linetype_data: the reformatted pandas dataframe with statistics data
                 reorganized into columns based on the individual ECNT statistic names.

        """

        # Relevant columns for the ECNT line type
        linetype: str = cn.ECNT
        end = cn.NUM_STAT_ECNT_COLS
        ecnt_columns_to_use: List[str] = (
            np.arange(0, end).tolist())

        # Subset original dataframe to one containing only the ECNT data
        ecnt_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:,
                                ecnt_columns_to_use]

        # Replace the column numbers with the name of the corresponding statistic as specified in MET
        # User's Guide for the ECNT linetype in the ensemble stat table.
        all_headers = cn.ECNT_HEADERS
        all_headers_lc = [cur_hdr.lower() for cur_hdr in all_headers]
        ecnt_df.columns = all_headers_lc

        # Add the stat_name column and stat_value columns.  Populate the stat_name column with the
        # 'ECNT_' prefixed statistic names (e.g. for crps, this becomes ECNT_CRPS).  Do this for
        # each ECNT-specific statistic.  This will result in a very large dataframe.
        linetype_str = linetype.upper() + '_'
        ecnt_headers = cn.LC_ECNT_SPECIFIC
        renamed_ecnt = [linetype_str + cur_hdr.upper() for cur_hdr in ecnt_headers]

        # Create a list of dataframes, each corresponding to the ECNT statistics, then merge them
        # all into one final dataframe.
        dfs_to_merge = []

        for renamed in renamed_ecnt:
            tmp_df:pd.DataFrame = ecnt_df.copy()
            tmp_df['stat_name'] = renamed
            dfs_to_merge.append(tmp_df)

        # Merge all the statistics dataframes into one, then add the
        # stat_value column. Initialize the stat_values to NaN/NA.  These
        # values will be filled by the METcalcpy agg_stat calculation.
        merged_dfs:pd.DataFrame = pd.concat(dfs_to_merge, axis=0, ignore_index=True)
        merged_dfs['stat_value'] = np.nan
        merged_dfs.replace('N/A', pd.NA)

        return merged_dfs


    def rename_confidence_level_columns(self, confidence_level_columns: List[str]) -> \
    List[str]:
        """

        Rename the column headers for the confidence levels so they begin with the
        name of the
        confidence level type (i.e. BCL_FBAR rather than FBAR_BCL).  This facilitates
        using pandas
        wide_to_long() to reshape the data in the dataframe.  Maintain the order of
        the column header, leaving
        the other column header names unchanged.

        Arguments:
        @param confidence_level_columns: A list of all the columns in a dataframe

        Returns:
          renamed:  A list of renamed confidence level header names.
        """

        renamed: List[str] = []

        for cur_col in confidence_level_columns:
            match = re.match(r'(.+)_(BCL|bcl|BCU|bcu|NCL|ncl|NCU|ncu)', cur_col)
            if match:
                rearranged = match.group(2) + '_' + match.group(1)
                renamed.append(rearranged.upper())
            else:
                renamed.append(cur_col)

        return renamed

    def rename_statistics_columns(self, df: pd.DataFrame,
                                  statistics_columns: List[str]) -> List[str]:
        """

        Rename the column headers for the statistics columns to begin with 'STAT_' (
        i.e. FBAR becomes STAT_FBAR)
        This facilitates using pandas wide_to_long() to reshape the data in the
        dataframe.
        Maintain the order of the column header, leaving all other column header
        names unchanged.

        Args:
        @param df: The dataframe of interest
        @param statistic_columns: A list of all the statistics columns in a dataframe

        Returns:
           renamed: A list of renamed column headers for the dataframe of interest,
           where the name of the statistic is
                    prefixed with 'STAT_'
        """

        renamed: List[str] = []
        all_columns: List[str] = df.columns.tolist()

        for cur_col in all_columns:
            if cur_col in statistics_columns:
                renamed_column = 'STAT_' + cur_col
                renamed.append(renamed_column)
            else:
                renamed.append(cur_col)

        return renamed


def main():
    '''
       Open the yaml config file specified at the command line to get output
       directory, output filename,
       and location of input files, and the MET tool used to create the input data. 

       Then invoke necessary methods to read and process data to reformat the MET
       .stat file from wide to long format to
       collect statistics information into stat_name, stat_value, stat_bcl, stat_bcu,
       stat_ncl, and stat_ncu columns.
    '''

    # Acquire the output file name and output directory information and location of
    # the xml specification file
    config_file: str = util.read_config_from_command_line()
    with open(config_file, 'r') as stream:
        try:
            parms: dict = yaml.load(stream, Loader=yaml.FullLoader)
            pathlib.Path(parms['output_dir']).mkdir(parents=True, exist_ok=True)
        except yaml.YAMLError as exc:
            print(exc)

    # Check that the config file has all the necessary settings and values
    config_file_ok = config_file_complete(parms)
    if not config_file_ok:
        raise ValueError("Configuration file is missing one or more required settings and/or values.")
        sys.exit(1)

    # Replacing the need for an XML specification file, pass in the XMLLoadFile and
    # ReadDataFile parameters
    rdf_obj: ReadDataFiles = ReadDataFiles()
    xml_loadfile_obj: XmlLoadFile = XmlLoadFile(None)

    # Retrieve all the filenames in the data_dir specified in the YAML config file
    beg_load = time.perf_counter()
    load_files = xml_loadfile_obj.filenames_from_template(parms['input_data_dir'],
                                                          {})

    flags = xml_loadfile_obj.flags
    line_types = xml_loadfile_obj.line_types
    beg_read_data = time.perf_counter()
    rdf_obj.read_data(flags, load_files, line_types)
    end_read_data = time.perf_counter()
    time_to_read = end_read_data - beg_read_data
    logger.info("Time to read input .stat data files using METdbLoad: {time_to_read}" )
    file_df = rdf_obj.stat_data

    # Check if the output file already exists, if so, delete it to avoid
    # appending output from subsequent runs into the same file.
    existing_output_file = os.path.join(parms['output_dir'], parms['output_filename'])
    logger.info("Checking if {existing_output_file}  already exists")
    if os.path.exists(existing_output_file):
        logger.info("Removing existing output file {existing_output_file}")
        os.remove(existing_output_file)

    # Write stat file in ASCII format
    stat_lines_obj: WriteStatAscii = WriteStatAscii(parms)
    # stat_lines_obj.write_stat_ascii(file_df, parms, logger)
    stat_lines_obj.write_stat_ascii(file_df, parms)

def config_file_complete(parms):
    '''
        Determines if the config file contains all the necessary fields.

        Input:
            parms:  The config file
        Returns:
            True if all expected settings are found, False otherwise.
    '''

    # Check for log directory, log filename, log level, line type, output_dir, output_filename, input_data_dir
    expected_settings = ['input_stats_aggregated', 'output_dir', 'output_filename', 'input_data_dir',
                         'log_directory', 'log_filename', 'log_level', 'line_type']
    actual_keys = []
    for k,v in parms.items():
        actual_keys.append(k)
        if v is None:
            msg = "ERROR: Missing the value for the " + k + " setting."
            logger.error(msg)
            return False

    for expected in expected_settings:
        if expected not in actual_keys:
            msg = "ERROR: The " + expected + " setting is missing in the YAML config file"
            logger.error(msg)
            return False





    return True



if __name__ == "__main__":

    main()
