#!/usr/bin/env python3

"""
Program Name: write_stat_ascii.py
Contact(s):  Minna Win
Abstract:
History Log:  Initial version (supports CNT, CTC, CTS, and SL1L2 line types for point stat .stat file)
Usage: Write MET stat files (.stat) to an ASCII file with additional columns of information.
Parameters: Requires an xml specification file and yaml configuration file
Input Files: transformed dataframe of MET lines
Output Files: A text file containing reformatted data
Copyright 2022 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member
# constants exist in constants.py

import sys
import os
import logging
import time
from datetime import timedelta
from typing import List, Set
import numpy as np
import pandas as pd
import yaml
import re

import constants as cn
from METdbLoad.ush.read_load_xml import XmlLoadFile
from METdbLoad.ush.read_data_files import ReadDataFiles
import util


class WriteStatAscii:
    """ Class to write MET .stat files to an ASCII file

        Returns:
           None. Generates an output file
    """

    def write_stat_ascii(self, stat_data: pd.DataFrame, parms: dict):
        """ write MET stat files (.stat) to an ASCII file with stat_name, stat_value, stat_bcl, stat_bcu,
            stat_ncl, and stat_ncu columns, converting the original data file from wide form to long form.


            Args:
                @param stat_data: pandas dataframe corresponding to the MET stat input file generated from the METdbLoad
                                  file reader
                @param parms:  The yaml configuration object (dictionary) containing the settings for output dir, output file

            Returns:  None, write an output ASCII file associated with the original MET .stat file with statistics
                      information aggregated into these six columns: stat_name, stat_value, stat_ncl, stat_ncu,
                      stat_bcl, and stat_bcu (the stat_xyz are not available in all line types,
                      these will have values of NA)

        """

        logging.debug("[--- Start write_stat_data ---]")

        write_time_start: float = time.perf_counter()

        try:

            # --------------------
            # Write Stat Headers
            # --------------------

            # Create a generic set of headers, the common headers for all stat files
            # (columns 1-14, then create headers for the maximum number of allowable
            # MET stat headers). The FCST_INIT_BEG header is added in after the FCST_VALID_END
            # column, so there is one additional column to the common header.
            line_types: List[str] = list(stat_data['line_type'])
            unique_line_types: Set[str] = set(line_types)

            # ------------------------------
            # Extract statistics information
            # ------------------------------
            # For each line type, extract the statistics information and save it in a list of dataframes which will
            # be appended together.
            all_reshaped_data_df: List[pd.DataFrame] = []

            # Replace any nan records with 'NA'.  These nan values were set by the METdbLoad read_data_files module.
            stat_data = stat_data.fillna('NA')

            for idx, cur_line_type in enumerate(unique_line_types):
                if cur_line_type == cn.FHO:
                    fho_df: pd.DataFrame = self.process_by_stat_linetype(cur_line_type, stat_data)
                    all_reshaped_data_df.append(fho_df)
                elif cur_line_type == cn.CNT:
                    cnt_df = self.process_by_stat_linetype(cur_line_type, stat_data)
                    all_reshaped_data_df.append(cnt_df)
                elif cur_line_type == cn.CTC:
                    ctc_df = self.process_by_stat_linetype(cur_line_type, stat_data)
                    all_reshaped_data_df.append(ctc_df)
                elif cur_line_type == cn.CTS:
                    cts_df = self.process_by_stat_linetype(cur_line_type, stat_data)
                    all_reshaped_data_df.append(cts_df)
                elif cur_line_type == cn.SL1L2:
                    sl1l2_df = self.process_by_stat_linetype(cur_line_type, stat_data)
                    all_reshaped_data_df.append(sl1l2_df)

            # Consolidate all the line type dataframes into one dataframe
            #
            for idx, dfs in enumerate(all_reshaped_data_df):
                if idx == 0:
                    combined_dfs: pd.DataFrame = all_reshaped_data_df[0].copy(deep=True)
                elif idx != 0:
                    combined_dfs: pd.DataFrame = combined_dfs.append(dfs)

            # Write out to the tab-separated text file
            output_file = os.path.join(parms['output_dir'], parms['output_filename'])
            final_df: pd.DataFrame = combined_dfs.to_csv(output_file, index=None, sep='\t', mode='a')


        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_stat_ascii ***", sys.exc_info()[0])

        write_time_end: float = time.perf_counter()
        write_time: timedelta = timedelta(seconds=write_time_end - write_time_start)

        logging.info("    >>> Write time Stat: %s", str(write_time))

        logging.debug("[--- End write_stat_data ---]")

    def process_by_stat_linetype(self, linetype: str, stat_data: pd.DataFrame):
        """
           For a given linetype, extract the relevant statistics information into the
           stat_name, stat_value columns,
           along with the original data in the all_vars dataframe.

        Args:
            @param linetype: The linetype of interest (i.e. CNT, CTS, FHO, etc.)
            @param stat_data: The original MET data read in from the .stat file. Empty columns from the original .stat
                              file are named with the string representation of the numbers 1-n.

            @return: linetype_data The dataframe that is reshaped (from wide to long), now including the stat_name,
            stat_value, stat_bcl, stat_bcu, stat_ncl, and stat_ncu columns.
        """


        # FHO forecast, hit rate, observation rate
        if linetype == cn.FHO:
            pass
            linetype_data: pd.DataFrame = self.process_fho(stat_data)

        # CNT Continuous Statistics
        elif linetype == cn.CNT:
            linetype_data: pd.DataFrame = self.process_cnt(stat_data)

        # CTC Contingency Table Counts
        elif linetype == cn.CTC:
            pass
            linetype_data: pd.DataFrame = self.process_ctc(stat_data)

        # CTS Contingency Table Statistics
        elif linetype == cn.CTS:
            pass
            linetype_data: pd.DataFrame = self.process_cts(stat_data)


        # SL1L2 Scalar Partial sums
        elif linetype == cn.SL1L2:
            pass
            linetype_data: pd.DataFrame = self.process_sl1l2(stat_data)

        return linetype_data

    def process_fho(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        """
             Retrieve the FHO line type data and reshape it to replace the original columns (based on column number) into
             stat_name, stat_value, stat_bcl, stat_bcu, stat_ncu, and stat_ncl

             Arguments:
             @param stat_data: The dataframe containing all the original data from the MET .stat file.

             Returns:
             linetype_data:  The dataframe with the reshaped data for the FHO line type
         """

        # Extract the stat_names and stat_values for this line type:
        # F_RATE, H_RATE, O_RATE (these will be the stat name).  There are no corresponding xyz_bcl, xyz_bcu,
        # xyz_ncl, and xyz_ncu values where xyz = stat name

        #
        # Subset the stat_data dataframe into a smaller data frame containing only the FHO line type with all its
        # columns.
        #

        # Relevant columns for the FHO line type
        linetype: str = cn.FHO
        fho_columns_to_use: List[str] = np.arange(0, cn.NUM_STAT_FHO_COLS - 1).tolist()

        # Subset original dataframe to another dataframe consisting of only the FHO line type.
        fho_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:, fho_columns_to_use]

        # Add the stat columns header names for the CTC line type
        fho_columns: List[str] = cn.FHO_FULL_HEADER
        fho_df.columns: List[str] = fho_columns

        # Create another index column to preserve the index values from the stat_data dataframe (ie the dataframe
        # containing the original data from the MET output file).
        idx: int = list(fho_df.index)
        fho_df.insert(loc=0, column='Idx', value=idx)

        # Use pandas 'melt' to reshape the data frame from wide to long shape (i.e. collecting the f_rate, h_rate,
        # and o_rate values and putting them under the column 'stat_value' corresponding to the 'stat_name' column
        # containing the names F_RATE, H_RATE, and O_RATE

        # columns that we don't want to change (the last three columns are the stat columns of interest,
        # we want to capture that information into the stat_name and stat_values columns)
        columns_to_use: List[str] = fho_df.columns[0:-3].tolist()
        fho_copy: pd.DataFrame = fho_df.copy(deep=True)
        linetype_data: pd.DataFrame = pd.melt(fho_copy, id_vars=columns_to_use, var_name='stat_name',
                                              value_name='stat_value')

        # FHO line type doesn't have the bcl and bcu stat values; set these to NA
        na_column: List[str] = ['NA' for na_column in range(0, linetype_data.shape[0])]

        linetype_data['stat_ncl']: pd.Series = na_column
        linetype_data['stat_ncu']: pd.Series = na_column
        linetype_data['stat_bcl']: pd.Series = na_column
        linetype_data['stat_bcu']: pd.Series = na_column

        return linetype_data

    def process_cnt(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        """
           Reshape the data from the original MET output file (stat_data) into new statistics columns:
           stat_name, stat_value, stat_ncl, stat_ncu, stat_bcl, and stat_bcu specifically for the CNT line type data.

           Arguments:
           @param stat_data: the dataframe containing all the data from the MET .stat file.

           Returns:
           linetype_data: the reshaped pandas dataframe with statistics and confidence level data reorganized into the
                          stat_name, stat_value, stat_ncl, stat_ncu, stat_bcl, and stat_bcu columns.

        """

        # Relevant columns for the CNT line type
        linetype: str = cn.CNT
        cnt_columns_to_use: List[str] = np.arange(0, cn.NUM_STAT_CNT_COLS).tolist()

        # Subset original dataframe to one containing only the CNT data
        cnt_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:, cnt_columns_to_use]

        # Add the stat columns for the CNT line type
        cnt_columns: List[str] = cn.FULL_CNT_HEADER
        cnt_df.columns: List[str] = cnt_columns

        # Create another index column to preserve the index values from the stat_data dataframe (ie the dataframe
        # containing the original data from the MET output file).
        idx: int = list(cnt_df.index)
        cnt_df.insert(loc=0, column='Idx', value=idx)

        # Use the pd.wide_to_long() to collect the statistics and confidence level data into the appropriate columns.
        # Rename the <stat_group>_BCL|BCU|NCL|NCU to BCL|BCU|NCL|NCU_<stat_group> in order to
        # use pd.wide_to_long().

        # Rename confidence level column header names so the BCL, BCU, NCL, and NCU are appended with the statistic name
        # (i.e. from FBAR_BCU to BCU_FBAR to be able to use the pandas wide_to_long).
        confidence_level_columns_renamed: List[str] = self.rename_confidence_level_columns(cnt_df.columns.tolist())
        cnt_df.columns: List[str] = confidence_level_columns_renamed

        # Rename the statistics columns (ie. FBAR, MAE, FSTDEV, etc. to STAT_FBAR, STAT_MAE, etc.)
        stat_confidence_level_columns_renamed = self.rename_statistics_columns(cnt_df, cn.CNT_STATISTICS_HEADERS)
        cnt_df.columns = stat_confidence_level_columns_renamed

        # Get the name of the columns to be used for indexing, this will also preserve the ordering of columns from the
        # original data.
        indexing_columns = ['Idx'] + cn.LC_COMMON_STAT_HEADER + ['total']

        wide_to_long_df: pd.DataFrame = pd.wide_to_long(cnt_df,
                                                        stubnames=['STAT', 'NCL', 'NCU', 'BCL', 'BCU'],
                                                        i=indexing_columns,
                                                        j='stat_name',
                                                        sep='_',
                                                        suffix='.+'
                                                        ).sort_values('Idx')

        # Rename the BCL, BCU, NCL, NCU, and STAT columns to stat_bcl, stat_bcu, stat_ncl, stat_ncu, and stat_value
        # respectively.
        wide_to_long_df = wide_to_long_df.reset_index()

        renamed_wide_to_long_df:pd.DataFrame = wide_to_long_df.rename(
            columns={'BCL': 'stat_bcl', 'BCU': 'stat_bcu', 'NCL': 'stat_ncl', 'NCU': 'stat_ncu', 'STAT': 'stat_value'})

        # Some statistics in the CNT line type only have confidence level confidence limits (ie. no normal confidence limits).
        # Set any nan stat_ncl and stat_ncu records to 'NA'
        linetype_data: pd.DataFrame = renamed_wide_to_long_df.fillna('NA')


        return linetype_data

    def process_ctc(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        """
             Reshape the data from the original MET output file (stat_data) into new statistics columns:
             stat_name, stat_value specifically for the CTC line type data.

             Arguments:
             @param stat_data: the dataframe containing all the data from the MET .stat file.

             Returns:
                 linetype_data: the reshaped pandas dataframe with statistics data reorganized into the stat_name and
                                stat_value, stat_ncl, stat_ncu, stat_bcl, and stat_bcu columns.

        """

        # Relevant columns for the CTC line type
        linetype: str = cn.CTC
        ctc_columns_to_use: List[str] = np.arange(0, cn.NUM_STAT_CTC_COLS).tolist()

        # Subset original dataframe to one containing only the CTC data
        ctc_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:, ctc_columns_to_use]

        # Add the stat columns header names for the CTC line type
        ctc_columns: List[str] = cn.CTC_HEADERS
        ctc_df.columns: List[str] = ctc_columns

        # Create another index column to preserve the index values from the stat_data dataframe (ie the dataframe
        # containing the original data from the MET output file).
        idx: int = list(ctc_df.index)
        ctc_df.insert(loc=0, column='Idx', value=idx)

        # Now apply melt to get the stat_name and stat_values from the statistics

        # Columns we don't want to stack (i.e. treat these columns as a multi index)
        id_vars_list = ['Idx'] + cn.LC_COMMON_STAT_HEADER + ['total']
        linetype_data = ctc_df.melt(id_vars=id_vars_list, value_vars=cn.CTC_STATISTICS_HEADERS,
                                    var_name='stat_name',
                                    value_name='stat_value').sort_values('Idx')

        # CTC line type doesn't have the ncl, ncu, bcl and bcu stat values set these to NA
        na_column: List[str] = ['NA' for na_column in range(0, linetype_data.shape[0])]

        linetype_data['stat_ncl']: pd.Series = na_column
        linetype_data['stat_ncu']: pd.Series = na_column
        linetype_data['stat_bcl']: pd.Series = na_column
        linetype_data['stat_bcu']: pd.Series = na_column

        return linetype_data

    def process_cts(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        """
             Reshape the data from the original MET output file (stat_data) into new statistics columns:
             stat_name, stat_value specifically for the CTS line type data.

             Arguments:
             @param stat_data: the dataframe containing all the data from the MET .stat file.

             Returns:
                 linetype_data: the reshaped pandas dataframe with statistics data reorganized into the stat_name and
                                stat_value, stat_ncl, stat_ncu, stat_bcl, and stat_bcu columns.

        """

        # Relevant columns for the CTS line type
        linetype: str = cn.CTS
        cts_columns_to_use: List[str] = np.arange(0, cn.NUM_STAT_CTS_COLS).tolist()

        # Subset original dataframe to one containing only the CTS data
        cts_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:, cts_columns_to_use]

        # Add all the columns header names for the CTS line type
        cts_columns: List[str] = cn.CTS_SPECIFIC_HEADERS
        cts_df.columns: List[str] = cts_columns

        # Create another index column to preserve the index values from the stat_data dataframe (ie the dataframe
        # containing the original data from the MET output file).
        idx: int = list(cts_df.index)
        cts_df.insert(loc=0, column='Idx', value=idx)

        # Use the pd.wide_to_long() to collect the statistics and confidence level data into the appropriate columns.
        # Rename the <stat_group>_BCL|BCU|NCL|NCU to BCL|BCU|NCL|NCU_<stat_group> in order to
        # use pd.wide_to_long().

        # Rename confidence level column header names so the BCL, BCU, NCL, and NCU are appended with the statistic name
        # (i.e. from FBAR_BCU to BCU_FBAR to be able to use the pandas wide_to_long).
        confidence_level_columns_renamed: List[str] = self.rename_confidence_level_columns(cts_df.columns.tolist())
        cts_df.columns: List[str] = confidence_level_columns_renamed

        # Rename the statistics columns (ie. FBAR, MAE, FSTDEV, etc. to STAT_FBAR, STAT_MAE, etc.)
        stat_confidence_level_columns_renamed = self.rename_statistics_columns(cts_df, cn.CTS_STATS_ONLY_HEADERS)
        cts_df.columns = stat_confidence_level_columns_renamed

        # Get the name of the columns to be used for indexing, this will also preserve the ordering of columns from the
        # original data.
        indexing_columns = ['Idx'] + cn.LC_COMMON_STAT_HEADER + ['total']

        wide_to_long_df: pd.DataFrame = pd.wide_to_long(cts_df,
                                                        stubnames=['STAT', 'NCL', 'NCU', 'BCL', 'BCU'],
                                                        i=indexing_columns,
                                                        j='stat_name',
                                                        sep='_',
                                                        suffix='.+'
                                                        ).sort_values('Idx')

        # Rename the BCL, BCU, NCL, NCU, and STAT columns to stat_bcl, stat_bcu, stat_ncl, stat_ncu, and stat_value
        # respectively.
        wide_to_long_df = wide_to_long_df.reset_index()

        renamed_wide_to_long_df = wide_to_long_df.rename(
            columns={'BCL': 'stat_bcl', 'BCU': 'stat_bcu', 'NCL': 'stat_ncl', 'NCU': 'stat_ncu', 'STAT': 'stat_value'})

        # Some statistics in the CNT line type only have confidence level confidence limits (ie. no normal confidence limits).
        # Set any nan stat_ncl and stat_ncu records to 'NA'
        linetype_data: pd.DataFrame = renamed_wide_to_long_df.fillna('NA')

        return linetype_data

    def process_sl1l2(self, stat_data: pd.DataFrame) -> pd.DataFrame:
        """
             Reshape the data from the original MET output file (stat_data) into new statistics columns:
             stat_name, stat_value specifically for the SL1L2 line type data.

             Arguments:
             @param stat_data: the dataframe containing all the data from the MET .stat file.

             Returns:
                 linetype_data: the reshaped pandas dataframe with statistics data reorganized into the stat_name and
                                stat_value columns.

        """

        # Relevant columns for the SL1L2 line type
        linetype: str = cn.SL1L2
        sl1l2_columns_to_use: List[str] = np.arange(0, cn.NUM_STAT_SL1L2_COLS).tolist()

        # Subset original dataframe to one containing only the Sl1L2 data
        sl1l2_df: pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:, sl1l2_columns_to_use]

        # Add the stat columns header names for the SL1L2 line type
        sl1l2_columns: List[str] = cn.SL1L2_HEADERS
        sl1l2_df.columns: List[str] = sl1l2_columns

        # Create another index column to preserve the index values from the stat_data dataframe (ie the dataframe
        # containing the original data from the MET output file).
        idx: int = list(sl1l2_df.index)
        sl1l2_df.insert(loc=0, column='Idx', value=idx)

        # Now apply melt to get the stat_name and stat_values from the statistics

        # Columns we don't want to stack (i.e. treat these columns as a multi index)
        id_vars_list = ['Idx'] + cn.LC_COMMON_STAT_HEADER + ['total']
        reshaped = sl1l2_df.melt(id_vars=id_vars_list, value_vars=cn.SL1L2_STATISTICS_HEADERS,
                                 var_name='stat_name',
                                 value_name='stat_value').sort_values('Idx')

        # SL1L2 line type doesn't have the bcl and bcu stat values set these to NA
        na_column: List[str] = ['NA' for na_column in range(0, reshaped.shape[0])]

        reshaped['stat_ncl']: pd.Series = na_column
        reshaped['stat_ncu']: pd.Series = na_column
        reshaped['stat_bcl']: pd.Series = na_column
        reshaped['stat_bcu']: pd.Series = na_column

        return reshaped

    def rename_confidence_level_columns(self, confidence_level_columns: List[str]) -> List[str]:
        """

        Rename the column headers for the confidence levels so they begin with the name of the
        confidence level type (i.e. BCL_FBAR rather than FBAR_BCL).  This facilitates using pandas
        wide_to_long() to reshape the data in the dataframe.  Maintain the order of the column header, leaving
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

    def rename_statistics_columns(self, df: pd.DataFrame, statistics_columns: List[str]) -> List[str]:
        """

        Rename the column headers for the statistics columns to begin with 'STAT_' (i.e. FBAR becomes STAT_FBAR)
        This facilitates using pandas wide_to_long() to reshape the data in the dataframe.
        Maintain the order of the column header, leaving all other column header names unchanged.

        Args:
        @param df: The dataframe of interest
        @param statistic_columns: A list of all the statistics columns in a dataframe

        Returns:
           renamed: A list of renamed column headers for the dataframe of interest, where the name of the statistic is
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
       Open the yaml config file specified at the command line to get output directory, output filename,
       and location and name of the xml specification file. The xml specification file contains information
       about what MET file types to reformat and the directory of where input MET output files (.stat) are located.

       Then invoke necessary methods to read and process data to reformat the MET .stat file from wide to long format to
       collect statistics information into stat_name, stat_value, stat_bcl, stat_bcu, stat_ncl, and stat_ncu columns.

    '''

    # Acquire the output file name and output directory information and location of the xml specification file
    config_file: str = util.read_config_from_command_line()
    with open(config_file, 'r') as stream:
        try:
            parms: dict = yaml.load(stream, Loader=yaml.FullLoader)
        except yaml.YAMLError as exc:
            print(exc)

    # Read in the XML load file. This contains information about which MET output files are to be loaded.
    xml_file: str = parms['xml_spec_file']
    xml_loadfile_obj: XmlLoadFile = XmlLoadFile(xml_file)
    xml_loadfile_obj.read_xml()

    # Read all of the data from the data files into a dataframe
    rdf_obj: ReadDataFiles = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    rdf_obj.read_data(xml_loadfile_obj.flags,
                      xml_loadfile_obj.load_files,
                      xml_loadfile_obj.line_types)

    # Write stat file in ASCII format, one for each line type
    stat_lines_obj: WriteStatAscii = WriteStatAscii()
    stat_lines_obj.write_stat_ascii(rdf_obj.stat_data, parms)


if __name__ == "__main__":
    main()
