#!/usr/bin/env python3

"""
Program Name: write_stat_ascii.py
Contact(s):  Minna Win
Abstract:
History Log:  Initial version
Usage: Write MET stat files (.stat) to an ASCII file with additional columns of information.
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
import yaml

import constants as cn
from METdbLoad.ush.read_load_xml import XmlLoadFile
from METdbLoad.ush.read_data_files import ReadDataFiles
import util

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
            # MET stat headers). The FCST_INIT_BEG header is added in after the FCST_VALID_END
            # column, so there is one additional column to the common header.
            common_stat_headers = cn.LC_COMMON_STAT_HEADER
            line_types = list(stat_data['line_type'])
            unique_line_types = set(line_types)

            # ------------------------------
            # Extract statistics information
            # ------------------------------
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
            # Extract the stat_names and stat_values for this line type:
            # F_RATE, H_RATE, O_RATE (these will be the stat name).  There are no corresponding xyz_bcl, xyz_bcu,
            # xyz_ncl, and xyz_ncu values where xyz = stat name

            #
            # Subset the stat_data dataframe into a smaller data frame containing only the FHO line type with all its
            # columns.
            #


            # Don't include the 'total' column (column 25, zero-based counting)
            fho_columns_to_use = np.arange(0,29).tolist()
            # fho_columns_to_use = np.append(np.arange(0,26), np.arange(27,30)).tolist()
            fho_df = stat_data[stat_data['line_type'] == linetype].iloc[:, fho_columns_to_use]

            # Add the stat columns for the FHO line type
            fho_columns = cn.LC_COMMON_STAT_HEADER + ['total', 'F_RATE', 'H_RATE', 'O_RATE']
            fho_df.columns = fho_columns

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

            linetype_data.to_csv("/Volumes/d1/minnawin/feature_121_met_reformatter/fho_df.txt", sep="\t")

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


def main():
    '''
       Open the yaml config file specified at the command line to get output directory, output filename,
       and location and name of the xml specification file. The xml specification file contains information
       about what MET file types to reformat and the directory of where input MET output files (.stat) are located.

       Then invoke necessary methods to read and process data to reformat the MET .stat file from wide to long format to
       collect statistics information into stat_name, stat_value, stat_bcl, stat_bcu, stat_ncl, and stat_ncu columns.

    '''

    # Acquire the output file name and output directory information and location of the xml specification file
    config_file = util.read_config_from_command_line()
    with open(config_file, 'r') as stream:
        try:
            parms = yaml.load(stream, Loader=yaml.FullLoader)
        except yaml.YAMLError as exc:
            print(exc)


    # Read in the XML load file. This contains information about which MET output files are to be loaded.
    xml_file = parms['xml_spec_file']
    xml_loadfile_obj = XmlLoadFile(xml_file)
    xml_loadfile_obj.read_xml()

    # Read all of the data from the data files into a dataframe
    rdf_obj = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    rdf_obj.read_data(xml_loadfile_obj.flags,
                      xml_loadfile_obj.load_files,
                      xml_loadfile_obj.line_types)

    # Write stat file in ASCII format, one for each line type
    stat_lines_obj = WriteStatAscii()
    stat_lines_obj.write_stat_ascii(xml_loadfile_obj.flags,
                                    rdf_obj.stat_data)

if __name__ == "__main__":
    main()