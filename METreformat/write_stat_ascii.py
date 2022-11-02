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
import os
import logging
import time
from datetime import timedelta
from typing import List, Set
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
           None. Generates an output file
    """

    def write_stat_ascii(self, stat_data:pd.DataFrame, parms:dict):
        """ write MET stat files (.stat) to an ASCII file with stat_name, stat_value, stat_bcl, stat_bcu,
            stat_ncl, and stat_ncu columns, converting the original data file from wide form to long form.


            Args:
                @ param stat_data: pandas dataframe corresponding to the MET stat input file

                @param parms:  The yaml configuration object (dictionary) containing the settings for output dir, output file

            Returns:  None, write an output ASCII file associated with the original MET .stat file with statistics
                      information aggregated into these six columns: stat_name, stat_value, stat_ncl, stat_ncu,
                      stat_bcl, and stat_bcu (the stat_xyz are not available in all line types,
                      these will have values of NA)

        """

        logging.debug("[--- Start write_stat_data ---]")

        write_time_start:float = time.perf_counter()

        try:

            # --------------------
            # Write Stat Headers
            # --------------------

            # Create a generic set of headers, the common headers for all stat files
            # (columns 1-14, then create headers for the maximum number of allowable
            # MET stat headers). The FCST_INIT_BEG header is added in after the FCST_VALID_END
            # column, so there is one additional column to the common header.
            common_stat_headers:str = cn.LC_COMMON_STAT_HEADER
            line_types:List[str] = list(stat_data['line_type'])
            unique_line_types:Set[str] = set(line_types)

            # ------------------------------
            # Extract statistics information
            # ------------------------------
            # For each line type, extract the statistics information and save it in a new dataframe
            expanded_headers:List[str] = common_stat_headers + ['stat_name', 'stat_value', 'stat_bcl', 'stat_bcu', 'stat_ncl,'
                                                                                                         'stat_ncu']
            all_var:pd.DataFrame = pd.DataFrame(columns=expanded_headers)

            for idx, cur_line_type in enumerate(unique_line_types):
                if cur_line_type == cn.FHO:
                    fho_var:pd.DataFrame = self.process_by_stat_linetype(cur_line_type, stat_data)
                if cur_line_type == cn.CNT:
                    pass
                    cnt_var = self.process_by_stat_linetype(cur_line_type, stat_data)

            # ToDo
            # Consolidate all the line type dataframes into one dataframe
            #

            # Write out to an ASCII file
            out_path:str = parms['output_dir']
            out_filename:str = parms['output_filename']
            output_file:os.path = os.path.join(out_path, out_filename)
            print(f"Writing output...")
            final_df:pd.DataFrame = fho_var.to_csv(output_file, index=None, sep='\t', mode='a')




        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in write_stat_ascii ***", sys.exc_info()[0])

        write_time_end:float = time.perf_counter()
        write_time:timedelta = timedelta(seconds=write_time_end - write_time_start)

        logging.info("    >>> Write time Stat: %s", str(write_time))

        logging.debug("[--- End write_stat_data ---]")

    def process_by_stat_linetype(self, linetype:str, stat_data:pd.DataFrame):
        '''
           For a given linetype, extract the relevant statistics information into the
           stat_name, stat_value, stat_bcl, stat_bcu, stat_ncl, and stat_ncu columns,
           along with the original data in the all_vars dataframe.

        Args:
            @param linetype: The linetype of interest (i.e. CNT, CTS, FHO, etc.)
            @param stat_data: The original MET data read in from the .stat file. Empty columns from the original .stat
                              file are named with the string representation of the numbers 1-n.

            @return: linetype_data The dataframe that is reshaped (from wide to long), now including the stat_name,
            stat_value, stat_bcl, stat_bcu, stat_ncl, and stat_ncu columns.
        '''

        # FHO forecast, hit rate, observation rate
        linetype_data:pd.DataFrame = self.process_fho(stat_data)

        # CNT Continuous Statistics
        if linetype == cn.CNT:
            # Relevant columns for the CNT line type
            cnt_columns_to_use:List[str] = np.arange(0, 126).tolist()
            cnt_df:pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:, cnt_columns_to_use]

            pass

        # CTC Contingency Table Counts
        if linetype == cn.CTC:
            pass

        # CTS Contingency Table Statistics
        if linetype == cn.CTS:
            pass

        # SL1L2 Scalar Partial sums
        if linetype == cn.SL1L2:
            pass

        return linetype_data

    def process_fho(self, stat_data:pd.DataFrame) -> pd.DataFrame:
        '''
            Retrieve the FHO line type data and reshape it to replace the original columns (based on column number) into
            stat_name, stat_value, stat_bcl, stat_bcu, stat_ncu, and stat_ncl

            Arguments:
            @param stat_data: The dataframe containing all the original data from the MET .stat file.

            Returns:
            linetype_data:  The dataframe with the reshaped data for the FHO line type
        '''

        linetype:str = cn.FHO

        # Extract the stat_names and stat_values for this line type:
        # F_RATE, H_RATE, O_RATE (these will be the stat name).  There are no corresponding xyz_bcl, xyz_bcu,
        # xyz_ncl, and xyz_ncu values where xyz = stat name

        #
        # Subset the stat_data dataframe into a smaller data frame containing only the FHO line type with all its
        # columns.
        #

        # Relevant columns for the FHO line type
        fho_columns_to_use:List[str] = np.arange(0, 29).tolist()
        fho_df:pd.DataFrame = stat_data[stat_data['line_type'] == linetype].iloc[:, fho_columns_to_use]

        # Add the stat columns for the FHO line type
        fho_columns:List[str] = cn.LC_COMMON_STAT_HEADER + ['total', 'F_RATE', 'H_RATE', 'O_RATE']
        fho_df.columns:List[str] = fho_columns

        # Create another index column to preserve the index values from the stat_data dataframe (ie the dataframe
        # containing the original data from the MET output file).
        idx:int = list(fho_df.index)
        fho_df.insert(loc=0, column='Idx', value=idx)

        # Use pandas 'melt' to reshape the data frame from wide to long shape (i.e. collecting the f_rate, h_rate,
        # and o_rate values and putting them under the column 'stat_value' corresponding to the 'stat_name' column
        # containing the names F_RATE, H_RATE, and O_RATE

        # columns that we don't want to change
        columns_to_use:pd.Index = fho_df.columns[0:-3]
        fho_copy:pd.DataFrame = fho_df.copy(deep=True)
        linetype_data:pd.DataFrame = pd.melt(fho_copy, id_vars=list(columns_to_use), var_name='stat_name',
                                value_name='stat_value')

        # FHO line type doesn't have the bcl and bcu stat values set these to NA
        na_column:List[str] = ['NA' for na_column in range(0, linetype_data.shape[0])]

        linetype_data['stat_ncl']:pd.Series = na_column
        linetype_data['stat_ncu']:pd.Series = na_column
        linetype_data['stat_bcl']:pd.Series = na_column
        linetype_data['stat_bcu']:pd.Series = na_column

        return linetype_data

    def process_cnt(self, stat_data:pd.DataFrame) -> pd.DataFrame:
        '''
           Reshape the data from the original MET output file (stat_data)
        :param stat_data:
        :return:
        '''


def main():
    '''
       Open the yaml config file specified at the command line to get output directory, output filename,
       and location and name of the xml specification file. The xml specification file contains information
       about what MET file types to reformat and the directory of where input MET output files (.stat) are located.

       Then invoke necessary methods to read and process data to reformat the MET .stat file from wide to long format to
       collect statistics information into stat_name, stat_value, stat_bcl, stat_bcu, stat_ncl, and stat_ncu columns.

    '''

    # Acquire the output file name and output directory information and location of the xml specification file
    config_file:str = util.read_config_from_command_line()
    with open(config_file, 'r') as stream:
        try:
            parms:dict = yaml.load(stream, Loader=yaml.FullLoader)
        except yaml.YAMLError as exc:
            print(exc)

    # Read in the XML load file. This contains information about which MET output files are to be loaded.
    xml_file:str = parms['xml_spec_file']
    xml_loadfile_obj:XmlLoadFile= XmlLoadFile(xml_file)
    xml_loadfile_obj.read_xml()

    # Read all of the data from the data files into a dataframe
    rdf_obj:ReadDataFiles = ReadDataFiles()

    # read in the data files, with options specified by XML flags
    rdf_obj.read_data(xml_loadfile_obj.flags,
                      xml_loadfile_obj.load_files,
                      xml_loadfile_obj.line_types)

    # Write stat file in ASCII format, one for each line type
    stat_lines_obj:WriteStatAscii = WriteStatAscii()
    stat_lines_obj.write_stat_ascii(rdf_obj.stat_data, parms)


if __name__ == "__main__":
    main()
