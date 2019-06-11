#!/usr/bin/env python3

"""
Program Name: met_db_load.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Load files into METdb
Parameters: -index
Input Files: load_spec XML file
Output Files: N/A
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=import-error
# imported modules exist

import argparse
from datetime import datetime
import logging
import sys
import os

import constants as CN

from read_load_xml import XmlLoadFile
from read_data_files import ReadDataFiles
from write_stat_sql import WriteStatSql

def main():
    """ Main program to load files into the METdb/METviewer database
        Returns:
           N/A
    """
    # use the current date/time (without a space) as part of the log filename
    begin_time = str(datetime.now())
    begin_time_fname = begin_time.replace(" ", "_")

    logging.basicConfig(filename=os.getenv('HOME') + '/METdbLoad' + begin_time_fname + '.log',
                        level=logging.DEBUG)

    logging.info("--- *** --- Start METdbLoad --- *** ---")
    logging.info("Begin time: %s", begin_time)

    parser = argparse.ArgumentParser()
    parser.add_argument("xmlfile", help="Please provide required xml load_spec filename")
    parser.add_argument("-index", action="store_true", help="Only process index, do not load data")

    # get the command line arguments
    args = parser.parse_args()

    #
    #  Read the XML file
    #
    try:
        logging.debug("XML filename is %s", args.xmlfile)

        # instantiate a load_spec XML file
        xml_loadfile = XmlLoadFile(args.xmlfile)

        # read in the XML file and get the information out of its tags
        xml_loadfile.read_xml()

    except (RuntimeError, TypeError, NameError, KeyError):
        logging.error("*** %s occurred in Main reading XML ***", sys.exc_info()[0])


    # if -index is used, only process the index
    if args.index:
        logging.debug("-index is true - only process index")

    try:
        # If user set flags to not read files, remove those files from load_files list
        xml_loadfile.load_files = purge_files(xml_loadfile.load_files, xml_loadfile.flags)

    except (RuntimeError, TypeError, NameError, KeyError):
        logging.error("*** %s occurred in Main purging files not selected ***", sys.exc_info()[0])

    #
    #  Read the data files
    #
    try:

        if xml_loadfile.load_files:
            # instantiate a read data files object
            file_data = ReadDataFiles()

            # read in the data files, with options specified by XML flags
            file_data.read_data(xml_loadfile.load_files,
                                xml_loadfile.flags,
                                xml_loadfile.line_types)
        else:
            # Warn user if no files were given or if no files left after purge
            logging.warning("No files to load")

    except (RuntimeError, TypeError, NameError, KeyError):
        logging.error("*** %s occurred in Main reading data ***", sys.exc_info()[0])

    #
    #  Write the data to a database
    #
    try:

        if not file_data.stat_data.empty and \
                xml_loadfile.connection['db_management_system'] in CN.RELATIONAL:
            stat_lines = WriteStatSql()

            stat_lines.write_sql_data(xml_loadfile.connection, file_data.stat_data)

    except (RuntimeError, TypeError, NameError, KeyError):
        logging.error("*** %s occurred in Main writing data ***", sys.exc_info()[0])

    logging.info("End time: %s", str(datetime.now()))
    logging.info("--- *** --- End METdbLoad --- *** ---")


def purge_files(load_files, xml_flags):
    """ remove any files from load list that user has disallowed in XML tags
        Returns:
           List with files user wants to load
    """

    updated_list = load_files

    try:
        # Remove names of MET and VSDB files if user set load_stat tag to false
        if not xml_flags["load_stat"]:
            updated_list = [item for item in updated_list
                            if not (item.lower().endswith(".stat") or
                                    item.lower().endswith(".vsdb"))]

        # Remove names of MODE files if user set load_mode tag to false
        if not xml_flags["load_mode"] and updated_list:
            updated_list = [item for item in updated_list
                            if not (item.lower().endswith("cts.txt") or
                                    item.lower().endswith("obj.txt"))]

        # Remove names of MTD files if user set load_mtd tag to false
        if not xml_flags["load_mtd"] and updated_list:
            updated_list = [item for item in updated_list
                            if not (item.lower().endswith("2d.txt") or
                                    "3d_s" in item.lower() or
                                    "3d_p" in item.lower())]

    except (RuntimeError, TypeError, NameError, KeyError):
        logging.error("*** %s occurred in purge_files ***", sys.exc_info()[0])

    return updated_list


if __name__ == '__main__':
    main()
