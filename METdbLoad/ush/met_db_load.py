#!/usr/bin/env python3

"""
Program Name: met_db_load.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Load files into METdataio
Parameters: -index
Input Files: load_spec XML file
Output Files: N/A
Copyright 2020 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=import-error
# pylint:disable=no-member
# imported modules exist
# constants exist in constants.py


import argparse
import logging
import time
from datetime import datetime
from datetime import timedelta
import sys
import os
import getpass

import METdbLoad.ush.constants as CN

from METdbLoad.ush.read_load_xml import XmlLoadFile
from METdbLoad.ush.read_data_files import ReadDataFiles
from METdbLoad.ush.run_sql import RunSql
from METdbLoad.ush.write_file_sql import WriteFileSql
from METdbLoad.ush.write_stat_sql import WriteStatSql
from METdbLoad.ush.write_mode_sql import WriteModeSql
from METdbLoad.ush.write_tcst_sql import WriteTcstSql
from METdbLoad.ush.write_mtd_sql import WriteMtdSql


def main(args):
    """ Main program to load files into the METdataio/METviewer database
        Returns:
           N/A
    """
    # use the current date/time for logging
    begin_time = str(datetime.now())

    # Default logging level is INFO. Can be changed with XML tag verbose
    logging.basicConfig(level=logging.INFO)

    # Print the METdbload version from the docs folder
    print_version()

    logging.info("--- *** --- Start METdbLoad --- *** ---")
    logging.info("Begin time: %s", begin_time)

    try:
        logging.info("User name is: %s",  getpass.getuser())
    except:
        logging.info("User name is not available")

    # time execution
    load_time_start = time.perf_counter()


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
        sys.exit("*** Error reading XML")

    #
    #  Verify the tmp file
    #
    try:
        tmp_dir = args.tmpdir[0]
        if not os.path.isdir(tmp_dir):
            logging.error("*** Error occurred in Main accessing tmp dir %s ***", tmp_dir)
            sys.exit("*** Error accessing tmp dir")

    except (RuntimeError, TypeError, NameError, KeyError):
        logging.error("*** %s occurred in Main accessing tmp dir ***", sys.exc_info()[0])
        sys.exit("*** Error accessing tmp dir")

    # If XML tag verbose is set to True, change logging to debug level
    if xml_loadfile.flags["verbose"]:
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        logging.basicConfig(level=logging.DEBUG)

    #
    # If argument -index is used, only process the index
    #
    if args.index and xml_loadfile.flags["apply_indexes"]:
        try:
            if xml_loadfile.connection['db_management_system'] in CN.RELATIONAL:
                sql_run = RunSql()
                sql_run.sql_on(xml_loadfile.connection)
                sql_run.apply_indexes(False, sql_run.cur)
                logging.debug("-index is true - only process index")
                if sql_run.conn.open:
                    sql_run.sql_off(sql_run.conn, sql_run.cur)
            sys.exit("*** Only processing index with -index as argument")
        except (RuntimeError, TypeError, NameError, KeyError, AttributeError):
            if sql_run.conn.open:
                sql_run.sql_off(sql_run.conn, sql_run.cur)
            logging.error("*** %s occurred in Main processing index ***", sys.exc_info()[0])
            sys.exit("*** Error processing index")

    #
    #  Purge files if flags set to not load certain types of files
    #
    try:
        # If user set flags to not read files, remove those files from load_files list
        xml_loadfile.load_files = purge_files(xml_loadfile.load_files, xml_loadfile.flags)

        if not xml_loadfile.load_files:
            logging.warning("!!! No files to load")
            sys.exit("*** No files to load")

    except (RuntimeError, TypeError, NameError, KeyError):
        logging.error("*** %s occurred in Main purging files not selected ***", sys.exc_info()[0])
        sys.exit("*** Error when removing files from load list per XML")

    # Set up indices to process some maximum number of files at a time
    set_count = 0
    first_file = 0
    last_file = len(xml_loadfile.load_files) - 1

    if last_file > CN.MAX_FILES:
        mid_file = first_file + CN.MAX_FILES
    else:
        mid_file = last_file

    line_counts = {"Stat": 0, "Mode CTS": 0, "Mode Obj": 0, "Tcst": 0,
                   "MTD 2D": 0, "MTD 3D Single": 0, "MTD 3D Pair": 0}

    while mid_file <= last_file:
        try:
            # Keep track of which set of files is being processed
            set_count = set_count + 1
            # Handle only 1 file, or more files
            if first_file == last_file:
                current_files = [xml_loadfile.load_files[first_file]]
            else:
                current_files = xml_loadfile.load_files[first_file:mid_file + 1]

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s occurred in Main setting up loop ***", sys.exc_info()[0])
            sys.exit("*** Error when setting up loop")

        #
        #  Read the data files
        #
        try:

            # instantiate a read data files object
            file_data = ReadDataFiles()

            # read in the data files, with options specified by XML flags
            file_data.read_data(xml_loadfile.flags,
                                current_files,
                                xml_loadfile.line_types)

            current_files = []

            if file_data.data_files.empty:
                logging.warning("!!! No files to load in current set %s", str(set_count))
                # move indices to the next set of files
                first_file, mid_file, last_file = next_set(mid_file, last_file)
                continue

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s occurred in Main reading data ***", sys.exc_info()[0])
            sys.exit("*** Error when reading data files")

        #
        #  Write the data to a database
        #
        try:

            if xml_loadfile.connection['db_management_system'] in CN.RELATIONAL:
                # for the first set of files, connect to the database
                if set_count == 1:
                    sql_run = RunSql()
                    sql_run.sql_on(xml_loadfile.connection)

                    #  if drop_indexes is set to true, drop the indexes
                    if xml_loadfile.flags["drop_indexes"]:
                        sql_run.apply_indexes(True, sql_run.cur)

                # write the data file records out. put data file ids into other dataframes
                write_file = WriteFileSql()
                updated_data = write_file.write_file_sql(xml_loadfile.flags,
                                                         file_data.data_files,
                                                         file_data.stat_data,
                                                         file_data.mode_cts_data,
                                                         file_data.mode_obj_data,
                                                         file_data.tcst_data,
                                                         file_data.mtd_2d_data,
                                                         file_data.mtd_3d_single_data,
                                                         file_data.mtd_3d_pair_data,
                                                         tmp_dir,
                                                         sql_run.cur,
                                                         sql_run.local_infile)

                file_data.data_files = updated_data[0]
                file_data.stat_data = updated_data[1]
                line_counts["Stat"] += len(file_data.stat_data)
                file_data.mode_cts_data = updated_data[2]
                line_counts["Mode CTS"] += len(file_data.mode_cts_data)
                file_data.mode_obj_data = updated_data[3]
                line_counts["Mode Obj"] += len(file_data.mode_obj_data)
                file_data.tcst_data = updated_data[4]
                line_counts["Tcst"] += len(file_data.tcst_data)
                file_data.mtd_2d_data = updated_data[5]
                line_counts["MTD 2D"] += len(file_data.mtd_2d_data)
                file_data.mtd_3d_single_data = updated_data[6]
                line_counts["MTD 3D Single"] += len(file_data.mtd_3d_single_data)
                file_data.mtd_3d_pair_data = updated_data[7]
                line_counts["MTD 3D Pair"] += len(file_data.mtd_3d_pair_data)

                if file_data.data_files.empty:
                    logging.warning("!!! No data to load in current set %s", str(set_count))
                    # move indices to the next set of files
                    first_file, mid_file, last_file = next_set(mid_file, last_file)

                if not file_data.stat_data.empty:
                    stat_lines = WriteStatSql()

                    stat_lines.write_stat_data(xml_loadfile.flags,
                                               file_data.stat_data,
                                               tmp_dir,
                                               sql_run.cur,
                                               sql_run.local_infile)

                if (not file_data.mode_cts_data.empty) or (not file_data.mode_obj_data.empty):
                    cts_lines = WriteModeSql()
                    cts_lines.write_mode_data(xml_loadfile.flags,
                                              file_data.mode_cts_data,
                                              file_data.mode_obj_data,
                                              tmp_dir,
                                              sql_run.cur,
                                              sql_run.local_infile)

                if not file_data.tcst_data.empty:
                    tcst_lines = WriteTcstSql()

                    tcst_lines.write_tcst_data(xml_loadfile.flags,
                                               file_data.tcst_data,
                                               tmp_dir,
                                               sql_run.cur,
                                               sql_run.local_infile)

                if (not file_data.mtd_2d_data.empty) or (not file_data.mtd_3d_single_data.empty) \
                        or (not file_data.mtd_3d_pair_data.empty):
                    mtd_lines = WriteMtdSql()

                    mtd_lines.write_mtd_data(xml_loadfile.flags,
                                             file_data.mtd_2d_data,
                                             file_data.mtd_3d_single_data,
                                             file_data.mtd_3d_pair_data,
                                             tmp_dir,
                                             sql_run.cur,
                                             sql_run.local_infile)

                # Processing for the last set of data
                if mid_file >= last_file:
                    # If any data was written, write to the metadata and instance_info tables
                    if not file_data.data_files.empty:
                        write_file.write_metadata_sql(xml_loadfile.flags,
                                                      file_data.data_files,
                                                      xml_loadfile.group,
                                                      xml_loadfile.description,
                                                      xml_loadfile.load_note,
                                                      xml_loadfile.xml_str,
                                                      tmp_dir,
                                                      sql_run.cur,
                                                      sql_run.local_infile)

                    #  if apply_indexes is set to true, load the indexes
                    if xml_loadfile.flags["apply_indexes"]:
                        sql_run.apply_indexes(False, sql_run.cur)

                    if sql_run.conn.open:
                        sql_run.sql_off(sql_run.conn, sql_run.cur)

            # move indices to the next set of files
            first_file, mid_file, last_file = next_set(mid_file, last_file)

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s occurred in Main writing data ***", sys.exc_info()[0])
            sys.exit("*** Error when writing data to database")

    if not file_data.data_files.empty:
        if sql_run.conn.open:
            sql_run.sql_off(sql_run.conn, sql_run.cur)

    load_time_end = time.perf_counter()
    load_time = timedelta(seconds=load_time_end - load_time_start)

    logging.info("    >>> Total load time: %s", str(load_time))
    for k in line_counts:
        logging.info("For %s Count %s", k, line_counts[k])

    try:
        logging.info("User name is: %s",  getpass.getuser())
    except:
        logging.info("User name is not available")

    logging.info("End time: %s\n", str(datetime.now()))
    logging.info("--- *** --- End METdbLoad --- *** ---")


def print_version():
    """ Get version number from docs folder and print it
        Returns:
           N/A
    """
    try:
        base_dir = os.path.dirname(os.path.realpath(__file__))
        version_file = "{}/../../docs/version".format(base_dir)
        file = open(version_file, mode='r')
        code_version = str.strip(file.read())
        logging.info("METdbload Version: %s", code_version)

    except (RuntimeError, TypeError, NameError, KeyError):
        logging.error("*** %s occurred in print_version ***", sys.exc_info()[0])
        logging.error("*** %s occurred in Main printing version ***", sys.exc_info()[0])
        sys.exit("*** Error in print version")


def next_set(mid_file, last_file):
    """ move indices to next set of files
        Returns:
           N/A
    """
    first_file = mid_file + 1
    if first_file > last_file:
        mid_file = last_file + 1
    else:
        mid_file = first_file + CN.MAX_FILES
        if mid_file > last_file:
            mid_file = last_file
    return first_file, mid_file, last_file


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
        logging.error("*** %s occurred in Main purging files not selected ***", sys.exc_info()[0])
        sys.exit("*** Error in purge files")

    return updated_list


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # Allow user to choose dir for tmp files - default to user home
    tmp_dir = [os.getenv('HOME')]
    parser.add_argument("xmlfile", help="Please provide required xml load_spec filename")
    parser.add_argument("-index", action="store_true", help="Only process index, do not load data")
    parser.add_argument("tmpdir", nargs='*', default=tmp_dir,
                        help="Optional - when different directory wanted for tmp file")

    # get the command line arguments
    args = parser.parse_args()

    main(args)
