#!/usr/bin/env python3

"""
Program Name: METdbLoad.py
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

from read_load_xml import XmlLoadFile
from read_data_files import ReadDataFiles


def main():
    """! Class to read in load_spec xml file
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

    # if -index is used, only process the index
    if args.index:
        logging.debug("-index is true - only process index")

    try:
        logging.debug("XML filename is %s", args.xmlfile)

        # instantiate a load_spec XML file
        xml_loadfile = XmlLoadFile(args.xmlfile)

        # read in the XML file and get the information out of its tags
        xml_loadfile.read_xml()

    except (RuntimeError, TypeError, NameError, KeyError):
        logging.error("*** %s occurred in Main reading XML ***", sys.exc_info()[0])

    try:
        # instantiate a read data files object
        file_data = ReadDataFiles()

        # read in the data files, with options specified by XML flags
        file_data.read_data(xml_loadfile.load_files,
                            xml_loadfile.flags,
                            xml_loadfile.line_types)

    except (RuntimeError, TypeError, NameError, KeyError):
        logging.error("*** %s occurred in Main reading data ***", sys.exc_info()[0])

    logging.info("End time: %s", str(datetime.now()))
    logging.info("--- *** --- End METdbLoad --- *** ---")


if __name__ == '__main__':
    main()
