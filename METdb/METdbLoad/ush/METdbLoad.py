#!/usr/bin/env python3

'''
Program Name: METdbLoad.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Load files into METdb
Parameters: -index
Input Files: load_spec XML file
Output Files: N/A
Copyright 2019 UCAR/NCAR/RAL, Colorado State University, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
'''

import argparse
import sys
import pymysql

import METdb.METdbLoad.ush.readLoadXML as RX

def main():

    print("--- Start METdbLoad ---")

    parser = argparse.ArgumentParser()
    parser.add_argument("xmlfile", help="please provide required xml load_spec filename")
    parser.add_argument("-index", action="store_true", help="only process index commands, do not load data")

    # get the command line arguments
    args =  parser.parse_args()

    print(sys.path)

    # if -index is used, only process the index
    if args.index:
        print("index is true - only process index")

    try:
        print ("XML filename is ", args.xmlfile)

        # instantiate a load_spec XML file
        xml_loadfile = RX.XmlLoadFile(args.xmlfile)

        # read in the XML file and get the information out of its tags
        xml_loadfile.read_xml()
    except:
        print("No XML filename")

    if not xml_loadfile.stat_header_db_check:
        print("Do not check stat headers")

    print("--- End METdbLoad ---")

if __name__ == '__main__':

    main()
