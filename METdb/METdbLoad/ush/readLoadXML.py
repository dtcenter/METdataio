#!/usr/bin/env python3

'''
Program Name: readLoadXML.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Read load_spec XML file
Parameters: N/A
Input Files: load_spec XML file
Output Files: N/A
Copyright 2019 UCAR/NCAR/RAL, Colorado State University, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
'''

import sys
from lxml import etree
from lxml import objectify
import METdb.METdbLoad.ush.constants as CN

class XmlLoadFile:
    def __init__(self, xmlfile):
        self.xmlfilename = xmlfile
        self.db_host = None
        self.db_name = None
        self.db_user = None
        self.db_password = None
        self.db_management_system = "mysql"
        self.db_driver = None
        self.mod_schema = False
        self.line_type_load = False
        self.load_stat = True
        self.load_mode = True
        self.load_mtd = True
        self.load_mpr = False
        self.load_orank = False
        self.force_dup_file = False
        self.verbose = False
        self.insert_size = 1
        self.stat_header_db_check = True
        self.mode_header_db_check = True
        self.mtd_header_db_check = True
        self.drop_indexes = False
        self.apply_indexes = True
        self.folder_tmpl = None
        self.load_note = None
        self.load_xml = True
        self.group = CN.DEFAULT_DATABASE_GROUP
        self.description = None

    def read_xml(self):
        try:
            tree = etree.parse(self.xmlfilename)
            root = tree.getroot()
            print (root.tag)
        except:
            print("Couldn't read XML file")

        for child in root:
            print ("child: ", child.tag, child.text)
            for subchild in child.getchildren():
                print("subchild: ", subchild.tag, subchild.text)
            if child.tag == "connection":
                for subchild in child.getchildren():
                    if subchild.tag.lower() == "host":
                        self.db_host = subchild.text
                    elif subchild.tag.lower() == "user":
                        self.db_user = subchild.text
                    elif subchild.tag.lower() == "password":
                        self.db_password = subchild.text
                    elif subchild.tag.lower() == "database":
                        self.db_name = subchild.text
                    elif subchild.tag.lower() == "management_system":
                        self.db_user = subchild.text
            elif child.tag == "verbose":
                if child.text.lower() == "true":
                    self.verbose = True
            elif child.tag == "stat_header_db_check":
                if child.text.lower() == "false":
                    self.stat_header_db_check = False
            elif child.tag == "mode_header_db_check":
                if child.text.lower() == "false":
                    self.mode_header_db_check = False
            elif child.tag == "mtd_header_db_check":
                if child.text.lower() == "false":
                    self.mtd_header_db_check = False
            elif child.tag == "load_stat":
                if child.text.lower() == "false":
                    self.load_stat = False
            elif child.tag == "load_mode":
                if child.text.lower() == "false":
                    self.load_mode = False
            elif child.tag == "load_mtd":
                if child.text.lower() == "false":
                    self.load_std = False
            elif child.tag == "load_mpr":
                if child.text.lower() == "true":
                    self.load_mpr = True
            elif child.tag == "load_orank":
                if child.text.lower() == "true":
                    self.load_orank = True
            elif child.tag == "group":
                self.group = child.text
            elif child.tag == "description":
                self.description = child.text
            elif child.tag == "force_dup_file":
                if child.text.lower() == "true":
                    self.load_orank = True
            elif child.tag == "drop_indexes":
                if child.text.lower() == "true":
                    self.drop_indexes = True
            elif child.tag == "apply_indexes":
                if child.text.lower() == "false":
                    self.apply_indexes = False

        print("group is:", self.group)
        print("db_name is:", self.db_name)
        print("load_mode is:", self.load_mode)

