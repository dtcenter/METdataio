#!/usr/bin/env python3

'''
Program Name: read_load_xml.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Read load_spec XML file
Parameters: N/A
Input Files: load_spec XML file
Output Files: N/A
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
'''

import sys
import os
from lxml import etree
import METdb.METdbLoad.ush.constants as CN


class XmlLoadFile:
    """! Class to read in load_spec xml file
        Returns:
           N/A
    """

    def __init__(self, xmlfile):
        # set the defaults
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
        self.load_note = None
        self.load_xml = True
        self.group = CN.DEFAULT_DATABASE_GROUP
        self.description = None

        self.load_files = []
        self.line_types = []

    def read_xml(self):
        """! Read in load_spec xml file, sotre values as attributes
            Returns:
               N/A
        """
        try:
            # parse the XML file
            tree = etree.parse(self.xmlfilename)
            root = tree.getroot()
            print(root.tag)
        except (RuntimeError, TypeError, NameError):
            print("***", sys.exc_info()[0], "in", "read_xml", "***")

        folder_template = None
        template_fillins = {}

        try:
            # extract values from load_spec XML tags, store in attributes of class XmlLoadFile
            for child in root:
                print("child: ", child.tag, child.text)
                for subchild in list(child):
                    print("subchild: ", subchild.tag, subchild.text)
                if child.tag.lower() == "connection":
                    for subchild in list(child):
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
                elif child.tag.lower() == "load_files":
                    for subchild in list(child):
                        self.load_files.append(subchild.text)
                elif child.tag.lower() == "folder_tmpl":
                    folder_template = child.text
                # get the values to fill in to the folder template
                elif child.tag.lower() == "load_val":
                    for subchild in list(child):
                        template_key = subchild.get("name")
                        template_values = []
                        for template_value in list(subchild):
                            template_values.append(template_value.text)
                        template_fillins[template_key] = template_values
                elif child.tag.lower() == "verbose":
                    if child.text.lower() == CN.LCTRUE:
                        self.verbose = True
                elif child.tag.lower() == "drop_indexes":
                    if child.text.lower() == CN.LCTRUE:
                        self.drop_indexes = True
                elif child.tag.lower() == "apply_indexes":
                    if child.text.lower() == CN.LCFALSE:
                        self.apply_indexes = False
                elif child.tag.lower() == "stat_header_db_check":
                    if child.text.lower() == CN.LCFALSE:
                        self.stat_header_db_check = False
                elif child.tag.lower() == "mode_header_db_check":
                    if child.text.lower() == CN.LCFALSE:
                        self.mode_header_db_check = False
                elif child.tag.lower() == "mtd_header_db_check":
                    if child.text.lower() == CN.LCFALSE:
                        self.mtd_header_db_check = False
                elif child.tag.lower() == "load_stat":
                    if child.text.lower() == CN.LCFALSE:
                        self.load_stat = False
                elif child.tag.lower() == "load_mode":
                    if child.text.lower() == CN.LCFALSE:
                        self.load_mode = False
                elif child.tag.lower() == "load_mtd":
                    if child.text.lower() == CN.LCFALSE:
                        self.load_mtd = False
                elif child.tag.lower() == "load_mpr":
                    if child.text.lower() == CN.LCTRUE:
                        self.load_mpr = True
                elif child.tag.lower() == "load_orank":
                    if child.text.lower() == CN.LCTRUE:
                        self.load_orank = True
                elif child.tag.lower() == "force_dup_file":
                    if child.text.lower() == CN.LCTRUE:
                        self.force_dup_file = True
                elif child.tag.lower() == "insert_size":
                    if child.text.isdigit():
                        self.insert_size = int(child.text)
                # group and description for putting databases into groups/categories
                elif child.tag.lower() == "group":
                    self.group = child.text
                elif child.tag.lower() == "description":
                    self.description = child.text
                # load_note and load_xml are used to put a note in the database
                elif child.tag.lower() == "load_note":
                    self.load_note = child.text
                elif child.tag.lower() == "load_xml":
                    if child.text.lower() == CN.LCFALSE:
                        self.load_xml = False
                # MET line types to load. If omitted, all line types are loaded
                elif child.tag.lower() == "line_type":
                    self.line_type_load = True
                    for subchild in list(child):
                        self.line_types.append(subchild.text)
                else:
                    print("unknown tag:", child.tag)
        except (RuntimeError, TypeError, NameError):
            print("***", sys.exc_info()[0], "in", "read_xml", "***")


        print("group is:", self.group)
        print("db_name is:", self.db_name)

        if folder_template is not None:
            self.load_files = self.filenames_from_template(folder_template, template_fillins)

        print("load_files are:", len(self.load_files), self.load_files)

    def filenames_from_template(self, folder_template, template_fillins):
        """! given a folder template and the values to fill in, generates list of filenames
            Returns:
               list of filenames
        """
        print("folder template is:", folder_template)

        try:

            fillins_open = folder_template.count("{")
            if fillins_open != folder_template.count("}"):
                raise ValueError("mismatched curly braces")
            # remove any values that are not in the template
            if template_fillins:
                copy_template_fillins = dict(template_fillins)
                for key in copy_template_fillins:
                    if key not in folder_template:
                        del template_fillins[key]
            if fillins_open > len(template_fillins):
                raise ValueError("not enough template fillin values")
            # generate a list of directories with all combinations of values filled in
            load_dirs = [folder_template]
            for key in template_fillins:
                alist = []
                for fvalue in template_fillins[key]:
                    for wvalue in load_dirs:
                        alist.append(wvalue.replace("{" + key + "}", fvalue))
                load_dirs = alist
            # find all files in directories, append path to them, and put on load_files list
            file_list = []
            for file_dir in load_dirs:
                for file_name in os.listdir(file_dir):
                    file_list.append(file_dir + "/" + file_name)
            print("template_fillins are:", template_fillins)
            print("load_dirs", load_dirs)

        except ValueError as value_error:
            print("***", sys.exc_info()[0], "in", "filenames_from_template", "***")
            print(value_error)
        except (RuntimeError, TypeError, NameError):
            print("***", sys.exc_info()[0], "in", "filenames_from_template", "***")

        return file_list
