#!/usr/bin/env python3

"""
Program Name: read_load_xml.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Read load_spec XML file
Parameters: N/A
Input Files: load_spec XML file
Output Files: N/A
Copyright 2020 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member
# constants exist in constants.py

import sys
import os
from pathlib import Path
import logging
import pandas as pd
from lxml import etree

import constants as CN


class XmlLoadFile:
    """! Class to read in load_spec xml file
        Returns:
           N/A
    """

    def __init__(self, xmlfile):
        # set the defaults
        self.xmlfilename = xmlfile

        self.connection = {}
        self.connection['db_port'] = CN.SQL_PORT

        self.insert_size = 1
        self.load_note = None
        self.group = CN.DEFAULT_DATABASE_GROUP
        self.description = "None"
        self.xml_str = None

        self.flags = {}
        self.flags['line_type_load'] = False
        self.flags['load_stat'] = True
        self.flags['load_mode'] = True
        self.flags['load_mtd'] = True
        self.flags['load_mpr'] = False
        self.flags['load_orank'] = False
        self.flags['force_dup_file'] = False
        self.flags['verbose'] = False
        self.flags['stat_header_db_check'] = True
        self.flags['tcst_header_db_check'] = True
        self.flags['mode_header_db_check'] = True
        self.flags['mtd_header_db_check'] = True
        self.flags['drop_indexes'] = False
        self.flags['apply_indexes'] = False
        self.flags['load_xml'] = True

        self.load_files = []
        self.line_types = []

    def read_xml(self):
        """! Read in load_spec xml file, store values as class attributes
            Returns:
               N/A
        """

        logging.debug("[--- Start read_xml ---]")
        logging.info("User name is: %s", os.getlogin())

        try:

            # check for existence of XML file
            if not Path(self.xmlfilename).is_file():
                sys.exit("*** XML file " + self.xmlfilename + " can not be found!")

            # parse the XML file
            logging.info('Reading XML Load file')
            parser = etree.XMLParser(remove_comments=True, resolve_entities=False)
            tree = etree.parse(self.xmlfilename, parser=parser)
            root = tree.getroot()

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_xml ***", sys.exc_info()[0])
            sys.exit("*** Parsing error(s) in XML file!")

        # Extract values from load_spec XML tags, store in attributes of class XmlLoadFile
        try:

            # Extract values for connecting to database
            if root.xpath("connection"):
                self.read_db_connect(root)
                logging.info("Database name is: %s", self.connection['db_database'])

            # group and description for putting databases into groups/categories
            if root.xpath("group") and root.xpath("description"):
                self.group = root.xpath("group")[0].text
                self.description = root.xpath("description")[0].text

            # load_note and load_xml are used to put a note in the database
            if root.xpath('load_note'):
                self.load_note = root.xpath("load_note")[0].text

            # MET line types to load. If omitted, all line types are loaded
            if root.xpath('line_type'):
                self.flags['line_type_load'] = True
                self.line_types = [x.text.upper() for x in root.xpath('line_type')[0]]

            # insert_size value is an integer
            if root.xpath('insert_size') and root.xpath('insert_size')[0].text.isdigit():
                self.insert_size = int(root.xpath('insert_size')[0].text)

            # Handle flags with a default of True
            default_true = ["stat_header_db_check", "mode_header_db_check",
                            "mtd_header_db_check", "tcst_header_db_check",
                            "load_stat", "load_mode", "load_mtd", "load_xml"]

            self.flag_default_true(root, default_true)

            # Handle flags with a default of False
            default_false = ["verbose", "drop_indexes", "apply_indexes",
                             "load_mpr", "load_orank", "force_dup_file"]

            self.flag_default_false(root, default_false)

            # if requested, get a string of the XML to put in the database
            if self.flags['load_xml']:
                self.xml_str = etree.tostring(tree).decode().replace('\n', '').replace(' ', '')

            # Get a list of all of the file names to load
            if root.xpath('load_files'):
                self.load_files = [x.text for x in root.xpath('load_files')[0]]
            else:
                # Or get info on file template, fill-in values, and dates, if needed
                self.read_file_info(root)

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_xml ***", sys.exc_info()[0])
            sys.exit("*** Error(s) found while reading XML file!")

        # This removes duplicate file names. do we want that?
        if self.load_files:
            self.load_files = list(dict.fromkeys(self.load_files))

        # Remove directory names
        self.load_files = [lf for lf in self.load_files if '.' in lf.split('/')[-1]]

        logging.info("Initial number of files: %s", str(len(self.load_files)))

        logging.debug("[--- End read_xml ---]")

    def read_file_info(self, root):
        """! Gather info on file template, fill-in values, and dates
            Returns:
               N/A
        """
        try:
            folder_template = None
            template_fills = {}
            date_list = {}
            all_dates = []

            # Handle the date_list tag and its child tags
            if root.xpath('date_list'):
                date_list = {x.tag.lower(): x.text for x in root.xpath('date_list')[0]}
                date_list['name'] = root.xpath('date_list')[0].attrib['name']

            # if the date_list tag is included, generate a list of dates
            if "start" in date_list.keys() and "end" in date_list.keys():
                all_dates = self.filenames_from_date(date_list)

            if root.xpath("folder_tmpl"):
                folder_template = root.xpath("folder_tmpl")[0].text

            # get the values to fill in to the folder template
            field_names = [x.attrib['name'] for x in root.xpath('load_val')[0].xpath('field')]

            for field_name in field_names:
                # Process zero or more val tags
                xml_exp = "//field[@name='" + field_name + "']/val"
                if root.xpath(xml_exp):
                    template_fills[field_name] = \
                        [x.text for x in root.xpath(xml_exp)]

                # Process date_list tag, if any
                xml_exp = "//field[@name='" + field_name + "']/date_list"
                if root.xpath(xml_exp):
                    template_fills[field_name] = \
                        [root.xpath(xml_exp)[0].attrib['name']]
                    date_field = field_name

            # If the date_list tag was used correctly, put the dates in
            if all_dates and template_fills[date_field][0] == date_list["name"]:
                template_fills[date_field] = all_dates

            # Generate all possible path/filenames from folder template
            if folder_template and template_fills:
                self.load_files = self.filenames_from_template(folder_template, template_fills)

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_xml read_file_info ***", sys.exc_info()[0])
            sys.exit("*** Error(s) found while reading XML file info!")

    def read_db_connect(self, root):
        """! Gather values from tags that have info on database connection
            Returns:
               N/A
        """
        try:
            host_and_port = root.xpath('connection')[0].xpath('host')[0].text
            if host_and_port:
                host_and_port = host_and_port.split(":")
                self.connection['db_host'] = host_and_port[0]
                if len(host_and_port) > 1:
                    self.connection['db_port'] = int(host_and_port[1])
                else:
                    self.connection['db_port'] = CN.SQL_PORT
            else:
                logging.error("!!! XML must include host tag")
                raise NameError("Missing required host tag")

            if root.xpath('connection')[0].xpath('database'):
                self.connection['db_database'] = \
                    root.xpath('connection')[0].xpath('database')[0].text
            else:
                logging.error("!!! XML must include database tag")
                raise NameError("Missing required database tag")

            if not self.connection['db_database'].startswith("mv_"):
                logging.warning("!!! Database not visible unless name starts with mv_")

            self.connection['db_user'] = \
                root.xpath('connection')[0].xpath('user')[0].text
            self.connection['db_password'] = \
                root.xpath('connection')[0].xpath('password')[0].text
            if ((not self.connection['db_user']) or
                    (not self.connection['db_password'])):
                logging.error("!!! XML must include user and password tags")
                raise NameError("Missing required user or password tag or both")

            self.connection['db_management_system'] = \
                root.xpath('connection')[0].xpath('management_system')[0].text
            if not self.connection['db_management_system']:
                self.connection['db_management_system'] = "mysql"

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read_xml read_db_connect ***", sys.exc_info()[0])
            sys.exit("*** Error(s) found while reading XML file connection tag!")

    def flag_default_true(self, root, default_true):
        """! Given list of flags that default to true, set to false if needed
            Returns:
               N/A
        """
        for flag_name in default_true:
            if root.xpath(flag_name) and root.xpath(flag_name)[0].text.lower() == CN.LC_FALSE:
                self.flags[root.xpath(flag_name)[0].tag.lower()] = False

    def flag_default_false(self, root, default_false):
        """! Given list of flags that default to false, set to true if needed
            Returns:
               N/A
        """
        for flag_name in default_false:
            if root.xpath(flag_name) and root.xpath(flag_name)[0].text.lower() == CN.LC_TRUE:
                self.flags[root.xpath(flag_name)[0].tag.lower()] = True

    @staticmethod
    def filenames_from_date(date_list):
        """! given date format, start and end dates, and increment, generates list of dates
            Returns:
               list of dates
        """
        logging.debug("date format is: %s", date_list["format"])

        try:
            date_format = date_list["format"]
            # check to make sure that the date format string only has known characters
            if set(date_format) <= CN.DATE_CHARS:
                # Change the java formatting string to a Python formatting string
                for java_date, python_date in CN.DATE_SUBS.items():
                    date_format = date_format.replace(java_date, python_date)
                # format the start and end dates
                date_start = pd.to_datetime(date_list["start"], format=date_format)
                date_end = pd.to_datetime(date_list["end"], format=date_format)
                date_inc = int(date_list["inc"])
                all_dates = []
                while date_start < date_end:
                    all_dates.append(date_start.strftime(date_format))
                    date_start = date_start + pd.Timedelta(seconds=date_inc)
                all_dates.append(date_end.strftime(date_format))
            else:
                logging.error("*** date_list tag has unknown characters ***")

        except ValueError as value_error:
            logging.error("*** %s in filenames_from_date ***", sys.exc_info()[0])
            logging.error(value_error)
            sys.exit("*** Value Error found while expanding XML date format!")
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in filenames_from_date ***", sys.exc_info()[0])
            sys.exit("*** Error found while expanding XML date format!")

        return all_dates

    @staticmethod
    def filenames_from_template(folder_template, template_fills):
        """! given a folder template and the values to fill in, generates list of filenames
            Returns:
               list of filenames
        """
        logging.debug("folder template is: %s", folder_template)

        try:
            fills_open = folder_template.count("{")
            if fills_open != folder_template.count("}"):
                raise ValueError("mismatched curly braces")

            # remove any fill values that are not in the template
            not_in = []
            if template_fills:
                not_in = [tf for tf in template_fills.keys() if not (tf in folder_template)]

            for wrong_key in not_in:
                del template_fills[wrong_key]

            if fills_open > len(template_fills):
                raise ValueError("not enough template fill values")

            # generate a list of directories with all combinations of values filled in
            load_dirs = [folder_template]
            for key in template_fills:
                alist = []
                for tvalue in load_dirs:
                    alist = alist + \
                        [tvalue.replace("{" + key + "}", x) for x in template_fills[key]]
                load_dirs = alist

            # find all files in directories, append path to them, and put on load_files list
            file_list = []
            for file_dir in load_dirs:
                if os.path.exists(file_dir):
                    file_list = file_list + [os.path.join(file_dir, x)
                                             for x in os.listdir(file_dir)]

        except ValueError as value_error:
            logging.error("*** %s in filenames_from_template ***", sys.exc_info()[0])
            logging.error(value_error)
            sys.exit("*** Value Error found while expanding XML folder templates!")
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in filenames_from_template ***", sys.exc_info()[0])
            sys.exit("*** Error found while expanding XML folder templates!")

        return file_list
