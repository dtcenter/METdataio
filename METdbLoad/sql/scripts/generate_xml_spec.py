'''
    Creates the METviewer database to store MET output.
'''
import os.path
import subprocess

import yaml
import argparse
from dataclasses import dataclass
import logging

logging.basicConfig(encoding='utf-8', level=logging.DEBUG)


@dataclass
class DatabaseLoadingInfo:
    '''
        Data class for keeping the relevant information for loading the
        METviewer database.
    '''

    db_name: str
    user_name: str
    password: str
    host_name: str
    port_number: int
    group: str
    schema_path: str
    data_dir: str
    xml_spec_file: str
    load_stat: bool
    load_mode: bool
    load_mtd: bool
    load_mpr: bool
    load_orank: bool
    config_file_dir: str

    def __init__(self, config_obj: dict, config_file_dir:str):
        '''

        Args:
            config_obj: A dictionary containing the
                        settings to be used in creating the database.
        '''

        self.db_name = config_obj['dbname']
        self.user_name = config_obj['username']
        self.password = config_obj['password']
        self.host_name = config_obj['host']
        self.port_number = config_obj['port']
        self.group = config_obj['group']
        self.schema_path = config_obj['schema_location']
        self.data_dir = config_obj['data_dir']
        self.xml_spec_file = config_obj['xml_specification']
        self.load_stat = config_obj['load_stat']
        self.load_mode = config_obj['load_mode']
        self.load_mtd = config_obj['load_mtd']
        self.load_mpr = config_obj['load_mpr']
        self.load_orank = config_obj['load_orank']
        self.description = config_obj['description']
        self.config_file_dir = config_file_dir


    def update_spec_file(self):
        '''
           Edit the XML specification file to reflect the settings in the
           YAML configuration file.
        '''

        # Assign the host with the host and port assigned in the YAML config file
        import xml.etree.ElementTree as et
        tree = et.parse(self.xml_spec_file)
        root = tree.getroot()

        for host in root.iter('host'):
            host.text = self.host_name + ":" + str(self.port_number)

        for dbname in root.iter('database'):
            dbname.text = self.db_name

        for user in root.iter('user'):
            user.text = self.user_name

        for password in root.iter('password'):
            password.text = self.password

        for data_folder in root.iter('folder_tmpl'):
            data_folder.text = self.data_dir

        for group in root.iter('group'):
            group.text = self.group

        for desc in root.iter('description'):
            desc.text = self.description

        tree.write(os.path.join(self.config_file_dir, 'load_met.xml'))



if __name__ == "__main__":

    # Create a parser
    parser = argparse.ArgumentParser()

    # Add arguments to the parser
    parser.add_argument('config_file')

    # Parse the arguments
    args = parser.parse_args()

    # Get arguments value
    config_file = args.config_file

    logging.debug(f'Config file to use: {str(config_file)}')
    config_file_dir = os.path.dirname(config_file)
    logging.debug(f'Directory of config file: {config_file_dir}')

    with open(config_file, 'r') as cf:
        db_config_info = yaml.safe_load(cf)
        db_loader = DatabaseLoadingInfo(db_config_info, config_file_dir)
        db_loader.update_spec_file()
