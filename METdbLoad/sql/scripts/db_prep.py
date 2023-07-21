'''
    Creates the METviewer database to store MET output.
'''

import yaml
import argparse
from dataclasses import dataclass
import logging
import xml.etree.ElementTree as et

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

    def __init__(self, config_obj: dict):
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


    def update_spec_file(self):
        '''
           Edit the XML specification file to reflect the settings in the
           YAML configuration file.
        '''

        specification_tree = et.parse(self.xml_spec_file)
        myroot = specification_tree.getroot()
        host = 
            host.txt = str(self.host_name)



if __name__ == "__main__":

    # Create a parser
    parser = argparse.ArgumentParser()

    # Add arguments to the parser
    parser.add_argument('action')
    parser.add_argument('config_file')

    # Parse the arguments
    args = parser.parse_args()

    # Get arguments value
    action = args.action
    config_file = args.config_file

    action_requested = str(action).lower()
    logging.debug(f'Action requested: {action_requested}')
    logging.debug(f'Config file to use: {str(config_file)}')

    with open(config_file, 'r') as cf:
        db_config_info = yaml.safe_load(cf)
        db_loader = DatabaseLoadingInfo(db_config_info)
        if action_requested == 'create':
            db_loader.update_spec_file()
        elif action_requested == 'delete':
            pass
        else:
            logging.warning(f'{action_requested} is not a supported option.')
