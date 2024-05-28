import argparse
import logging
import sys
import getpass
from METreformat.context_filter import ContextFilter as cf

def read_config_from_command_line():
    """
        Read the "custom" config file from the command line

        Args:

        Returns:
            The full path to the config file
    """
    # Create Parser
    parser = argparse.ArgumentParser(description='parser for yaml config file')

    # Add arguments
    parser.add_argument('Path', metavar='path', type=str,
                        help='the full path to config file')

    # Execute the parse_args() method
    args = parser.parse_args()
    return args.Path



def get_common_logger(log_level, log_filename):
    '''
      Args:
         @param log_level:  The log level
         @param log_filename: The full path to the log file + filename
      Returns:
         common_logger: the logger common to all the METplotpy modules that are
                        currently in use by a plot type.
    '''

    # Supported log levels.
    log_level = log_level.upper()
    log_levels = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO,
                  'WARNING': logging.WARNING, 'ERROR': logging.ERROR,
                  'CRITICAL': logging.CRITICAL}

    if log_filename.lower() == 'stdout':
        logging.basicConfig(level=log_levels[log_level],
                            format='%(asctime)s||User:%('
                                   'user)s||%(pathname)s: %(funcName)s|| [%(levelname)s]: %('
                                   'message)s',
                            datefmt='%Y-%m-%d %H:%M:%S',
                            stream=sys.stdout)
    else:
        logging.basicConfig(level=log_levels[log_level],
                            format='%(asctime)s||User:%('
                                   'user)s||%(pathname)s: %(funcName)s|| [%(levelname)s]: %('
                                   'message)s',
                            datefmt='%Y-%m-%d %H:%M:%S',
                            filename=log_filename,
                            filemode='w')
    common_logger = logging.getLogger(__name__)
    dbload_read_files_logger = logging.getLogger(name='read_data_files').setLevel(logging.DEBUG)
    dbload_read_load_logger = logging.getLogger(name='read_load_xml').setLevel(logging.DEBUG)
    f = cf()
    common_logger.addFilter(f)

    return common_logger
