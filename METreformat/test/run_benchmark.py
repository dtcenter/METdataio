import os
import pathlib

import pandas as pd
import yaml

from METdataio.METdbLoad.ush.read_data_files import ReadDataFiles
from METdataio.METdbLoad.ush.read_load_xml import XmlLoadFile
from METdataio.METreformat.write_stat_ascii import WriteStatAscii
import METdataio.METreformat.util as util

full_log_filename = os.path.join('../output', 'test_benchmarking_log.txt')
logger = util.get_common_logger('DEBUG', full_log_filename)


def read_input(config_file, is_tcst):
    """
       Read in the input .stat data file, return a data frame representation of all the data in the specified
       input data directory.

    :param input_data_dir: The full path of the directory where the input data is located.
    :param is_tcst: If the linetype is a TCMPR or TCDiag (.tcst file)
    :return: file_df, the dataframe representation of the input data
    """

    with open(config_file, 'r') as stream:
        try:
            parms: dict = yaml.load(stream, Loader=yaml.FullLoader)
            pathlib.Path(parms['output_dir']).mkdir(parents=True, exist_ok=True)
        except yaml.YAMLError as exc:
            print(exc)

    input_data_filename = parms['input_data_dir']
    input_data = os.path.join(os.path.dirname(__file__), input_data_filename)

    # Replacing the need for an XML specification file, pass in the XMLLoadFile and
    # ReadDataFile parameters
    rdf_obj: ReadDataFiles = ReadDataFiles()
    xml_loadfile_obj: XmlLoadFile = XmlLoadFile(None)

    # Retrieve all the filenames in the data_dir specified in the YAML config file
    load_files = xml_loadfile_obj.filenames_from_template(input_data, {})

    flags = xml_loadfile_obj.flags
    line_types = xml_loadfile_obj.line_types
    rdf_obj.read_data(flags, load_files, line_types)

    if is_tcst:
        file_df = rdf_obj.tcst_data
    else:
        file_df = rdf_obj.stat_data
    # Check if the output file already exists, if so, delete it to avoid
    # appending output from subsequent runs into the same file.
    existing_output_file = os.path.join(parms['output_dir'], parms['output_filename'])
    if os.path.exists(existing_output_file):
        os.remove(existing_output_file)

    return file_df, parms


def setup_test(yaml_file, is_tcst=False):
    """
       Read in the YAML config settings, then generate the input data as a data frame and perform reformatting.

    """

    cwd = os.path.dirname(__file__)
    full_yaml_file = os.path.join(cwd, yaml_file)
    file_df, config = read_input(full_yaml_file, is_tcst)

    return file_df, config


# BENCHMARKING
def test_tcdiag_benchmark(benchmark):
    stat_data, config = setup_test('../test/test_reformat_tcdiag.yaml', is_tcst=True)
    wsa = WriteStatAscii(config, logger)
    # reformatted_df = wsa.process_tcdiag(stat_data)
    result = benchmark(wsa.process_tcdiag, stat_data)


def test_ecnt_benchmark(benchmark):
    stat_data, config = setup_test('../test/ECNT_for_agg.yaml')

    wsa = WriteStatAscii(config, logger)

    # Benchmark
    result = benchmark(wsa.process_ecnt, stat_data)
    assert isinstance(result, pd.DataFrame)
