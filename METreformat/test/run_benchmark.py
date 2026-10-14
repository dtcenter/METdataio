import os
import pathlib

import pandas as pd
import yaml

from METdataio.METdbLoad.ush.read_data_files import ReadDataFiles
from METdataio.METdbLoad.ush.read_load_xml import XmlLoadFile
from METdataio.METreformat.write_stat_ascii import WriteStatAscii
import METdataio.METreformat.util as util
from METdataio.METreformat.test.test_reformatting import setup_test, read_input

full_log_filename = os.path.join('../output', 'test_benchmarking_log.txt')
logger = util.get_common_logger('DEBUG', full_log_filename)


# BENCHMARKING
def test_tcdiag_benchmark(benchmark):
    stat_data, config = setup_test("TCDIAG")
    wsa = WriteStatAscii(config, logger)
    # reformatted_df = wsa.process_tcdiag(stat_data)
    result = benchmark(wsa.process_tcdiag, stat_data)


def test_ecnt_benchmark(benchmark):
    stat_data, config = setup_test("ECNT", is_aggregated=False)

    wsa = WriteStatAscii(config, logger)

    # Benchmark
    result = benchmark(wsa.process_ecnt, stat_data)
    assert isinstance(result, pd.DataFrame)
