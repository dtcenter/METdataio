"""Test the command line entry point of write_stat_ascii and reading its config."""
import os
import sys
from unittest.mock import MagicMock

import pandas as pd
import pytest

import METreformat.util as util
from METreformat import write_stat_ascii

# Top-level directory of the test input data, read from the METDATAIO_TEST_INPUT_DIR
# environment variable. Defaults to METdataio_Data in the top of the repository.
INPUT_DIR = os.path.abspath(os.environ.get(
    'METDATAIO_TEST_INPUT_DIR',
    os.path.join(os.path.dirname(__file__), '..', '..', 'METdataio_Data')
))


def make_parms(tmp_path, line_type, input_data_dir, output_filename='reformatted.data'):
    return {
        'input_stats_aggregated': True,
        'output_dir': str(tmp_path / 'output'),
        'output_filename': output_filename,
        'input_data_dir': os.path.join(INPUT_DIR, input_data_dir),
        'log_directory': str(tmp_path / 'logs'),
        'log_filename': 'reformat_log.txt',
        'log_level': 'WARNING',
        'line_type': line_type,
        'keep_all_cols': False,
    }


def write_config(tmp_path, parms):
    config_file = tmp_path / 'config.yaml'
    config_file.write_text(''.join(f'{key}: {value}\n' for key, value in parms.items()))
    return str(config_file)


def test_config_file_complete(tmp_path):
    mock_logger = MagicMock()
    parms = make_parms(tmp_path, 'FHO', 'point_stat')
    assert write_stat_ascii.config_file_complete(parms, mock_logger)
    mock_logger.error.assert_not_called()

    # a setting without a value
    assert not write_stat_ascii.config_file_complete({**parms, 'output_dir': None}, mock_logger)
    mock_logger.error.assert_called_with('ERROR: Missing the value for the output_dir setting.')

    # a missing setting
    del parms['line_type']
    assert not write_stat_ascii.config_file_complete(parms, mock_logger)
    mock_logger.error.assert_called_with(
        'ERROR: The line_type setting is missing in the YAML config file')


def test_read_config_from_command_line(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['write_stat_ascii.py', '/path/to/config.yaml'])
    assert util.read_config_from_command_line() == '/path/to/config.yaml'


def test_main(tmp_path, monkeypatch):
    """Reformat the FHO lines in a point_stat file from wide to long format"""
    parms = make_parms(tmp_path, 'FHO', 'point_stat', 'fho.data')
    monkeypatch.setattr(sys, 'argv', ['write_stat_ascii.py', write_config(tmp_path, parms)])

    # output from an earlier run is replaced, not appended to
    os.makedirs(parms['output_dir'])
    output_file = os.path.join(parms['output_dir'], parms['output_filename'])
    with open(output_file, 'w') as f:
        f.write('old output\n')

    write_stat_ascii.main()

    df = pd.read_csv(output_file, sep='\t')
    # point_stat/<file> has 24 FHO lines, each with F_RATE, H_RATE and O_RATE
    assert len(df) == 72
    assert df['line_type'].unique().tolist() == ['FHO']
    assert df['stat_name'].value_counts().to_dict() == {'F_RATE': 24, 'H_RATE': 24, 'O_RATE': 24}

    # HGT L0 <152 FHO line: point_stat/point_stat_FV3_GFS_v15p2_..._010000V.stat line 22
    # ... FHO 3956 0 0 0.0015167
    rows = df[(df['fcst_var'] == 'HGT') & (df['fcst_thresh'] == '<152')]
    assert rows['total'].unique().tolist() == [3956]
    assert rows.set_index('stat_name')['stat_value'].to_dict() == {
        'F_RATE': 0, 'H_RATE': 0, 'O_RATE': 0.0015167}


def test_main_incomplete_config(tmp_path, monkeypatch):
    parms = make_parms(tmp_path, 'FHO', 'point_stat')
    del parms['output_filename']
    monkeypatch.setattr(sys, 'argv', ['write_stat_ascii.py', write_config(tmp_path, parms)])
    with pytest.raises(SystemExit):
        write_stat_ascii.main()


@pytest.mark.parametrize('line_type, input_data_dir', [
    # MPR lines are only read when load_mpr is set
    ('MPR', 'mpr/climo_data'),
    # TCDIAG lines are in .tcst files, and read_input returns the tcst data
    ('TCDIAG', 'tcdiag_tcmpr'),
])
def test_read_input(tmp_path, line_type, input_data_dir):
    parms = make_parms(tmp_path, line_type, input_data_dir)
    file_df = write_stat_ascii.read_input(parms, MagicMock())
    assert line_type in file_df['line_type'].unique()
    # same counts as METdbLoad/test/test_met_db_load.py::test_met_db_table_counts
    expected_count = {'MPR': 1386, 'TCDIAG': 168}[line_type]
    assert (file_df['line_type'] == line_type).sum() == expected_count
