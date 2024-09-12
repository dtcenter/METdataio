import pytest
from unittest import mock
from pymysql import OperationalError

from METdbLoad.ush.met_db_load import main as load_main
from METdbLoad.ush import constants as CN
from METdbLoad.test.utils import dict_to_args
from METdataio.METdbLoad.test.utils import (
    get_xml_test_file,
    GRID_STAT_DATA_DIR,
)


def populate_some_data(
    tmp_path, met_data_dir=GRID_STAT_DATA_DIR, met_tool="grid_stat", load_flags={}
):
    test_args = dict_to_args(
        {
            "xmlfile": str(
                get_xml_test_file(tmp_path, met_data_dir, met_tool, load_flags)
            ),
            "index": "true",
            "tmpdir": [str(tmp_path)],
        }
    )
    load_main(test_args)


def test_get_file_name(tmp_path, emptyDB, testRunSql):
    file_name = testRunSql.get_file_name(1, testRunSql.cur)
    assert file_name == None

    populate_some_data(tmp_path)

    file_name = testRunSql.get_file_name(1, testRunSql.cur)
    assert file_name == "grid_stat_GTG_latlon_060000L_20130827_180000V.stat"

    # check for a nonexistant id
    file_name = testRunSql.get_file_name(9999999, testRunSql.cur)
    assert file_name == None


@pytest.mark.parametrize(
    "drop, cmds",
    (
        [True, CN.DROP_INDEXES_QUERIES],
        [False, CN.CREATE_INDEXES_QUERIES],
    ),
)
def test_apply_indexes(testRunSql, drop, cmds):

    mock_cursor = mock.MagicMock()
    testRunSql.apply_indexes(drop, mock_cursor)

    # Check the correct commands were executed
    assert len(mock_cursor.execute.call_args_list) == len(cmds)
    for cmd in cmds:
        mock_cursor.execute.assert_any_call(cmd)

    # check no exception is raised on pymysql error
    mock_cursor.execute.side_effect = OperationalError
    testRunSql.apply_indexes(drop, mock_cursor)
