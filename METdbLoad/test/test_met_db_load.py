import pytest
from unittest.mock import patch
from METdbLoad.ush.met_db_load import main as load_main
from METdbLoad.ush.run_sql import RunSql
from METdbLoad.test.utils import dict_to_args

from METdataio.METdbLoad.test.utils import (
    get_xml_test_file,
    POINT_STAT_DATA_DIR,
    ENSEMBLE_STAT_DATA_DIR,
    GRID_STAT_DATA_DIR,
    MPR_DATA_DIR,
    MTD_DATA_DIR,
    MODE_DATA_DIR,
    TCDIAG_DATA_DIR,
    VSDB_DATA_DIR,
    RHIST_DATA_DIR,
)


def assert_count_rows(cur, table, expected_count):
    cur.execute(f"SELECT * FROM {table}")
    actual = cur.fetchall()
    assert (
        len(actual) == expected_count
    ), f"Table {table} has {len(actual)} rows. Expected {expected_count}."


@pytest.mark.parametrize(
    "met_data_dir, met_tool, expected_counts",
    [
        (
            POINT_STAT_DATA_DIR,
            "point_stat",
            {
                "line_data_vcnt": 1,
                "line_data_fho": 24,
                "line_data_cts": 24,
                "line_data_ctc": 24,
                "line_data_cnt": 10,
                "line_data_vl1l2": 1,
            },
        ),
        (
            ENSEMBLE_STAT_DATA_DIR,
            "ensemble_stat",
            {
                "line_data_orank": 1426,
                "line_data_phist_bin": 180,
                "line_data_rhist_rank": 84,
                "line_data_phist": 9,
            },
        ),
        (
            GRID_STAT_DATA_DIR,
            "grid_stat",
            {
                "line_data_eclv": 9,
                "line_data_fho": 9,
                "line_data_eclv_pnt": 171,
                "line_data_cts": 9,
                "line_data_ctc": 9,
                "line_data_cnt": 3,
            },
        ),
        (
            MPR_DATA_DIR,
            "grid_stat",
            {
                "line_data_mpr": 1386,
            },
        ),
        (
            TCDIAG_DATA_DIR,
            "tc_diag",
            {
                "line_data_tcmpr": 703,
                "line_data_tcdiag_diag": 592,
                "line_data_tcdiag": 168,
            },
        ),
        (
            MTD_DATA_DIR,
            "mtd",
            {
                "mtd_2d_obj": 278,
                "mtd_3d_obj_single": 8,
            },
        ),
        (
            MODE_DATA_DIR,
            "mtd",
            {
                "mode_cts": 2,
                "mode_obj_pair": 5,
                "mode_obj_single": 6,
            },
        ),
        (
            VSDB_DATA_DIR,
            "vsdb",
            {
                "line_data_ctc": 19,
                "line_data_sl1l2": 5,
            },
        ),
        (
            RHIST_DATA_DIR,
            "ensemble_stat",
                        {
                "line_data_rhist": 2,
                "line_data_rhist_rank":22,
                "line_data_phist": 2,
                "line_data_phist_bin": 40,
                "line_data_ecnt": 2,
                "line_data_relp": 2,
                "line_data_relp_ens": 20,
            },
        ),
    ],
)
def test_met_db_table_counts(
    emptyDB,
    testRunSql,
    tmp_path,
    met_data_dir,
    met_tool,
    expected_counts,
):

    test_args = dict_to_args(
        {
            "xmlfile": str(get_xml_test_file(tmp_path, met_data_dir, met_tool)),
            "index": "true",
            "tmpdir": [str(tmp_path)],
            "loglevel": None,
        }
    )

    load_main(test_args)

    for table, expected_count in expected_counts.items():
        assert_count_rows(testRunSql.cur, table, expected_count)


def test_met_db_indexes(
    emptyDB,
    testRunSql,
    tmp_path,
):
    # set up to "apply_indexes"
    test_args = dict_to_args(
        {
            "xmlfile": str(
                get_xml_test_file(
                    tmp_path,
                    POINT_STAT_DATA_DIR,
                    "point_stat",
                    {"apply_indexes": "true"},
                )
            ),
            "index": "false",
            "tmpdir": [str(tmp_path)],
            "loglevel": None
        }
    )

    # file_id and stat_header are already indexed
    idx_cnt = testRunSql.cur.execute("SHOW INDEX FROM line_data_fho")
    assert idx_cnt == 2

    # sys.exit is called after processing indexes
    with pytest.raises(SystemExit):
        load_main(test_args)

    # check extra indicies have been created
    idx_cnt = testRunSql.cur.execute("SHOW INDEX FROM line_data_fho")
    assert idx_cnt == 5

    # check no data was loaded
    assert_count_rows(testRunSql.cur, "line_data_fho", 0)

    # check sys.exit called on error
    with pytest.raises(SystemExit):
        with patch.object(RunSql, "apply_indexes", side_effect=KeyError):
            load_main(test_args)

@pytest.mark.parametrize(
        "met_data_dir, met_tool, expected_counts, local_infile",
        [
        (
            POINT_STAT_DATA_DIR,
            "point_stat",
            {
                "line_data_vcnt": 1,
                "line_data_fho": 24,
                "line_data_cts": 24,
                "line_data_ctc": 24,
                "line_data_cnt": 10,
                "line_data_vl1l2": 1,
            },
            'false',
        ),
        (
            POINT_STAT_DATA_DIR,
            "point_stat",
            {
                "line_data_vcnt": 1,
                "line_data_fho": 24,
                "line_data_cts": 24,
                "line_data_ctc": 24,
                "line_data_cnt": 10,
                "line_data_vl1l2": 1,
            },
            'true',
        ),
        (
            MTD_DATA_DIR,
            "mtd",
            {
                "mtd_2d_obj": 278,
                "mtd_3d_obj_single": 8,
            },
            'false',
        ),
        (
        MTD_DATA_DIR,
            "mtd",
            {
                "mtd_2d_obj": 278,
                "mtd_3d_obj_single": 8,
            },
            'true',
        ),
        ],
)
def test_local_in_file(emptyDB, testRunSql, tmp_path, met_data_dir, met_tool, expected_counts, local_infile):
    """check we get the same result when local_file is on or off"""

    test_args = dict_to_args(
        {
            "xmlfile": str(get_xml_test_file(tmp_path, met_data_dir, met_tool, local_infile=local_infile)),
            "index": "false",
            "tmpdir": [str(tmp_path)],
            "loglevel": None,
        }
    )

    load_main(test_args)

    for table, expected_count in expected_counts.items():
        assert_count_rows(testRunSql.cur, table, expected_count)
