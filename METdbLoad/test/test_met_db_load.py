import pytest
import sys
from pathlib import Path

import pymysql
from unittest.mock import patch, MagicMock, Mock
from METdbLoad.ush.met_db_load import main as load_main
from METdbLoad.ush.met_db_load import purge_files, parse_args, next_set, print_version
from METdbLoad.ush.run_sql import RunSql
from METdbLoad.test.utils import dict_to_args
from METdbLoad.ush import constants as CN
from METdbLoad.ush.read_data_files import ReadDataFiles
from METdbLoad.ush.write_stat_sql import WriteStatSql
import METdbLoad.test.utils as utils

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
                "line_data_orank": 1921,
                "line_data_phist_bin": 360,
                "line_data_rhist_rank": 108,
                "line_data_phist": 18,
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
                "mtd_3d_obj_pair": 4,
                "mtd_header": 24,
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
                "line_data_rhist_rank": 22,
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

    load_flags = {
        "mode_header_db_check": "true",
        "mtd_header_db_check": "true",
    }

    test_args = dict_to_args(
        {
            "xmlfile": str(
                get_xml_test_file(
                    tmp_path, met_data_dir, met_tool, load_flags=load_flags
                )
            ),
            "index": "true",
            "tmpdir": [str(tmp_path)],
            "loglevel": None,
        }
    )

    load_main(test_args)

    for table, expected_count in expected_counts.items():
        assert_count_rows(testRunSql.cur, table, expected_count)


@pytest.mark.parametrize(
    "met_data_dir, met_tool, expected_counts",
    [
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
            MTD_DATA_DIR,
            "mtd",
            {
                "mtd_2d_obj": 278,
                "mtd_3d_obj_single": 8,
                "mtd_3d_obj_pair": 4,
                "mtd_header": 24,
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
    ],
)
def test_met_db_table_dups(
    emptyDB,
    testRunSql,
    tmp_path,
    met_data_dir,
    met_tool,
    expected_counts,
):

    load_flags = {
        "mode_header_db_check": "true",
        "mtd_header_db_check": "true",
        "force_dup_file": "false",
    }
    test_args = dict_to_args(
        {
            "xmlfile": str(
                get_xml_test_file(
                    tmp_path, met_data_dir, met_tool, load_flags=load_flags
                )
            ),
            "index": "true",
            "tmpdir": [str(tmp_path)],
            "loglevel": None,
        }
    )

    load_main(test_args)
    # load again to check duplicates aren't loaded in db
    load_main(test_args)

    for table, expected_count in expected_counts.items():
        assert_count_rows(testRunSql.cur, table, expected_count)


@pytest.mark.parametrize(
    "met_data_dir, met_tool, expected_counts",
    [
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
            MTD_DATA_DIR,
            "mtd",
            {
                "mtd_2d_obj": 278,
                "mtd_3d_obj_single": 8,
                "mtd_3d_obj_pair": 4,
                "mtd_header": 24 / 2,  # header not duplicated
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
    ],
)
def test_met_db_table_dups_allowed(
    emptyDB,
    testRunSql,
    tmp_path,
    met_data_dir,
    met_tool,
    expected_counts,
):

    load_flags = {
        "mode_header_db_check": "true",
        "mtd_header_db_check": "true",
        "force_dup_file": "true",
    }
    test_args = dict_to_args(
        {
            "xmlfile": str(
                get_xml_test_file(
                    tmp_path, met_data_dir, met_tool, load_flags=load_flags
                )
            ),
            "index": "true",
            "tmpdir": [str(tmp_path)],
            "loglevel": None,
        }
    )

    load_main(test_args)
    # load again to add duplicates
    load_main(test_args)

    for table, expected_count in expected_counts.items():
        assert_count_rows(testRunSql.cur, table, expected_count * 2)


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
            "loglevel": None,
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
            },
            "false",
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
            },
            "true",
        ),
        (
            MTD_DATA_DIR,
            "mtd",
            {
                "mtd_2d_obj": 278,
                "mtd_3d_obj_single": 8,
            },
            "false",
        ),
        (
            MTD_DATA_DIR,
            "mtd",
            {
                "mtd_2d_obj": 278,
                "mtd_3d_obj_single": 8,
            },
            "true",
        ),
    ],
)
def test_local_in_file(
    emptyDB, testRunSql, tmp_path, met_data_dir, met_tool, expected_counts, local_infile
):
    """check we get the same result when local_file is on or off"""

    test_args = dict_to_args(
        {
            "xmlfile": str(
                get_xml_test_file(
                    tmp_path, met_data_dir, met_tool, local_infile=local_infile
                )
            ),
            "index": "false",
            "tmpdir": [str(tmp_path)],
            "loglevel": None,
        }
    )

    load_main(test_args)

    for table, expected_count in expected_counts.items():
        assert_count_rows(testRunSql.cur, table, expected_count)


def test_empty_files(tmp_path):
    """Junk files shouldn't cause an error when running load_main"""

    met_data_dir = tmp_path / "empty_files_test"
    met_data_dir.mkdir()

    open(met_data_dir / "mtd_empty_2d.txt", "a").close()
    with open(met_data_dir / "mtd_bad_header_3d.txt", "w") as f:
        f.write("SOME HEADER INFO\n")

    with open(met_data_dir / "mtd_good_header_bad_data_3d.txt", "w") as f:
        f.write(
            "VERSION  MODEL  DESC  FCST_LEAD       FCST_VALID  OBS_LEAD        OBS_VALID  T_DELTA  FCST_T_BEG  FCST_T_END  FCST_RAD  FCST_THR  OBS_T_BEG  OBS_T_END  OBS_RAD  OBS_THR  FCST_VAR  FCST_UNITS  FCST_LEV  OBS_VAR  OBS_UNITS  OBS_LEV    OBJECT_ID   OBJECT_CAT  SPACE_CENTROID_DIST  TIME_CENTROID_DELTA  AXIS_DIFF  SPEED_DELTA  DIRECTION_DIFF  VOLUME_RATIO  START_TIME_DELTA  END_TIME_DELTA  INTERSECTION_VOLUME  DURATION_DIFF  INTEREST\n 1    2   3   xxx"
        )

    with open(met_data_dir / "grid_stat_header_only.stat", "w") as f:
        f.write(
            "VERSION MODEL DESC FCST_LEAD FCST_VALID_BEG  FCST_VALID_END  OBS_LEAD OBS_VALID_BEG   OBS_VALID_END   FCST_VAR FCST_UNITS   FCST_LEV OBS_VAR OBS_UNITS    OBS_LEV  OBTYPE VX_MASK INTERP_MTHD INTERP_PNTS FCST_THRESH         OBS_THRESH          COV_THRESH ALPHA LINE_TYPE\n"
        )

    test_args = dict_to_args(
        {
            "xmlfile": str(
                get_xml_test_file(
                    tmp_path,
                    met_data_dir,
                    "mtd",
                )
            ),
            "index": "false",
            "tmpdir": [str(tmp_path)],
            "loglevel": None,
        }
    )

    load_main(test_args)


def test_print_version():
    mock_logger = MagicMock()
    print_version(mock_logger)
    assert mock_logger.info.called
    assert mock_logger.info.call_args[0][0].startswith("METdbload Version:")

    with pytest.raises(SystemExit):
        with patch("os.path.dirname", side_effect=TypeError):
            print_version(mock_logger)
    assert mock_logger.error.call_count == 2


@pytest.mark.parametrize(
    "mid,last,expected",
    [
        (99, 100, (100, 100, 100)),
        (99, 347, (100, 200, 347)),
        (99, 147, (100, 147, 147)),
    ],
)
def test_next_set(mid, last, expected):
    assert next_set(mid, last) == expected


@pytest.mark.parametrize(
    "xml_flags,expected",
    [
        (
            {
                "load_stat": False,
                "load_mode": False,
                "load_mtd": False,
            },
            [],
        ),
        (
            {
                "load_stat": True,
                "load_mode": False,
                "load_mtd": False,
            },
            ["test.stat", "test.vsdb"],
        ),
        (
            {
                "load_stat": False,
                "load_mode": True,
                "load_mtd": True,
            },
            [
                "test_cts.txt",
                "test_obj.txt",
                "test_2d.txt",
                "test_3d_s.txt",
                "test_3d_p.txt",
            ],
        ),
    ],
)
def test_purge_files(xml_flags, expected):
    load_files = [
        "test.stat",
        "test.vsdb",
        "test_cts.txt",
        "test_obj.txt",
        "test_2d.txt",
        "test_3d_s.txt",
        "test_3d_p.txt",
    ]

    mock_logger = MagicMock()

    actual = purge_files(load_files, xml_flags, mock_logger)
    assert expected == actual


def test_purge_files_raises():
    mock_logger = MagicMock()
    with pytest.raises(SystemExit):
        purge_files([], {}, mock_logger)
    assert mock_logger.error.call_count == 2


def test_parse_args():
    good_args = [
        "met_db_load.py",
        "-index",
        "test/test_load_specification.xml",
        "--loglevel",
        "ERROR",
    ]
    bad_args = ["met_db_load.py", "-f", "test/test_load_specification.xml"]

    # should run without error
    with patch.object(sys, "argv", good_args):
        args = parse_args()
        assert args.index
        assert args.xmlfile == "test/test_load_specification.xml"
        assert args.loglevel == "ERROR"

    # produces error
    with pytest.raises(SystemExit):
        with patch.object(sys, "argv", bad_args):
            parse_args()


def load_args(tmp_path, met_data_dir, met_tool, load_flags={}, index=False):
    return dict_to_args(
        {
            "xmlfile": str(
                get_xml_test_file(tmp_path, met_data_dir, met_tool, load_flags=load_flags)
            ),
            "index": index,
            "tmpdir": [str(tmp_path)],
            "loglevel": None,
        }
    )


def test_load_in_sets(emptyDB, testRunSql, tmp_path, monkeypatch):
    """Files are loaded in sets of at most MAX_FILES files, with the same result"""
    # MTD_DATA_DIR has 3 files. With MAX_FILES = 1 they are loaded in 2 sets.
    monkeypatch.setattr(CN, "MAX_FILES", 1)
    load_main(load_args(tmp_path, MTD_DATA_DIR, "mtd", {"mtd_header_db_check": "true"}))

    expected_counts = {
        "mtd_2d_obj": 278,
        "mtd_3d_obj_single": 8,
        "mtd_3d_obj_pair": 4,
        "mtd_header": 24,
        "data_file": 3,
    }
    for table, expected_count in expected_counts.items():
        assert_count_rows(testRunSql.cur, table, expected_count)


def test_load_drop_and_apply_indexes(emptyDB, testRunSql, tmp_path):
    """drop_indexes and apply_indexes drop the indexes before loading and add them after"""
    load_main(
        load_args(
            tmp_path,
            POINT_STAT_DATA_DIR,
            "point_stat",
            {"drop_indexes": "true", "apply_indexes": "true"},
        )
    )
    # file_id and stat_header indexes, plus the 3 applied after loading
    assert testRunSql.cur.execute("SHOW INDEX FROM line_data_fho") == 5
    assert_count_rows(testRunSql.cur, "line_data_fho", 24)


def assert_db_not_locked():
    """Fail if a connection left open by a load still holds locks on the test database"""
    conn = pymysql.connect(
        host=utils.TEST_DB_HOST,
        port=utils.TEST_DB_PORT,
        user=utils.TEST_DB_USER,
        password=utils.TEST_DB_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            # an open transaction on any table would make this wait, then fail
            cur.execute("SET SESSION lock_wait_timeout = 5")
            cur.execute(f"DROP DATABASE {utils.TEST_DB_NAME}")
    finally:
        conn.close()


@pytest.mark.parametrize(
    "error, expected",
    [
        # error not handled by met_db_load is raised
        (pymysql.err.DataError(1265, "Data truncated"), pymysql.err.DataError),
        # error handled by met_db_load exits
        (RuntimeError("write failed"), SystemExit),
        # error handled by the write class, which exits
        (SystemExit("*** Error writing stat SQL line data"), SystemExit),
    ],
)
def test_write_error_closes_connection(emptyDB, tmp_path, error, expected):
    """The database connection is closed when writing the data fails"""
    with patch.object(WriteStatSql, "write_stat_data", side_effect=error):
        with pytest.raises(expected):
            load_main(load_args(tmp_path, POINT_STAT_DATA_DIR, "point_stat"))

    assert_db_not_locked()


@pytest.mark.parametrize(
    "error, expected",
    [
        (RuntimeError("read failed"), SystemExit),
        (ValueError("read failed"), ValueError),
    ],
)
def test_read_error_closes_connection(emptyDB, tmp_path, monkeypatch, error, expected):
    """The database connection opened for the first set of files is closed when
    reading the second set fails"""
    monkeypatch.setattr(CN, "MAX_FILES", 1)
    read_data = ReadDataFiles.read_data
    calls = []

    def read_then_fail(self, *args):
        calls.append(args)
        if len(calls) > 1:
            raise error
        return read_data(self, *args)

    with patch.object(ReadDataFiles, "read_data", read_then_fail):
        with pytest.raises(expected):
            load_main(load_args(tmp_path, MTD_DATA_DIR, "mtd"))

    assert len(calls) == 2
    assert_db_not_locked()


def test_load_missing_tmp_dir(tmp_path):
    args = load_args(tmp_path, POINT_STAT_DATA_DIR, "point_stat")
    args.tmpdir = [str(tmp_path / "missing")]
    with pytest.raises(SystemExit, match="tmp dir"):
        load_main(args)


def test_load_no_files_to_load(tmp_path):
    """No files are left to load when the only file type is not selected"""
    with pytest.raises(SystemExit, match="No files to load"):
        load_main(load_args(tmp_path, POINT_STAT_DATA_DIR, "point_stat", {"load_stat": "false"}))


def test_metadata_updated(emptyDB, testRunSql, tmp_path):
    """The group and description in the metadata table are updated by a later load"""
    args = load_args(tmp_path, POINT_STAT_DATA_DIR, "point_stat")
    load_main(args)
    testRunSql.cur.execute("SELECT category, description FROM metadata")
    assert testRunSql.cur.fetchall() == (("Testing", "testing with pytest"),)

    xml = Path(args.xmlfile).read_text()
    Path(args.xmlfile).write_text(xml.replace("testing with pytest", "second load"))
    load_main(args)
    testRunSql.cur.execute("SELECT category, description FROM metadata")
    assert testRunSql.cur.fetchall() == (("Testing", "second load"),)


def test_tcst_dups(emptyDB, testRunSql, tmp_path):
    """Loading the same TC-ST files again does not add duplicate lines"""
    args = load_args(tmp_path, TCDIAG_DATA_DIR, "tc_diag", {"force_dup_file": "false"})
    load_main(args)
    testRunSql.cur.execute("SELECT COUNT(*) FROM data_file")
    file_count = testRunSql.cur.fetchone()[0]
    load_main(args)
    # same counts as test_met_db_table_counts
    assert_count_rows(testRunSql.cur, "line_data_tcmpr", 703)
    assert_count_rows(testRunSql.cur, "line_data_tcdiag", 168)
    assert_count_rows(testRunSql.cur, "data_file", file_count)

