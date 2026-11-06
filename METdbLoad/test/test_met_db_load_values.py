"""Check specific values written to the database by METdbLoad.

The row count tests in test_met_db_load.py do not catch changes to the values
that are loaded. These tests load a data set, then compare a few rows to values
taken from the input files in METDATAIO_TEST_INPUT_DIR.
"""
import os

import pytest

from METdbLoad.ush.met_db_load import main as load_main
from METdbLoad.test.utils import (
    dict_to_args,
    get_xml_test_file,
    input_path,
    POINT_STAT_DATA_DIR,
    VSDB_DATA_DIR,
)

RRFS_DATA_DIR = input_path("RRFS_PCT_PRC_PJC_PSTD")

# METviewer value for Not Available
MV_NOTAV = -9999

POINT_STAT_FILE = (
    "point_stat_FV3_GFS_v15p2_CONUS_25km_NDAS_ADPSFC_010000L_20190615_010000V.stat"
)
VSDB_FILE = "gfs_20140802.vsdb"


def load_data(tmp_path, met_data_dir, met_tool, local_infile=True):
    test_args = dict_to_args(
        {
            "xmlfile": str(
                get_xml_test_file(
                    tmp_path, met_data_dir, met_tool, local_infile=local_infile
                )
            ),
            "index": "true",
            "tmpdir": [str(tmp_path)],
            "loglevel": None,
        }
    )
    load_main(test_args)


def fetch_row(cur, table, filename, line_num):
    """Return the row of a line_data table joined with its stat_header as a dict"""
    cur.execute(
        f"SELECT h.*, l.* FROM {table} l"
        " JOIN stat_header h USING (stat_header_id)"
        " JOIN data_file d USING (data_file_id)"
        " WHERE d.filename = %s AND l.line_num = %s",
        (filename, line_num),
    )
    rows = cur.fetchall()
    assert len(rows) == 1, f"Expected 1 row in {table} for line {line_num}, got {len(rows)}"
    columns = [desc[0] for desc in cur.description]
    return dict(zip(columns, rows[0]))


@pytest.mark.parametrize("local_infile", ["true", "false"])
def test_point_stat_values(emptyDB, testRunSql, tmp_path, local_infile):
    load_data(tmp_path, POINT_STAT_DATA_DIR, "point_stat", local_infile)

    # TMP Z2 CNT line: point_stat/<POINT_STAT_FILE> line 6
    row = fetch_row(testRunSql.cur, "line_data_cnt", POINT_STAT_FILE, 6)
    assert row["fcst_var"] == "TMP"
    assert row["fcst_lev"] == "Z2"
    assert row["total"] == 4028
    assert row["fbar"] == 293.57329
    assert row["fbar_ncl"] == 293.4106
    assert row["me"] == -2.1377
    assert row["si"] == 0.010996
    # NA in the input file is stored as -9999
    assert row["fbar_bcl"] == MV_NOTAV
    assert row["sp_corr"] == MV_NOTAV
    assert row["anom_corr"] == MV_NOTAV
    # NA in header columns stays NA
    assert row["fcst_thresh"] == "NA"
    assert row["obs_units"] == "NA"

    # UGRD_VGRD Z10 VL1L2 line: point_stat/<POINT_STAT_FILE> line 52
    row = fetch_row(testRunSql.cur, "line_data_vl1l2", POINT_STAT_FILE, 52)
    assert row["total"] == 3962
    assert row["ufbar"] == 0.36901
    assert row["uvoobar"] == 19.04361
    assert row["total_dir"] == 4474


def test_vsdb_values(emptyDB, testRunSql, tmp_path):
    load_data(tmp_path, VSDB_DATA_DIR, "vsdb")

    # FHO line converted to CTC: vsdb/<VSDB_FILE> line 4
    # V01 GFS 45 2014080221 STAGE2 G218/SEC FHO>.50 APCP/03 SFC = 3524 0.03490 0.00028 0.00738
    row = fetch_row(testRunSql.cur, "line_data_ctc", VSDB_FILE, 4)
    # threshold is split out of the line type and copied to obs_thresh
    assert row["fcst_thresh"] == ">.50"
    assert row["obs_thresh"] == ">.50"
    # header fields filled in by the VSDB conversion
    assert row["descr"] == "NA"
    assert row["fcst_var"] == "APCP/03"
    assert row["obs_var"] == "APCP/03"
    assert row["obs_lev"] == "SFC"
    assert row["fcst_units"] == "NA"
    assert row["interp_mthd"] == "NA"
    assert row["interp_pnts"] == 0
    assert row["obtype"] == "STAGE2"
    assert row["vx_mask"] == "G218/SEC"
    # counts computed from total and f_rate, h_rate, o_rate
    assert row["total"] == 3524
    assert row["fy_oy"] == 1  # round(h_rate * total)
    assert row["fy_on"] == 122  # round((f_rate - h_rate) * total)
    assert row["fn_oy"] == 25  # round((o_rate - h_rate) * total)
    assert row["fn_on"] == 3376  # total - the other three

    # SL1L2 line: vsdb/<VSDB_FILE> line 20
    # V01 GFS 45 2014080221 STAGE2 G218/NWC SL1L2 APCP/03 SFC= 2556 0.2401E-01 0.6913E-01 0.0000E+00 0.4112E-01 0.1017E+00
    row = fetch_row(testRunSql.cur, "line_data_sl1l2", VSDB_FILE, 20)
    assert row["fcst_thresh"] == "NA"
    assert row["obs_var"] == "APCP/03"
    assert row["total"] == 2556
    assert row["fbar"] == 0.02401
    assert row["obar"] == 0.06913
    assert row["fobar"] == 0.0
    assert row["ffbar"] == 0.04112
    assert row["oobar"] == 0.1017
    # MAE is not in the VSDB line
    assert row["mae"] == MV_NOTAV


# Header line of a MET .stat file
STAT_HEADER = (
    "VERSION MODEL DESC FCST_LEAD FCST_VALID_BEG FCST_VALID_END OBS_LEAD OBS_VALID_BEG"
    " OBS_VALID_END FCST_VAR FCST_UNITS FCST_LEV OBS_VAR OBS_UNITS OBS_LEV OBTYPE VX_MASK"
    " INTERP_MTHD INTERP_PNTS FCST_THRESH OBS_THRESH COV_THRESH ALPHA LINE_TYPE\n"
)
# Start of a .stat data line, up to and including INTERP_PNTS
STAT_PREFIX = (
    "V10.1.1 GFS NA 010000 20190615_010000 20190615_010000 000000 20190615_003001"
    " 20190615_013000 TMP K Z2 TMP K Z2 ADPSFC CONUS BILIN 4 "
)
# Header line of a MET 4 .stat file, which has no DESC or units columns
OLD_STAT_HEADER = (
    "VERSION MODEL FCST_LEAD FCST_VALID_BEG FCST_VALID_END OBS_LEAD OBS_VALID_BEG"
    " OBS_VALID_END FCST_VAR FCST_LEV OBS_VAR OBS_LEV OBTYPE VX_MASK INTERP_MTHD"
    " INTERP_PNTS FCST_THRESH OBS_THRESH COV_THRESH ALPHA LINE_TYPE\n"
)

RRFS_FILE = (
    "point_stat_RRFS_GEFS_GF.SPP.SPPT_prob_ADPSFC_NDAS_110000L_20220506_110000V.stat"
)


def write_data_file(tmp_path, filename, contents):
    """Write a data file to a new directory under tmp_path and return the directory"""
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    (data_dir / filename).write_text(contents)
    return str(data_dir)


def fetch_dicts(cur, query, params=()):
    """Run a query and return the rows as a list of dicts"""
    cur.execute(query, params)
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def test_percentile_thresholds(emptyDB, testRunSql, tmp_path):
    """A percentile value in parens in the thresholds is split out into line_data_perc"""
    data_dir = write_data_file(
        tmp_path,
        "perc.stat",
        STAT_HEADER
        + STAT_PREFIX + ">=SFP50(291.81) >=SOP50(293.05) NA NA CTC 100 10 20 30 40\n"
        + STAT_PREFIX + ">273 >273 NA NA CTC 100 1 2 3 94\n",
    )
    load_data(tmp_path, data_dir, "point_stat")

    row = fetch_row(testRunSql.cur, "line_data_ctc", "perc.stat", 2)
    # the percentile value is removed from the thresholds in the header
    assert row["fcst_thresh"] == ">=SFP50"
    assert row["obs_thresh"] == ">=SOP50"
    assert (row["fy_oy"], row["fy_on"], row["fn_oy"], row["fn_on"]) == (10, 20, 30, 40)

    # only the line with a percentile threshold has a line_data_perc row
    perc = fetch_dicts(testRunSql.cur, "SELECT * FROM line_data_perc")
    assert len(perc) == 1
    assert perc[0]["line_num"] == 2
    assert perc[0]["stat_header_id"] == row["stat_header_id"]
    assert perc[0]["fcst_perc"] == 291.81
    assert perc[0]["obs_perc"] == 293.05


def test_old_rhist_creates_ecnt(emptyDB, testRunSql, tmp_path):
    """RHIST lines from MET 4 contain ECNT statistics, which are written to line_data_ecnt"""
    # MET 4 RHIST columns: TOTAL CRPS IGN N_RANK RANK_1 ... RANK_N
    data_dir = write_data_file(
        tmp_path,
        "rhist_v4.stat",
        OLD_STAT_HEADER
        + "V4.2 GFS 010000 20190615_010000 20190615_010000 000000 20190615_003001"
        " 20190615_013000 TMP Z2 TMP Z2 ADPSFC CONUS BILIN 4 NA NA NA NA RHIST"
        " 100 0.5 1.25 3 20 30 50\n",
    )
    load_data(tmp_path, data_dir, "ensemble_stat")

    rhist = fetch_row(testRunSql.cur, "line_data_rhist", "rhist_v4.stat", 2)
    # units and description are added for MET 4 files
    assert rhist["descr"] == "NA"
    assert rhist["fcst_units"] == "NA"
    assert rhist["total"] == 100
    assert rhist["n_rank"] == 3
    ranks = fetch_dicts(
        testRunSql.cur,
        "SELECT i_value, rank_i FROM line_data_rhist_rank WHERE line_data_id = %s"
        " ORDER BY i_value",
        (rhist["line_data_id"],),
    )
    assert [(r["i_value"], r["rank_i"]) for r in ranks] == [(1, 20), (2, 30), (3, 50)]

    ecnt = fetch_row(testRunSql.cur, "line_data_ecnt", "rhist_v4.stat", 2)
    assert ecnt["stat_header_id"] == rhist["stat_header_id"]
    assert ecnt["total"] == 100
    assert ecnt["n_ens"] == 3
    assert ecnt["crps"] == 0.5
    assert ecnt["ign"] == 1.25


def load_rrfs_file(tmp_path):
    """Load one file of PCT, PJC, PRC and PSTD lines"""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / RRFS_FILE).symlink_to(
        os.path.join(RRFS_DATA_DIR, "2022050600", RRFS_FILE)
    )
    load_data(tmp_path, str(data_dir), "point_stat")


def test_pstd_pjc_values(emptyDB, testRunSql, tmp_path):
    """Variable length PSTD and PJC lines are split into line data and threshold rows"""
    load_rrfs_file(tmp_path)

    for table in ("line_data_pct", "line_data_pjc", "line_data_prc", "line_data_pstd"):
        assert len(fetch_dicts(testRunSql.cur, f"SELECT * FROM {table}")) == 15

    # PSTD line: RRFS_PCT_PRC_PJC_PSTD/2022050600/<RRFS_FILE> line 3
    # ... PSTD 4390 11 1 0.99913 1 0.0025 0 0 NA 0.0025 -0.04876 0.05376 NA NA NA NA NA
    #     0 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1
    pstd = fetch_row(testRunSql.cur, "line_data_pstd", RRFS_FILE, 3)
    assert pstd["fcst_thresh"] == "==0.10000"
    assert pstd["obs_thresh"] == ">=268"
    assert pstd["total"] == 4390
    assert pstd["n_thresh"] == 11
    assert pstd["baser"] == 1
    assert pstd["baser_ncl"] == 0.99913
    assert pstd["reliability"] == 0.0025
    assert pstd["roc_auc"] == MV_NOTAV
    assert pstd["brier"] == 0.0025
    assert pstd["brier_ncl"] == -0.04876
    assert pstd["bss"] == MV_NOTAV

    # PJC line: RRFS_PCT_PRC_PJC_PSTD/2022050600/<RRFS_FILE> line 4
    # PJC has n_thresh - 1 sets of values: thresh_i oy_tp_i on_tp_i calibration_i
    # refinement_i likelihood_i baser_i. The last set is 0.9 1 0 1 1 1 1
    pjc = fetch_row(testRunSql.cur, "line_data_pjc", RRFS_FILE, 4)
    assert pjc["n_thresh"] == 11
    thresh = fetch_dicts(
        testRunSql.cur,
        "SELECT * FROM line_data_pjc_thresh WHERE line_data_id = %s ORDER BY i_value",
        (pjc["line_data_id"],),
    )
    assert len(thresh) == 10
    assert thresh[0]["thresh_i"] == 0
    assert thresh[0]["calibration_i"] == MV_NOTAV
    last = thresh[-1]
    assert (last["thresh_i"], last["oy_tp_i"], last["on_tp_i"], last["calibration_i"],
            last["refinement_i"], last["likelihood_i"], last["baser_i"]) == (0.9, 1, 0, 1, 1, 1, 1)


def test_pstd_thresholds(emptyDB, testRunSql, tmp_path):
    """The thresholds at the end of a PSTD line are written to line_data_pstd_thresh"""
    load_rrfs_file(tmp_path)

    # PSTD line: RRFS_PCT_PRC_PJC_PSTD/2022050600/<RRFS_FILE> line 3
    # thresholds are the last 11 values: 0 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1
    pstd = fetch_row(testRunSql.cur, "line_data_pstd", RRFS_FILE, 3)
    thresh = fetch_dicts(
        testRunSql.cur,
        "SELECT i_value, thresh_i FROM line_data_pstd_thresh WHERE line_data_id = %s"
        " ORDER BY i_value",
        (pstd["line_data_id"],),
    )
    assert [r["i_value"] for r in thresh] == list(range(1, 12))
    assert [r["thresh_i"] for r in thresh] == pytest.approx([i / 10 for i in range(11)])


def test_variable_length_record_too_long(emptyDB, testRunSql, tmp_path):
    """A variable length line with more repeating values than fit in a row is not loaded"""
    # PCT: TOTAL N_THRESH then THRESH_i OY_i ON_i for each threshold, plus the last THRESH
    data_dir = write_data_file(
        tmp_path,
        "pct.stat",
        STAT_HEADER
        + STAT_PREFIX + "==0.5 >273 NA NA PCT 100 3 0 10 40 0.5 30 20 1\n"
        + STAT_PREFIX + "==0.5 >273 NA NA PCT 100 500 0 10 40 0.5 30 20 1\n",
    )
    load_data(tmp_path, data_dir, "point_stat")

    pct = fetch_dicts(testRunSql.cur, "SELECT * FROM line_data_pct")
    assert [r["line_num"] for r in pct] == [2]
    thresh = fetch_dicts(
        testRunSql.cur, "SELECT * FROM line_data_pct_thresh ORDER BY i_value"
    )
    assert [(r["thresh_i"], r["oy_i"], r["on_i"]) for r in thresh] == [(0, 10, 40), (0.5, 30, 20)]


def test_variable_length_record_too_long_second_load(emptyDB, testRunSql, tmp_path):
    """A variable length line that is too long is not loaded when the table already has rows"""
    data_dir = write_data_file(
        tmp_path,
        "pct.stat",
        STAT_HEADER + STAT_PREFIX + "==0.5 >273 NA NA PCT 100 3 0 10 40 0.5 30 20 1\n",
    )
    load_data(tmp_path, data_dir, "point_stat")

    (tmp_path / "data" / "pct.stat").unlink()
    write_data_file(
        tmp_path,
        "pct_long.stat",
        STAT_HEADER
        + STAT_PREFIX + "==0.5 >273 NA NA PCT 100 500 0 10 40 0.5 30 20 1\n"
        + STAT_PREFIX + "==0.5 >273 NA NA PCT 100 3 0 11 41 0.5 31 21 1\n",
    )
    load_data(tmp_path, data_dir, "point_stat")

    pct = fetch_dicts(
        testRunSql.cur,
        "SELECT d.filename, l.line_num, l.line_data_id FROM line_data_pct l"
        " JOIN data_file d USING (data_file_id) ORDER BY l.line_data_id",
    )
    assert [(r["filename"], r["line_num"]) for r in pct] == [
        ("pct.stat", 2), ("pct_long.stat", 3)
    ]
    thresh = fetch_dicts(
        testRunSql.cur, "SELECT * FROM line_data_pct_thresh ORDER BY line_data_id, i_value"
    )
    assert [(r["line_data_id"], r["thresh_i"], r["oy_i"], r["on_i"]) for r in thresh] == [
        (pct[0]["line_data_id"], 0, 10, 40), (pct[0]["line_data_id"], 0.5, 30, 20),
        (pct[1]["line_data_id"], 0, 11, 41), (pct[1]["line_data_id"], 0.5, 31, 21),
    ]


# Header line of a MET 6 and 7 .stat file, which has DESC but no units columns
V6_STAT_HEADER = (
    "VERSION MODEL DESC FCST_LEAD FCST_VALID_BEG FCST_VALID_END OBS_LEAD OBS_VALID_BEG"
    " OBS_VALID_END FCST_VAR FCST_LEV OBS_VAR OBS_LEV OBTYPE VX_MASK INTERP_MTHD"
    " INTERP_PNTS FCST_THRESH OBS_THRESH COV_THRESH ALPHA LINE_TYPE\n"
)


@pytest.mark.parametrize(
    "header, version, n_stats",
    [
        (OLD_STAT_HEADER, "V4.2 GFS", 10),
        (V6_STAT_HEADER, "V6.0 GFS NA", 14),
        (V6_STAT_HEADER, "V7.0 GFS NA", 15),
        (STAT_HEADER, "V10.1.1 GFS NA", 15),
    ],
    ids=["V4.2", "V6.0", "V7.0", "V10.1.1"],
)
def test_pstd_thresholds_by_version(emptyDB, testRunSql, tmp_path, header, version, n_stats):
    """The number of statistics before the PSTD thresholds depends on the MET version"""
    units = "K " if header == STAT_HEADER else ""
    # BASER is 0.4, BRIER is 0.2, the other statistics are NA
    stats = ["0.4"] + ["NA"] * 6 + ["0.2"] + ["NA"] * (n_stats - 8)
    data_dir = write_data_file(
        tmp_path,
        "pstd.stat",
        header
        + f"{version} 010000 20190615_010000 20190615_010000 000000 20190615_003001"
        f" 20190615_013000 TMP {units}Z2 TMP {units}Z2 ADPSFC CONUS BILIN 4"
        f" ==0.5 >273 NA 0.05 PSTD 100 3 {' '.join(stats)} 0 0.5 1\n",
    )
    load_data(tmp_path, data_dir, "point_stat")

    pstd = fetch_row(testRunSql.cur, "line_data_pstd", "pstd.stat", 2)
    assert pstd["n_thresh"] == 3
    assert pstd["baser"] == 0.4
    assert pstd["brier"] == 0.2
    # columns after the statistics in the file do not get threshold values
    assert pstd["bss_smpl"] == MV_NOTAV
    thresh = fetch_dicts(
        testRunSql.cur,
        "SELECT i_value, thresh_i FROM line_data_pstd_thresh WHERE line_data_id = %s"
        " ORDER BY i_value",
        (pstd["line_data_id"],),
    )
    assert [(r["i_value"], r["thresh_i"]) for r in thresh] == [(1, 0), (2, 0.5), (3, 1)]
