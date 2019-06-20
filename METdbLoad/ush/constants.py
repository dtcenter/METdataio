#!/usr/bin/env python3
"""Define constants."""

# pylint:disable=no-member
# constants exist in constants.py

# name to use for a group when no group tag is included in load_spec
DEFAULT_DATABASE_GROUP = "NO GROUP"

# Maxiumum number of columns
MAX_COL = 120

COL_NUMS = [str(x) for x in range(MAX_COL - 24)]

MYSQL = "mysql"
MARIADB = "mariadb"
AURORA = "aurora"
RELATIONAL = [MYSQL, MARIADB, AURORA]

CB = "cb"

# default port for MySQL
SQL_PORT = 3306

# Lower Case true and false
LC_TRUE = "true"
LC_FALSE = "false"

# Not Available - NA
NOTAV = "NA"

# seperator for csv files
SEP = '$'

# No key is a number that would not be a valid key that is put in as a placeholder
NO_KEY = -1

# line types
FHO = "FHO"
CTC = "CTC"
CTS = "CTS"
MCTC = "MCTC"
MCTS = "MCTS"
CNT = "CNT"
SL1L2 = "SL1L2"
SAL1L2 = "SAL1L2"
VL1L2 = "VL1L2"
VAL1L2 = "VAL1L2"
PCT = "PCT"
PSTD = "PSTD"
PJC = "PJC"
PRC = "PRC"
ECLV = "ECLV"
MPR = "MPR"
NBRCTC = "NBRCTC"
NBRCTS = "NBRCTS"
NBRCNT = "NBRCNT"
ISC = "ISC"
RHIST = "RHIST"
PHIST = "PHIST"
ORANK = "ORANK"
SSVAR = "SSVAR"
GRAD = "GRAD"
VCNT = "VCNT"
RELP = "RELP"
ECNT = "ECNT"
ENSCNT = "ENSCNT"
PERC = "PERC"

UC_LINE_TYPES = [FHO, CTC, CTS, MCTC, MCTS, CNT, SL1L2, SAL1L2, VL1L2, VAL1L2,
                 PCT, PSTD, PJC, PRC, ECLV, MPR, NBRCTC, NBRCTS, NBRCNT, ISC,
                 RHIST, PHIST, ORANK, SSVAR, GRAD, VCNT, RELP, ECNT, ENSCNT, PERC]

LC_LINE_TYPES = [ltype.lower() for ltype in UC_LINE_TYPES]

LINE_TABLES = ['line_data_' + hname for hname in LC_LINE_TYPES]

ALPHA_LINE_TYPES = [CTS, CNT, PSTD, NBRCTS, NBRCNT, MCTS, SSVAR, VCNT]

COV_THRESH_LINE_TYPES = [NBRCTC, NBRCTS, PCT, PSTD, PJC, PRC]

VAR_LINE_TYPES = [PCT, PSTD, PJC, PRC, MCTC, RHIST, PHIST, RELP, ORANK, ECLV]

# column names
# MET column names are UC, SQL are LC
UC_DESC = "DESC"
UC_FCST_UNITS = "FCST_UNITS"

VERSION = "version"
MODEL = "model"
# MET file contains DESC. SQL field name is descr
DESCR = "descr"
FCST_LEAD = "fcst_lead"
FCST_VALID_BEG = "fcst_valid_beg"
FCST_VALID_END = "fcst_valid_end"
OBS_LEAD = "obs_lead"
OBS_VALID_BEG = "obs_valid_beg"
OBS_VALID_END = "obs_valid_end"
FCST_VAR = "fcst_var"
FCST_UNITS = "fcst_units"
FCST_LEV = "fcst_lev"
OBS_VAR = "obs_var"
OBS_UNITS = "obs_units"
OBS_LEV = "obs_lev"
OBTYPE = "obtype"
VX_MASK = "vx_mask"
INTERP_MTHD = "interp_mthd"
INTERP_PNTS = "interp_pnts"
FCST_THRESH = "fcst_thresh"
OBS_THRESH = "obs_thresh"
COV_THRESH = "cov_thresh"
ALPHA = "alpha"
LINE_TYPE = "line_type"
FCST_INIT_BEG = "fcst_init_beg"

FCST_LEAD_HR = "fcst_lead_hr"

# After units added in MET 8.1
LONG_HEADER = [VERSION, MODEL, DESCR, FCST_LEAD, FCST_VALID_BEG, FCST_VALID_END,
               OBS_LEAD, OBS_VALID_BEG, OBS_VALID_END, FCST_VAR, FCST_UNITS, FCST_LEV,
               OBS_VAR, OBS_UNITS, OBS_LEV, OBTYPE, VX_MASK, INTERP_MTHD, INTERP_PNTS,
               FCST_THRESH, OBS_THRESH, COV_THRESH, ALPHA, LINE_TYPE]

# Contains DESC but not UNITS
MID_HEADER = LONG_HEADER[0:10] + LONG_HEADER[11:13] + LONG_HEADER[14:]

# No DESC and no UNITS
SHORT_HEADER = MID_HEADER[0:2] + MID_HEADER[3:]

STAT_HEADER_KEYS = [VERSION, MODEL, DESCR, FCST_VAR, FCST_UNITS, FCST_LEV,
                    OBS_VAR, OBS_UNITS, OBS_LEV, OBTYPE, VX_MASK,
                    INTERP_MTHD, INTERP_PNTS, FCST_THRESH, OBS_THRESH]

Q_HEADER = "SELECT stat_header_id FROM stat_header WHERE " + \
           "=%s AND ".join(STAT_HEADER_KEYS) + "=%s"

STAT_HEADER = 'stat_header'
STAT_HEADER_ID = 'stat_header_id'
LINE_HEADER_ID = 'line_data_id'
DATA_FILE_ID = 'data_file_id'
LINE_NUM = 'line_num'
TOTAL_LC = 'total'

STAT_HEADER_FIELDS = [STAT_HEADER_ID, VERSION, MODEL, DESCR,
                      FCST_VAR, FCST_UNITS, FCST_LEV,
                      OBS_VAR, OBS_UNITS, OBS_LEV,
                      OBTYPE, VX_MASK, INTERP_MTHD, INTERP_PNTS,
                      FCST_THRESH, OBS_THRESH]

VALUE_SLOTS = '"%s", ' * len(STAT_HEADER_FIELDS)
VALUE_SLOTS = VALUE_SLOTS[:-2]

I_HEADER = "INSERT INTO stat_header (" + ",".join(STAT_HEADER_FIELDS) + \
           ") VALUES (" + VALUE_SLOTS + ")"

L_HEADER = "LOAD DATA LOCAL INFILE '{}' INTO TABLE {} FIELDS TERMINATED BY '{}';"

ALL_LINE_DATA_FIELDS = [STAT_HEADER_ID, DATA_FILE_ID, LINE_NUM,
                        FCST_LEAD, FCST_VALID_BEG, FCST_VALID_END, FCST_INIT_BEG,
                        OBS_LEAD, OBS_VALID_BEG, OBS_VALID_END]

TOT_LINE_DATA_FIELDS = ALL_LINE_DATA_FIELDS + [TOTAL_LC]

ALPH_LINE_DATA_FIELDS = ALL_LINE_DATA_FIELDS + [ALPHA, TOTAL_LC]

COV_LINE_DATA_FIELDS = ALL_LINE_DATA_FIELDS + [COV_THRESH, TOTAL_LC]

COVA_LINE_DATA_FIELDS = ALL_LINE_DATA_FIELDS + [COV_THRESH, ALPHA, TOTAL_LC]

ALL_COUNT = len(ALL_LINE_DATA_FIELDS)

LINE_DATA_FIELDS = dict()
LINE_DATA_COLS = dict()

# incomplete
LINE_DATA_FIELDS[CNT] = ALPH_LINE_DATA_FIELDS + \
                        ['fbar', 'fbar_ncl', 'fbar_ncu', 'fbar_bcl', 'fbar_bcu']

LINE_DATA_FIELDS[CTC] = TOT_LINE_DATA_FIELDS + \
                        ['fy_oy', 'fy_on', 'fn_oy', 'fn_on']

# incomplete
LINE_DATA_FIELDS[CTS] = ALPH_LINE_DATA_FIELDS + \
                        ['baser', 'baser_ncl', 'baser_ncu', 'baser_bcl', 'baser_bcu']

LINE_DATA_FIELDS[ECLV] = TOT_LINE_DATA_FIELDS + \
                         ['baser', 'value_baser', 'n_pnt']

# incomplete
LINE_DATA_FIELDS[ECNT] = TOT_LINE_DATA_FIELDS + \
                         ['n_ens', 'crps,crpss', 'ign,me', 'rmse,spread']

# incomplete
LINE_DATA_FIELDS[ENSCNT] = ALL_LINE_DATA_FIELDS + \
                           ['rpsf', 'rpsf_ncl', 'rpsf_ncu', 'rpsf_bcl', 'rpsf_bcu']

LINE_DATA_FIELDS[FHO] = TOT_LINE_DATA_FIELDS + \
                        ['f_rate', 'h_rate', 'o_rate']

LINE_DATA_FIELDS[GRAD] = TOT_LINE_DATA_FIELDS + \
                         ['fgbar', 'ogbar', 'mgbar', 'egbar', 's1', 's1_og', 'fgog_ratio',
                          'dx', 'dy']

LINE_DATA_FIELDS[ISC] = TOT_LINE_DATA_FIELDS + \
                        ['tile_dim', 'time_xll', 'tile_yll', 'nscale', 'iscale', 'mse,isc']

LINE_DATA_FIELDS[MCTC] = TOT_LINE_DATA_FIELDS + \
                         ['n_cat']

LINE_DATA_FIELDS[MCTS] = ALPH_LINE_DATA_FIELDS + \
                         ['n_cat', 'acc', 'acc_ncl', 'acc_ncu', 'acc_bcl', 'acc_bcu',
                          'hk', 'hk_bcl', 'hk_bcu', 'hss', 'hss_bcl', 'hss_bcu',
                          'ger', 'ger_bcl', 'ger_bcu']

# incomplete
LINE_DATA_FIELDS[MPR] = TOT_LINE_DATA_FIELDS + \
                         ['mp_index', 'obs_sid', 'obs_lat', 'obs_lon', 'obs_lvl', 'obs_elv']

# incomplete
LINE_DATA_FIELDS[NBRCNT] = ALPH_LINE_DATA_FIELDS + \
                           ['fbs', 'fbs_bcl', 'fbs_bcu']

LINE_DATA_FIELDS[NBRCTC] = COV_LINE_DATA_FIELDS + \
                           ['fy_oy', 'fy_on', 'fn_oy', 'fn_on']

# incomplete
LINE_DATA_FIELDS[NBRCTS] = COVA_LINE_DATA_FIELDS + \
                           ['baser', 'baser_ncl', 'baser_ncu']

# incomplete
LINE_DATA_FIELDS[ORANK] = COVA_LINE_DATA_FIELDS + \
                          ['orank_index', 'obs_sid']

LINE_DATA_FIELDS[PCT] = COV_LINE_DATA_FIELDS + \
                        ['n_thresh', 'pct_thresh']

LINE_DATA_FIELDS[PERC] = ALL_LINE_DATA_FIELDS + \
                         ['fcst_perc', 'obs_perc']

LINE_DATA_FIELDS[PHIST] = TOT_LINE_DATA_FIELDS + \
                          ['bin_size', 'n_bin']

LINE_DATA_FIELDS[PJC] = COV_LINE_DATA_FIELDS + \
                        ['n_thresh', 'pjc_thresh']

LINE_DATA_FIELDS[PRC] = COV_LINE_DATA_FIELDS + \
                        ['n_thresh', 'prc_thresh']

LINE_DATA_FIELDS[PSTD] = COVA_LINE_DATA_FIELDS + \
                         ['n_thresh', 'baser', 'baser_ncl', 'baser_ncu',
                          'reliability', 'resolution', 'uncertainty', 'roc_auc',
                          'brier', 'brier_ncl', 'brier_ncu', 'briercl', 'briercl_ncl',
                          'briercl_ncu', 'bss', 'bss_smpl']

LINE_DATA_FIELDS[RELP] = TOT_LINE_DATA_FIELDS + \
                         ['n_ens']

LINE_DATA_FIELDS[RHIST] = TOT_LINE_DATA_FIELDS + \
                          ['n_rank']

LINE_DATA_FIELDS[SL1L2] = TOT_LINE_DATA_FIELDS + \
                          ['fbar', 'obar', 'fobar', 'ffbar', 'oobar', 'mae']

LINE_DATA_FIELDS[SAL1L2] = TOT_LINE_DATA_FIELDS + \
                           ['fabar', 'oabar', 'foabar', 'ffabar', 'ooabar', 'mae']

# incomplete
LINE_DATA_FIELDS[SSVAR] = ALPH_LINE_DATA_FIELDS + \
                          ['n_bin', 'bin_i', 'bin_n']

LINE_DATA_FIELDS[VL1L2] = TOT_LINE_DATA_FIELDS + \
                          ['ufbar', 'vfbar', 'uobar', 'vobar', 'uvfobar', 'uvffbar',
                           'uvoobar', 'f_speed_bar', 'o_speed_bar']

LINE_DATA_FIELDS[VAL1L2] = TOT_LINE_DATA_FIELDS + \
                           ['ufabar', 'vfabar', 'uoabar', 'voabar', 'uvfoabar', 'uvffabar',
                            'uvooabar']

# incomplete
LINE_DATA_FIELDS[VCNT] = ALPH_LINE_DATA_FIELDS + \
                         ['fbar', 'fbar_bcl', 'fbar_bcu']

for line_type in UC_LINE_TYPES:
    LINE_DATA_COLS[line_type] = ALL_LINE_DATA_FIELDS + \
                                COL_NUMS[0:(len(LINE_DATA_FIELDS[line_type]) - ALL_COUNT)]
