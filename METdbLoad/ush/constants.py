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

# separator for csv files
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

Q_FILE = "SELECT data_file_id FROM data_file WHERE " + \
           "path=%s AND filename=%s"

Q_HEADER = "SELECT stat_header_id FROM stat_header WHERE " + \
           "=%s AND ".join(STAT_HEADER_KEYS) + "=%s"

Q_METADATA = "SELECT category, description FROM metadata"

STAT_HEADER = 'stat_header'
STAT_HEADER_ID = 'stat_header_id'
LINE_HEADER_ID = 'line_data_id'
LINE_NUM = 'line_num'
TOTAL_LC = 'total'

DATA_FILE = 'data_file'
FULL_FILE = 'full_file'
DATA_FILE_ID = 'data_file_id'
DATA_FILE_LU_ID = 'data_file_lu_id'
FILE_ROW = 'file_row'
FILENAME = 'filename'
FILEPATH = 'path'
LOAD_DATE = 'load_date'
MOD_DATE = 'mod_date'

INSTANCE_INFO = 'instance_info'
INSTANCE_INFO_ID = 'instance_info_id'

DATA_FILE_FIELDS = [DATA_FILE_ID, DATA_FILE_LU_ID, FILENAME, FILEPATH,
                    LOAD_DATE, MOD_DATE]

STAT_HEADER_FIELDS = [STAT_HEADER_ID, VERSION, MODEL, DESCR,
                      FCST_VAR, FCST_UNITS, FCST_LEV,
                      OBS_VAR, OBS_UNITS, OBS_LEV,
                      OBTYPE, VX_MASK, INTERP_MTHD, INTERP_PNTS,
                      FCST_THRESH, OBS_THRESH]

VALUE_SLOTS = '%s, ' * len(STAT_HEADER_FIELDS)
VALUE_SLOTS = VALUE_SLOTS[:-2]

I_HEADER = "INSERT INTO stat_header (" + ",".join(STAT_HEADER_FIELDS) + \
           ") VALUES (" + VALUE_SLOTS + ")"

I_DATA_FILES = "INSERT INTO data_file (" + ",".join(DATA_FILE_FIELDS) + \
               ") VALUES (%s, %s, %s, %s, %s, %s)"

I_METADATA = "INSERT INTO metadata (category, description) VALUES (%s, %s)"

I_INSTANCE = "INSERT INTO instance_info (instance_info_id, updater, update_date, " + \
             "update_detail, load_xml) VALUES (%s, %s, %s, %s, %s)"

U_METADATA = "UPDATE metadata SET category=%s, description=%s"

LD_TABLE = "LOAD DATA LOCAL INFILE '{}' INTO TABLE {} FIELDS TERMINATED BY '{}';"

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
LINE_DATA_Q = dict()

LINE_DATA_FIELDS[CNT] = ALPH_LINE_DATA_FIELDS + \
                        ['fbar', 'fbar_ncl', 'fbar_ncu', 'fbar_bcl', 'fbar_bcu',
                         'fstdev', 'fstdev_ncl', 'fstdev_ncu', 'fstdev_bcl', 'fstdev_bcu',
                         'obar', 'obar_ncl', 'obar_ncu', 'obar_bcl', 'obar_bcu',
                         'ostdev', 'ostdev_ncl', 'ostdev_ncu', 'ostdev_bcl', 'ostdev_bcu',
                         'pr_corr', 'pr_corr_ncl', 'pr_corr_ncu', 'pr_corr_bcl', 'pr_corr_bcu',
                         'sp_corr', 'dt_corr', 'ranks', 'frank_ties', 'orank_ties',
                         'me', 'me_ncl', 'me_ncu', 'me_bcl', 'me_bcu',
                         'estdev', 'estdev_ncl', 'estdev_ncu', 'estdev_bcl', 'estdev_bcu',
                         'mbias', 'mbias_bcl', 'mbias_bcu', 'mae', 'mae_bcl', 'mae_bcu',
                         'mse', 'mse_bcl', 'mse_bcu', 'bcmse', 'bcmse_bcl', 'bcmse_bcu',
                         'rmse', 'rmse_bcl', 'rmse_bcu', 'e10', 'e10_bcl', 'e10_bcu',
                         'e25', 'e25_bcl', 'e25_bcu', 'e50', 'e50_bcl', 'e50_bcu',
                         'e75', 'e75_bcl', 'e75_bcu', 'e90', 'e90_bcl', 'e90_bcu',
                         'iqr', 'iqr_bcl', 'iqr_bcu', 'mad', 'mad_bcl', 'mad_bcu',
                         'anom_corr', 'anom_corr_ncl', 'anom_corr_ncu',
                         'anom_corr_bcl', 'anom_corr_bcu',
                         'me2', 'me2_bcl', 'me2_bcu', 'msess', 'msess_bcl', 'msess_bcu',
                         'rmsfa', 'rmsfa_bcl', 'rmsfa_bcu', 'rmsoa', 'rmsoa_bcl', 'rmsoa_bcu']

LINE_DATA_FIELDS[CTC] = TOT_LINE_DATA_FIELDS + \
                        ['fy_oy', 'fy_on', 'fn_oy', 'fn_on']

LINE_DATA_FIELDS[CTS] = ALPH_LINE_DATA_FIELDS + \
                        ['baser', 'baser_ncl', 'baser_ncu', 'baser_bcl', 'baser_bcu',
                         'fmean', 'fmean_ncl', 'fmean_ncu', 'fmean_bcl', 'fmean_bcu',
                         'acc', 'acc_ncl', 'acc_ncu', 'acc_bcl', 'acc_bcu',
                         'fbias', 'fbias_bcl', 'fbias_bcu',
                         'pody', 'pody_ncl', 'pody_ncu', 'pody_bcl', 'pody_bcu',
                         'podn', 'podn_ncl', 'podn_ncu', 'podn_bcl', 'podn_bcu',
                         'pofd', 'pofd_ncl', 'pofd_ncu', 'pofd_bcl', 'pofd_bcu',
                         'far', 'far_ncl', 'far_ncu', 'far_bcl', 'far_bcu',
                         'csi', 'csi_ncl', 'csi_ncu', 'csi_bcl', 'csi_bcu',
                         'gss', 'gss_bcl', 'gss_bcu',
                         'hk', 'hk_ncl', 'hk_ncu', 'hk_bcl', 'hk_bcu',
                         'hss', 'hss_bcl', 'hss_bcu',
                         'odds', 'odds_ncl', 'odds_ncu', 'odds_bcl', 'odds_bcu',
                         'lodds', 'lodds_ncl', 'lodds_ncu', 'lodds_bcl', 'lodds_bcu',
                         'orss', 'orss_ncl', 'orss_ncu', 'orss_bcl', 'orss_bcu',
                         'eds', 'eds_ncl', 'eds_ncu', 'eds_bcl', 'eds_bcu',
                         'seds', 'seds_ncl', 'seds_ncu', 'seds_bcl', 'seds_bcu',
                         'edi', 'edi_ncl', 'edi_ncu', 'edi_bcl', 'edi_bcu',
                         'sedi', 'sedi_ncl', 'sedi_ncu', 'sedi_bcl', 'sedi_bcu',
                         'bagss', 'bagss_bcl', 'bagss_bcu']

LINE_DATA_FIELDS[ECLV] = TOT_LINE_DATA_FIELDS + \
                         ['baser', 'value_baser', 'n_pnt']

LINE_DATA_FIELDS[ECNT] = TOT_LINE_DATA_FIELDS + \
                         ['n_ens', 'crps', 'crpss', 'ign', 'me', 'rmse', 'spread',
                          'me_oerr', 'rmse_oerr', 'spread_oerr', 'spread_plus_oerr']

LINE_DATA_FIELDS[ENSCNT] = ALL_LINE_DATA_FIELDS + \
                           ['rpsf', 'rpsf_ncl', 'rpsf_ncu', 'rpsf_bcl', 'rpsf_bcu',
                            'rpscl', 'rpscl_ncl', 'rpscl_ncu', 'rpscl_bcl', 'rpscl_bcu',
                            'rpss', 'rpss_ncl', 'rpss_ncu', 'rpss_bcl', 'rpss_bcu',
                            'crpsf', 'crpsf_ncl', 'crpsf_ncu', 'crpsf_bcl', 'crpsf_bcu',
                            'crpscl', 'crpscl_ncl', 'crpscl_ncu', 'crpscl_bcl', 'crpscl_bcu',
                            'crpss', 'crpss_ncl', 'crpss_ncu', 'crpss_bcl', 'crpss_bcu']

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

LINE_DATA_FIELDS[MPR] = TOT_LINE_DATA_FIELDS + \
                         ['mp_index', 'obs_sid', 'obs_lat', 'obs_lon', 'obs_lvl', 'obs_elv',
                          'mpr_fcst', 'mpr_obs', 'mpr_climo', 'obs_qc',
                          'climo_mean', 'climo_stdev', 'climo_cdf']

LINE_DATA_FIELDS[NBRCNT] = ALPH_LINE_DATA_FIELDS + \
                           ['fbs', 'fbs_bcl', 'fbs_bcu', 'fss', 'fss_bcl', 'fss_bcu',
                            'afss', 'afss_bcl', 'afss_bcu', 'ufss', 'ufss_bcl', 'ufss_bcu',
                            'f_rate', 'f_rate_bcl', 'f_rate_bcu',
                            'o_rate', 'o_rate_bcl', 'o_rate_bcu']

LINE_DATA_FIELDS[NBRCTC] = COV_LINE_DATA_FIELDS + \
                           ['fy_oy', 'fy_on', 'fn_oy', 'fn_on']

# incomplete
LINE_DATA_FIELDS[NBRCTS] = COVA_LINE_DATA_FIELDS + \
                           ['baser', 'baser_ncl', 'baser_ncu']

# incomplete
LINE_DATA_FIELDS[ORANK] = TOT_LINE_DATA_FIELDS + \
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
    # for each line type, create a list of the columns in the dataframe
    if line_type in COV_THRESH_LINE_TYPES:
        LINE_DATA_COLS[line_type] = COV_LINE_DATA_FIELDS[0:-1] + \
                                    COL_NUMS[0:(len(LINE_DATA_FIELDS[line_type]) - (ALL_COUNT + 1))]
    elif line_type in ALPHA_LINE_TYPES:
        LINE_DATA_COLS[line_type] = ALPH_LINE_DATA_FIELDS[0:-1] + \
                                    COL_NUMS[0:(len(LINE_DATA_FIELDS[line_type]) - (ALL_COUNT + 1))]
    else:
        LINE_DATA_COLS[line_type] = ALL_LINE_DATA_FIELDS + \
                                    COL_NUMS[0:(len(LINE_DATA_FIELDS[line_type]) - ALL_COUNT)]
    # maybe this could be done more elegantly/automatically
    # if this works, have to handle alpha cov as well

    # For each line type, create insert queries
    VALUE_SLOTS = '%s, ' * len(LINE_DATA_FIELDS[line_type])
    VALUE_SLOTS = VALUE_SLOTS[:-2]

    line_table = LINE_TABLES[UC_LINE_TYPES.index(line_type)]

    i_line = "INSERT INTO " + line_table + " (" + ",".join(LINE_DATA_FIELDS[line_type]) + \
               ") VALUES (" + VALUE_SLOTS + ")"
    LINE_DATA_Q[line_type] = i_line

# From the data_file_lu table - to lookup file type
POINT_STAT = 0
GRID_STAT = 1
MODE_CTS = 2
MODE_OBJ = 3
WAVELET_STAT = 4
ENSEMBLE_STAT = 5
VSDB_POINT_STAT = 6
STAT = 7
MTD_2D = 8
MTD_3D_PC = 9
MTD_3D_PS = 10
MTD_3D_SC = 11
MTD_3D_SS = 12
