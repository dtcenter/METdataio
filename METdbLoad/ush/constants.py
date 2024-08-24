#!/usr/bin/env python3
"""Define constants."""

# pylint:disable=no-member
# constants exist in constants.py

from collections import OrderedDict
import numpy as np

# name to use for a group when no group tag is included in load_spec
DEFAULT_DATABASE_GROUP = "NO GROUP"

# Maximum number of columns
MAX_COL = 125

# Maximum number of files to load at a time
# Goal is to not max out memory
MAX_FILES = 100

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
NOTAV = 'NA'

# METviewer value for Not Available
MV_NOTAV = '-9999'

# Used for null fields in mode_header - needed by LOAD DATA INFILE
MV_NULL = '\\N'

# separator for csv files
SEP = '$'

# Equal Sign
EQS = '='

# Forward Slash
FWD_SLASH = '/'

# Underscore
U_SCORE = '_'

# Left paren for searching
L_PAREN = '('

# Right paren for searching
R_PAREN = ')'

# Triple zero for tests for MODE files
T_ZERO = '000'

# Characters expected in dates
DATE_CHARS = set('yYmMdDhHsSz')

# Substitutions, Java date format to Python
DATE_SUBS = OrderedDict([('yyyy', '%Y'),
                         ('yy', '%y'),
                         ('MM', '%m'),
                         ('dd', '%d'),
                         ('hh', '%I'),
                         ('HH', '%H'),
                         ('mm', '%M'),
                         ('SSS', '%f'),
                         ('ss', '%S'),
                         ('z', '%z'),
                         ('D', '%j')])

# Generic count of variable fields
N_VAR = 'n_var'

# No key is a number that would not be a valid key that is put in as a placeholder
NO_KEY = -1

# names for columns that contain not available and zero values
COL_NA = 'colna'
COL_ZERO = 'zeroes'

# repeat COL_NA for building VSDB dataframe
COL_NAS = [COL_NA] * 100

# STAT line types - comments from the v8.1.1 MET user's guide
FHO = "FHO"  # Forecast, Hit, Observation Rates
CTC = "CTC"  # Contingency Table Counts
CTS = "CTS"  # Contingency Table Statistics
MCTC = "MCTC"  # Multi-category Contingency Table Counts
MCTS = "MCTS"  # Multi-category Contingency Table Statistics
CNT = "CNT"  # Continuous Statistics
SL1L2 = "SL1L2"  # Scalar L1L2 Partial Sums
SAL1L2 = "SAL1L2"  # Scalar Anomaly L1L2 Partial Sums when climatological data is
# supplied
VL1L2 = "VL1L2"  # Vector L1L2 Partial Sums
VAL1L2 = "VAL1L2"  # Vector Anomaly L1L2 Partial Sums when climatological data is
# supplied
PCT = "PCT"  # Contingency Table Counts for Probabilistic Forecasts
PSTD = "PSTD"  # Contingency Table Stats for Probabilistic Forecasts with Dichotomous
# outcomes
PJC = "PJC"  # Joint and Conditional Factorization for Probabilistic Forecasts
PRC = "PRC"  # Receiver Operating Characteristic for Probabilistic Forecasts
ECLV = "ECLV"  # Economic Cost/Loss Value derived from CTC and PCT lines
MPR = "MPR"  # Matched Pair TCDiag_Data
NBRCTC = "NBRCTC"  # Neighborhood Contingency Table Counts
NBRCTS = "NBRCTS"  # Neighborhood Contingency Table Statistics
NBRCNT = "NBRCNT"  # Neighborhood Continuous Statistics
ISC = "ISC"  # Intensity-Scale
RHIST = "RHIST"  # Ranked Histogram
PHIST = "PHIST"  # Probability Integral Transform Histogram
ORANK = "ORANK"  # Observation Rank
SSVAR = "SSVAR"  # Spread Skill Variance
GRAD = "GRAD"  # Gradient statistics (S1 score)
VCNT = "VCNT"  # Vector Continuous Statistics
RELP = "RELP"  # Relative Position
ECNT = "ECNT"  # Ensemble Continuous Statistics - only for HiRA
ENSCNT = "ENSCNT"  #
PERC = "PERC"  #
DMAP = "DMAP"  # Distance Map
RPS = "RPS"  # Ranked Probability Score
SSIDX = "SSIDX"  # SKILL SCORE INDEX
SEEPS = "SEEPS"  # Stable Equitable Error in Probability Space (SEEPS) score
SEEPS_MPR = "SEEPS_MPR"  # SEEPS Matched Pair TCDiag_Data

# VSDB line types
BSS = "BSS"  # same as PSTD
RELI = "RELI"  # same as PCT
HIST = "HIST"  # same as RHIST
ECON = "ECON"  # same as ECLV
RMSE = "RMSE"  # same as CNT
FSS = "FSS"  # same as NBRCNT
# VSDB version of FHO goes to CTC
# VSDB version of RPS goes to ENSCNT
# VSDB versions of RELP, SL1L2, SAL1L2, VL1L2, VAL1L2, and GRAD do not change

# TCST line types
TCMPR = "TCMPR"  # Tropical Cyclone Matched Pair line type
PROBRIRW = "PROBRIRW"  # Probability of Rapid Intensification line type
TCDIAG = "TCDIAG"  # Tropical Cyclone Diagnostics line type

UC_LINE_TYPES = [FHO, CTC, CTS, MCTC, MCTS, CNT, SL1L2, SAL1L2, VL1L2, VAL1L2,
                 PCT, PSTD, PJC, PRC, ECLV, MPR, NBRCTC, NBRCTS, NBRCNT, ISC,
                 RHIST, PHIST, ORANK, SSVAR, GRAD, VCNT, RELP, ECNT, ENSCNT, PERC,
                 DMAP, RPS, SSIDX, SEEPS, SEEPS_MPR]

UC_LINE_TYPES_TCST = [TCMPR, PROBRIRW, TCDIAG]

LC_LINE_TYPES = [ltype.lower() for ltype in UC_LINE_TYPES]
LC_LINE_TYPES_TCST = [ltype.lower() for ltype in UC_LINE_TYPES_TCST]

LINE_TABLES = ['line_data_' + hname for hname in LC_LINE_TYPES]

LINE_TABLES_TCST = ['line_data_' + hname for hname in LC_LINE_TYPES_TCST]

ALPHA_LINE_TYPES = [CTS, NBRCTS, NBRCNT, MCTS, SSVAR, VCNT, DMAP, RPS, CNT,
                    PSTD, SSIDX]

COV_THRESH_LINE_TYPES = [NBRCTC, NBRCTS, PCT, PSTD, PJC, PRC]

VAR_LINE_TYPES = [PCT, PSTD, PJC, PRC, MCTC, RHIST, PHIST, RELP, ORANK, ECLV]

VAR_LINE_TYPES_TCST = [PROBRIRW, TCDIAG]

OLD_VSDB_LINE_TYPES = [BSS, ECON, HIST, RELI, RMSE, RPS, FHO, FSS]

VSDB_TO_STAT_TYPES = [PSTD, ECLV, RHIST, PCT, CNT, ENSCNT, CTC, NBRCNT]

ENS_VSDB_LINE_TYPES = [BSS, ECON, HIST, RELI, RELP, RMSE, RPS]

ALL_VSDB_LINE_TYPES = OLD_VSDB_LINE_TYPES + [RELP, SL1L2, SAL1L2, VL1L2, VAL1L2, GRAD]

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

AMODEL = "amodel"
BMODEL = "bmodel"
STORM_ID = 'storm_id'
BASIN = "basin"
CYCLONE = "cyclone"
STORM_NAME = "storm_name"
INIT = "init"
LEAD = "lead"
VALID = "valid"
INIT_MASK = "init_mask"
VALID_MASK = "valid_mask"

LEAD_HR = "lead_hr"

# After units added in MET 8.1
LONG_HEADER = [VERSION, MODEL, DESCR, FCST_LEAD, FCST_VALID_BEG, FCST_VALID_END,
               OBS_LEAD, OBS_VALID_BEG, OBS_VALID_END, FCST_VAR, FCST_UNITS, FCST_LEV,
               OBS_VAR, OBS_UNITS, OBS_LEV, OBTYPE, VX_MASK, INTERP_MTHD, INTERP_PNTS,
               FCST_THRESH, OBS_THRESH, COV_THRESH, ALPHA, LINE_TYPE]

LONG_HEADER_TCST = [VERSION, AMODEL, BMODEL, DESCR, STORM_ID, BASIN, CYCLONE,
                    STORM_NAME, INIT, LEAD, VALID, INIT_MASK, VALID_MASK, LINE_TYPE]

# Contains DESC but not UNITS
MID_HEADER = LONG_HEADER[0:10] + LONG_HEADER[11:13] + LONG_HEADER[14:]

# No DESC and no UNITS
SHORT_HEADER = MID_HEADER[0:2] + MID_HEADER[3:]
SHORT_HEADER_TCST = LONG_HEADER_TCST[0:3] + LONG_HEADER_TCST[4:]

STAT_HEADER_KEYS = [VERSION, MODEL, DESCR, FCST_VAR, FCST_UNITS, FCST_LEV,
                    OBS_VAR, OBS_UNITS, OBS_LEV, OBTYPE, VX_MASK,
                    INTERP_MTHD, INTERP_PNTS, FCST_THRESH, OBS_THRESH]

TCST_HEADER_KEYS = [VERSION, AMODEL, BMODEL, DESCR, STORM_ID, BASIN, CYCLONE,
                    STORM_NAME, INIT_MASK, VALID_MASK]

VSDB_HEADER = [VERSION, MODEL, FCST_LEAD, FCST_VALID_BEG, OBTYPE,
               VX_MASK, LINE_TYPE, FCST_VAR, FCST_LEV]

Q_FILE = "SELECT data_file_id FROM data_file WHERE " + \
         "path=%s AND filename=%s"

Q_HEADER = "SELECT stat_header_id FROM stat_header WHERE " + \
           "=%s AND ".join(STAT_HEADER_KEYS[1:]) + "=%s"

Q_HEADER_TCST = "SELECT tcst_header_id FROM tcst_header WHERE " + \
                "=%s AND ".join(TCST_HEADER_KEYS[1:]) + "=%s"

Q_METADATA = "SELECT category, description FROM metadata"

STAT_HEADER = 'stat_header'
TCST_HEADER = 'tcst_header'
STAT_HEADER_ID = 'stat_header_id'
TCST_HEADER_ID = 'tcst_header_id'
LINE_DATA_ID = 'line_data_id'
LINE_NUM = 'line_num'
TOTAL_LC = 'total'
FCST_PERC = 'fcst_perc'
OBS_PERC = 'obs_perc'

DATA_FILE = 'data_file'
FULL_FILE = 'full_file'
DATA_FILE_ID = 'data_file_id'
DATA_FILE_LU_ID = 'data_file_lu_id'
FILE_ROW = 'file_row'
FILENAME = 'filename'
FILEPATH = 'path'
LOAD_DATE = 'load_date'
MOD_DATE = 'mod_date'
FY_OY = 'fy_oy'
FY_ON = 'fy_on'
FN_OY = 'fn_oy'
FN_ON = 'fn_on'
BASER = 'baser'
FMEAN = 'fmean'
FCST_VALID = 'fcst_valid'
FCST_INIT = 'fcst_init'
EC_VALUE = 'ec_value'
FI_OJ = 'fi_oj'
N_CAT = 'n_cat'

INSTANCE_INFO = 'instance_info'
INSTANCE_INFO_ID = 'instance_info_id'

DATA_FILE_FIELDS = [DATA_FILE_ID, DATA_FILE_LU_ID, FILENAME, FILEPATH,
                    LOAD_DATE, MOD_DATE]

STAT_HEADER_FIELDS = [STAT_HEADER_ID] + STAT_HEADER_KEYS
TCST_HEADER_FIELDS = [TCST_HEADER_ID] + TCST_HEADER_KEYS

VALUE_SLOTS = '%s, ' * len(STAT_HEADER_FIELDS)
VALUE_SLOTS = VALUE_SLOTS[:-2]

VALUE_SLOTS_TCST = '%s, ' * len(TCST_HEADER_FIELDS)
VALUE_SLOTS_TCST = VALUE_SLOTS_TCST[:-2]

INS_HEADER = "INSERT INTO stat_header (" + ",".join(STAT_HEADER_FIELDS) + \
             ") VALUES (" + VALUE_SLOTS + ")"

INS_HEADER_TCST = "INSERT INTO tcst_header (" + ",".join(TCST_HEADER_FIELDS) + \
                  ") VALUES (" + VALUE_SLOTS_TCST + ")"

INS_DATA_FILES = "INSERT INTO data_file (" + ",".join(DATA_FILE_FIELDS) + \
                 ") VALUES (%s, %s, %s, %s, %s, %s)"

INS_METADATA = "INSERT INTO metadata (category, description) VALUES (%s, %s)"

INS_INSTANCE = "INSERT INTO instance_info (instance_info_id, updater, update_date, " + \
               "update_detail, load_xml) VALUES (%s, %s, %s, %s, %s)"

UPD_METADATA = "UPDATE metadata SET category=%s, description=%s"

LD_TABLE = "LOAD DATA LOCAL INFILE '{}' INTO TABLE {} FIELDS TERMINATED BY '{}';"

ALL_LINE_DATA_FIELDS = [STAT_HEADER_ID, DATA_FILE_ID, LINE_NUM,
                        FCST_LEAD, FCST_VALID_BEG, FCST_VALID_END, FCST_INIT_BEG,
                        OBS_LEAD, OBS_VALID_BEG, OBS_VALID_END]

ALL_LINE_DATA_FIELDS_TCST = [TCST_HEADER_ID, DATA_FILE_ID, LINE_NUM,
                             FCST_LEAD, FCST_VALID, FCST_INIT]

TOT_LINE_DATA_FIELDS = ALL_LINE_DATA_FIELDS + [TOTAL_LC]

ALPH_LINE_DATA_FIELDS = ALL_LINE_DATA_FIELDS + [ALPHA, TOTAL_LC]

COV_LINE_DATA_FIELDS = ALL_LINE_DATA_FIELDS + [COV_THRESH, TOTAL_LC]

COVA_LINE_DATA_FIELDS = ALL_LINE_DATA_FIELDS + [COV_THRESH, ALPHA, TOTAL_LC]

ALL_COUNT = len(ALL_LINE_DATA_FIELDS)

ALL_COUNT_TCST = len(ALL_LINE_DATA_FIELDS_TCST)

LINE_DATA_FIELDS = dict()
LINE_DATA_VAR_FIELDS = dict()
LINE_DATA_COLS = dict()
LINE_DATA_COLS_TCST = dict()
LINE_DATA_Q = dict()
LINE_DATA_VAR_Q = dict()
LINE_VAR_COUNTER = dict()
LINE_VAR_REPEATS = dict()
LINE_DATA_VAR_TABLES = dict()
COLUMNS = dict()

LINE_DATA_FIELDS_TO_REPLACE = dict()

LINE_DATA_VAR_TABLES[PCT] = 'line_data_pct_thresh'
LINE_DATA_VAR_TABLES[PSTD] = 'line_data_pstd_thresh'
LINE_DATA_VAR_TABLES[PJC] = 'line_data_pjc_thresh'
LINE_DATA_VAR_TABLES[PRC] = 'line_data_prc_thresh'
LINE_DATA_VAR_TABLES[MCTC] = 'line_data_mctc_cnt'
LINE_DATA_VAR_TABLES[RHIST] = 'line_data_rhist_rank'
LINE_DATA_VAR_TABLES[RELP] = 'line_data_relp_ens'
LINE_DATA_VAR_TABLES[PHIST] = 'line_data_phist_bin'
LINE_DATA_VAR_TABLES[ORANK] = 'line_data_orank_ens'
LINE_DATA_VAR_TABLES[ECLV] = 'line_data_eclv_pnt'
LINE_DATA_VAR_TABLES[PROBRIRW] = 'line_data_probrirw_thresh'
LINE_DATA_VAR_TABLES[TCDIAG] = 'line_data_tcdiag_diag'

LINE_DATA_FIELDS[CNT] = ALPH_LINE_DATA_FIELDS + \
                        ['fbar', 'fbar_ncl', 'fbar_ncu', 'fbar_bcl', 'fbar_bcu',
                         'fstdev', 'fstdev_ncl', 'fstdev_ncu', 'fstdev_bcl',
                         'fstdev_bcu',
                         'obar', 'obar_ncl', 'obar_ncu', 'obar_bcl', 'obar_bcu',
                         'ostdev', 'ostdev_ncl', 'ostdev_ncu', 'ostdev_bcl',
                         'ostdev_bcu',
                         'pr_corr', 'pr_corr_ncl', 'pr_corr_ncu', 'pr_corr_bcl',
                         'pr_corr_bcu',
                         'sp_corr', 'dt_corr', 'ranks', 'frank_ties', 'orank_ties',
                         'me', 'me_ncl', 'me_ncu', 'me_bcl', 'me_bcu',
                         'estdev', 'estdev_ncl', 'estdev_ncu', 'estdev_bcl',
                         'estdev_bcu',
                         'mbias', 'mbias_bcl', 'mbias_bcu', 'mae', 'mae_bcl', 'mae_bcu',
                         'mse', 'mse_bcl', 'mse_bcu', 'bcmse', 'bcmse_bcl', 'bcmse_bcu',
                         'rmse', 'rmse_bcl', 'rmse_bcu', 'e10', 'e10_bcl', 'e10_bcu',
                         'e25', 'e25_bcl', 'e25_bcu', 'e50', 'e50_bcl', 'e50_bcu',
                         'e75', 'e75_bcl', 'e75_bcu', 'e90', 'e90_bcl', 'e90_bcu',
                         'iqr', 'iqr_bcl', 'iqr_bcu', 'mad', 'mad_bcl', 'mad_bcu',
                         'anom_corr', 'anom_corr_ncl', 'anom_corr_ncu',
                         'anom_corr_bcl', 'anom_corr_bcu',
                         'me2', 'me2_bcl', 'me2_bcu', 'msess', 'msess_bcl', 'msess_bcu',
                         'rmsfa', 'rmsfa_bcl', 'rmsfa_bcu', 'rmsoa', 'rmsoa_bcl',
                         'rmsoa_bcu',
                         'anom_corr_uncntr', 'anom_corr_uncntr_bcl',
                         'anom_corr_uncntr_bcu',
                         'si', 'si_bcl', 'si_bcu']

LINE_DATA_FIELDS[CTC] = TOT_LINE_DATA_FIELDS + \
                        [FY_OY, FY_ON, FN_OY, FN_ON, EC_VALUE]

LINE_DATA_FIELDS[CTS] = ALPH_LINE_DATA_FIELDS + \
                        [BASER, 'baser_ncl', 'baser_ncu', 'baser_bcl', 'baser_bcu',
                         FMEAN, 'fmean_ncl', 'fmean_ncu', 'fmean_bcl', 'fmean_bcu',
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
                         'bagss', 'bagss_bcl', 'bagss_bcu', 'hss_ec', 'hss_ec_bcl',
                         'hss_ec_bcu', EC_VALUE]

LINE_DATA_FIELDS[DMAP] = ALPH_LINE_DATA_FIELDS + \
                         ['fy', 'oy', 'fbias', 'baddeley', 'hausdorff',
                          'med_fo', 'med_of', 'med_min', 'med_max', 'med_mean',
                          'fom_fo', 'fom_of', 'fom_min', 'fom_max', 'fom_mean',
                          'zhu_fo', 'zhu_of', 'zhu_min', 'zhu_max', 'zhu_mean',
                          'g', 'gbeta', 'beta_value']

LINE_DATA_FIELDS[ECLV] = TOT_LINE_DATA_FIELDS + \
                         [BASER, 'value_baser', 'n_pnt']

LINE_DATA_FIELDS[ECNT] = TOT_LINE_DATA_FIELDS + \
                         ['n_ens', 'crps', 'crpss', 'ign', 'me', 'rmse', 'spread',
                          'me_oerr', 'rmse_oerr', 'spread_oerr', 'spread_plus_oerr',
                          'crpscl', 'crps_emp', 'crpscl_emp', 'crpss_emp',
                          'crps_emp_fair', 'spread_md', 'mae', 'mae_oerr',
                          'bias_ratio', 'n_ge_obs', 'me_ge_obs',
                          'n_lt_obs', 'me_lt_obs', 'ign_conv_oerr', 'ign_corr_oerr']

LINE_DATA_FIELDS[ENSCNT] = ALL_LINE_DATA_FIELDS + \
                           ['rpsf', 'rpsf_ncl', 'rpsf_ncu', 'rpsf_bcl', 'rpsf_bcu',
                            'rpscl', 'rpscl_ncl', 'rpscl_ncu', 'rpscl_bcl', 'rpscl_bcu',
                            'rpss', 'rpss_ncl', 'rpss_ncu', 'rpss_bcl', 'rpss_bcu',
                            'crpsf', 'crpsf_ncl', 'crpsf_ncu', 'crpsf_bcl', 'crpsf_bcu',
                            'crpscl', 'crpscl_ncl', 'crpscl_ncu', 'crpscl_bcl',
                            'crpscl_bcu',
                            'crpss', 'crpss_ncl', 'crpss_ncu', 'crpss_bcl', 'crpss_bcu']

LINE_DATA_FIELDS[FHO] = TOT_LINE_DATA_FIELDS + \
                        ['f_rate', 'h_rate', 'o_rate']

LINE_DATA_FIELDS[GRAD] = TOT_LINE_DATA_FIELDS + \
                         ['fgbar', 'ogbar', 'mgbar', 'egbar', 's1', 's1_og',
                          'fgog_ratio',
                          'dx', 'dy']

LINE_DATA_FIELDS[ISC] = TOT_LINE_DATA_FIELDS + \
                        ['tile_dim', 'time_xll', 'tile_yll', 'nscale', 'iscale', 'mse',
                         'isc', 'fenergy2', 'oenergy2', BASER, 'fbias']

LINE_DATA_FIELDS[MCTC] = TOT_LINE_DATA_FIELDS + \
                         ['n_cat', EC_VALUE]

LINE_DATA_FIELDS[MCTS] = ALPH_LINE_DATA_FIELDS + \
                         ['n_cat', 'acc', 'acc_ncl', 'acc_ncu', 'acc_bcl', 'acc_bcu',
                          'hk', 'hk_bcl', 'hk_bcu', 'hss', 'hss_bcl', 'hss_bcu',
                          'ger', 'ger_bcl', 'ger_bcu', 'hss_ec', 'hss_ec_bcl',
                          'hss_ec_bcu', EC_VALUE]

LINE_DATA_FIELDS[MPR] = TOT_LINE_DATA_FIELDS + \
                        ['mp_index', 'obs_sid', 'obs_lat', 'obs_lon', 'obs_lvl',
                         'obs_elv',
                         'mpr_fcst', 'mpr_obs', 'mpr_climo', 'obs_qc',
                         'obs_climo_mean', 'obs_climo_stdev', 'obs_climo_cdf',
                         'fcst_climo_mean','fcst_climo_stdev']

LINE_DATA_FIELDS[NBRCNT] = ALPH_LINE_DATA_FIELDS + \
                           ['fbs', 'fbs_bcl', 'fbs_bcu', 'fss', 'fss_bcl', 'fss_bcu',
                            'afss', 'afss_bcl', 'afss_bcu', 'ufss', 'ufss_bcl',
                            'ufss_bcu',
                            'f_rate', 'f_rate_bcl', 'f_rate_bcu',
                            'o_rate', 'o_rate_bcl', 'o_rate_bcu']

LINE_DATA_FIELDS[NBRCTC] = COV_LINE_DATA_FIELDS + \
                           [FY_OY, FY_ON, FN_OY, FN_ON]

LINE_DATA_FIELDS[NBRCTS] = COVA_LINE_DATA_FIELDS + \
                           [BASER, 'baser_ncl', 'baser_ncu', 'baser_bcl', 'baser_bcu',
                            FMEAN, 'fmean_ncl', 'fmean_ncu', 'fmean_bcl', 'fmean_bcu',
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

LINE_DATA_FIELDS[ORANK] = TOT_LINE_DATA_FIELDS + \
                          ['orank_index', 'obs_sid', 'obs_lat', 'obs_lon', 'obs_lvl',
                           'obs_elv',
                           'obs', 'pit', 'obs_rank', 'n_ens_vld', 'n_ens', 'obs_qc',
                           'ens_mean', 'obs_climo_mean', 'spread', 'ens_mean_oerr',
                           'spread_oerr', 'spread_plus_oerr',
                           'obs_climo_stdev', 'fcst_climo_mean', 'fcst_climo_stdev']

LINE_DATA_FIELDS[PCT] = COV_LINE_DATA_FIELDS + \
                        ['n_thresh']

LINE_DATA_FIELDS[PERC] = ALL_LINE_DATA_FIELDS + \
                         ['fcst_perc', 'obs_perc']

LINE_DATA_FIELDS[PHIST] = TOT_LINE_DATA_FIELDS + \
                          ['bin_size', 'n_bin']

LINE_DATA_FIELDS[PJC] = COV_LINE_DATA_FIELDS + \
                        ['n_thresh']

LINE_DATA_FIELDS[PRC] = COV_LINE_DATA_FIELDS + \
                        ['n_thresh']

# the last 5 fields are currently (August 2019) blank in stat files, filled in in
# write_stat_sql
LINE_DATA_FIELDS[PSTD] = COVA_LINE_DATA_FIELDS + \
                         ['n_thresh', BASER, 'baser_ncl', 'baser_ncu',
                          'reliability', 'resolution', 'uncertainty', 'roc_auc',
                          'brier', 'brier_ncl', 'brier_ncu', 'briercl', 'briercl_ncl',
                          'briercl_ncu', 'bss', 'bss_smpl']

LINE_DATA_FIELDS[RELP] = TOT_LINE_DATA_FIELDS + \
                         ['n_ens']

LINE_DATA_FIELDS[RHIST] = TOT_LINE_DATA_FIELDS + \
                          ['n_rank']

LINE_DATA_FIELDS[RPS] = ALPH_LINE_DATA_FIELDS + \
                        ['n_prob', 'rps_rel', 'rps_res', 'rps_unc', 'rps',
                         'rpss', 'rpss_smpl', 'rps_comp']

LINE_DATA_FIELDS[SL1L2] = TOT_LINE_DATA_FIELDS + \
                          ['fbar', 'obar', 'fobar', 'ffbar', 'oobar', 'mae']

LINE_DATA_FIELDS[SAL1L2] = TOT_LINE_DATA_FIELDS + \
                           ['fabar', 'oabar', 'foabar', 'ffabar', 'ooabar', 'mae']

LINE_DATA_FIELDS[SEEPS] = TOT_LINE_DATA_FIELDS + \
                          ['s12', 's13', 's21', 's23', 's31', 's32',
                           'pf1', 'pf2', 'pf3', 'pv1', 'pv2', 'pv3',
                           'mean_fcst', 'mean_obs', 'seeps']

LINE_DATA_FIELDS[SEEPS_MPR] = ALL_LINE_DATA_FIELDS + \
                              ['obs_sid', 'obs_lat', 'obs_lon',
                               'fcst', 'obs', 'obs_qc', 'fcst_cat', 'obs_cat',
                               'p1', 'p2', 't1', 't2', 'seeps']

LINE_DATA_FIELDS[SSIDX] = ALL_LINE_DATA_FIELDS + \
                          [ALPHA, 'fcst_model', 'ref_model', 'n_init', 'n_term',
                           'v_vld', 'ss_index']

LINE_DATA_FIELDS[SSVAR] = ALPH_LINE_DATA_FIELDS + \
                          ['n_bin', 'bin_i', 'bin_n', 'var_min', 'var_max', 'var_mean',
                           'fbar', 'obar', 'fobar', 'ffbar', 'oobar',
                           'fbar_ncl', 'fbar_ncu', 'fstdev', 'fstdev_ncl', 'fstdev_ncu',
                           'obar_ncl', 'obar_ncu', 'ostdev', 'ostdev_ncl', 'ostdev_ncu',
                           'pr_corr', 'pr_corr_ncl', 'pr_corr_ncu', 'me', 'me_ncl',
                           'me_ncu',
                           'estdev', 'estdev_ncl', 'estdev_ncu', 'mbias', 'mse',
                           'bcmse', 'rmse']

LINE_DATA_FIELDS[VL1L2] = TOT_LINE_DATA_FIELDS + \
                          ['ufbar', 'vfbar', 'uobar', 'vobar', 'uvfobar', 'uvffbar',
                           'uvoobar', 'f_speed_bar', 'o_speed_bar', 'dir_me', 'dir_mae', 'dir_mse']

LINE_DATA_FIELDS[VAL1L2] = TOT_LINE_DATA_FIELDS + \
                           ['ufabar', 'vfabar', 'uoabar', 'voabar', 'uvfoabar',
                            'uvffabar',
                            'uvooabar', 'fa_speed_bar', 'oa_speed_bar', 'dira_me', 'dira_mae', 'dira_mse']

LINE_DATA_FIELDS[VCNT] = ALPH_LINE_DATA_FIELDS + \
                         ['fbar', 'fbar_bcl', 'fbar_bcu', 'obar', 'obar_bcl',
                          'obar_bcu',
                          'fs_rms', 'fs_rms_bcl', 'fs_rms_bcu',
                          'os_rms', 'os_rms_bcl', 'os_rms_bcu',
                          'msve', 'msve_bcl', 'msve_bcu', 'rmsve', 'rmsve_bcl',
                          'rmsve_bcu',
                          'fstdev', 'fstdev_bcl', 'fstdev_bcu',
                          'ostdev', 'ostdev_bcl', 'ostdev_bcu',
                          'fdir', 'fdir_bcl', 'fdir_bcu', 'odir', 'odir_bcl',
                          'odir_bcu',
                          'fbar_speed', 'fbar_speed_bcl', 'fbar_speed_bcu',
                          'obar_speed', 'obar_speed_bcl', 'obar_speed_bcu',
                          'vdiff_speed', 'vdiff_speed_bcl', 'vdiff_speed_bcu',
                          'vdiff_dir', 'vdiff_dir_bcl', 'vdiff_dir_bcu',
                          'speed_err', 'speed_err_bcl', 'speed_err_bcu',
                          'speed_abserr', 'speed_abserr_bcl', 'speed_abserr_bcu',
                          'dir_err', 'dir_err_bcl', 'dir_err_bcu',
                          'dir_abserr', 'dir_abserr_bcl', 'dir_abserr_bcu',
                          'anom_corr', 'anom_corr_ncl', 'anom_corr_ncu',
                          'anom_corr_bcl', 'anom_corr_bcu', 'anom_corr_uncntr',
                          'anom_corr_uncntr_bcl', 'anom_corr_uncntr_bcu', 'dir_me',
                          'dir_me_bcl', 'dir_me_bcu',
                          'dir_mae', 'dir_mae_bcl', 'dir_mae_bcu',
                          'dir_mse', 'dir_mse_bcl', 'dir_mse_bcu',
                          'dir_rmse', 'dir_rmse_bcl', 'dir_rmse_bcu']

COLUMNS[TCMPR] = ['total', 'index_pair', 'level', 'watch_warn', 'initials', 'alat',
                  'alon',
                  'blat', 'blon', 'tk_err', 'x_err', 'y_err', 'altk_err',
                  'crtk_err',
                  'adland', 'bdland', 'amslp', 'bmslp', 'amax_wind', 'bmax_wind',
                  'aal_wind_34',
                  'bal_wind_34', 'ane_wind_34', 'bne_wind_34', 'ase_wind_34',
                  'bse_wind_34', 'asw_wind_34',
                  'bsw_wind_34', 'anw_wind_34', 'bnw_wind_34', 'aal_wind_50',
                  'bal_wind_50', 'ane_wind_50',
                  'bne_wind_50', 'ase_wind_50', 'bse_wind_50', 'asw_wind_50',
                  'bsw_wind_50', 'anw_wind_50',
                  'bnw_wind_50', 'aal_wind_64', 'bal_wind_64', 'ane_wind_64',
                  'bne_wind_64', 'ase_wind_64',
                  'bse_wind_64', 'asw_wind_64', 'bsw_wind_64', 'anw_wind_64',
                  'bnw_wind_64', 'aradp',
                  'bradp', 'arrp', 'brrp', 'amrd', 'bmrd', 'agusts', 'bgusts',
                  'aeye', 'beye', 'adir', 'bdir',
                  'aspeed', 'bspeed', 'adepth', 'bdepth',
                  'num_members', 'track_spread', 'track_stdev',
                  'mslp_stdev', 'max_wind_stdev']

LINE_DATA_FIELDS[TCMPR] = ALL_LINE_DATA_FIELDS_TCST + COLUMNS[TCMPR]

COLUMNS[PROBRIRW] = ['alat', 'alon', 'blat', 'blon', 'initials', 'tk_err', 'x_err',
                     'y_err', 'adland', 'bdland', 'rirw_beg', 'rirw_end', 'rirw_window',
                     'awind_end', 'bwind_beg', 'bwind_end',
                     'bdelta', 'bdelta_max', 'blevel_beg', 'blevel_end',
                     'n_thresh']

LINE_DATA_FIELDS[PROBRIRW] = ALL_LINE_DATA_FIELDS_TCST + COLUMNS[PROBRIRW]

COLUMNS[TCDIAG] = ['total', 'index_pair', 'diag_source', 'track_source',
                   'field_source', 'n_diag']

LINE_DATA_FIELDS[TCDIAG] = ALL_LINE_DATA_FIELDS_TCST + COLUMNS[TCDIAG]

LINE_DATA_FIELDS_TO_REPLACE[TCMPR] = ['fcst_lead', 'total', 'index_pair', 'alat',
                                      'alon',
                                      'blat', 'blon', 'tk_err', 'x_err', 'y_err',
                                      'altk_err',
                                      'crtk_err',
                                      'adland', 'bdland', 'amslp', 'bmslp',
                                      'amax_wind', 'bmax_wind', 'aal_wind_34',
                                      'bal_wind_34', 'ane_wind_34', 'bne_wind_34',
                                      'ase_wind_34', 'bse_wind_34', 'asw_wind_34',
                                      'bsw_wind_34', 'anw_wind_34', 'bnw_wind_34',
                                      'aal_wind_50', 'bal_wind_50', 'ane_wind_50',
                                      'bne_wind_50', 'ase_wind_50', 'bse_wind_50',
                                      'asw_wind_50', 'bsw_wind_50', 'anw_wind_50',
                                      'bnw_wind_50', 'aal_wind_64', 'bal_wind_64',
                                      'ane_wind_64', 'bne_wind_64', 'ase_wind_64',
                                      'bse_wind_64', 'asw_wind_64', 'bsw_wind_64',
                                      'anw_wind_64', 'bnw_wind_64',
                                      'aradp', 'bradp', 'arrp', 'brrp', 'amrd', 'bmrd',
                                      'agusts', 'bgusts', 'aeye', 'beye',
                                      'adir', 'bdir', 'aspeed', 'bspeed',
                                      'num_members', 'track_spread', 'track_stdev',
                                      'mslp_stdev', 'max_wind_stdev']

LINE_DATA_FIELDS_TO_REPLACE[PROBRIRW] = ['fcst_lead', 'alat', 'alon', 'blat', 'blon',
                                         'tk_err', 'x_err',
                                         'y_err', 'adland', 'bdland', 'rirw_beg',
                                         'rirw_end',
                                         'rirw_window', 'awind_end',
                                         'bwind_beg', 'bwind_end', 'bdelta',
                                         'bdelta_max']

LINE_DATA_FIELDS_TO_REPLACE[TCDIAG] = ['fcst_lead', 'total', 'index_pair']

VAR_DATA_FIELDS = [LINE_DATA_ID, 'i_value']

LINE_DATA_VAR_FIELDS[PCT] = VAR_DATA_FIELDS + ['thresh_i', 'oy_i', 'on_i']

LINE_DATA_VAR_FIELDS[PSTD] = VAR_DATA_FIELDS + ['thresh_i']

LINE_DATA_VAR_FIELDS[PJC] = VAR_DATA_FIELDS + \
                            ['thresh_i', 'oy_tp_i', 'on_tp_i', 'calibration_i',
                             'refinement_i', 'likelihood_i', 'baser_i']

LINE_DATA_VAR_FIELDS[PRC] = VAR_DATA_FIELDS + ['thresh_i', 'pody_i', 'pofd_i']

LINE_DATA_VAR_FIELDS[MCTC] = VAR_DATA_FIELDS + ['j_value', 'fi_oj']

LINE_DATA_VAR_FIELDS[RHIST] = VAR_DATA_FIELDS + ['rank_i']

LINE_DATA_VAR_FIELDS[RELP] = VAR_DATA_FIELDS + ['ens_i']

LINE_DATA_VAR_FIELDS[PHIST] = VAR_DATA_FIELDS + ['bin_i']

LINE_DATA_VAR_FIELDS[ORANK] = VAR_DATA_FIELDS + ['ens_i']

LINE_DATA_VAR_FIELDS[ECLV] = VAR_DATA_FIELDS + ['x_pnt_i', 'y_pnt_i']

LINE_DATA_VAR_FIELDS[PROBRIRW] = VAR_DATA_FIELDS + ['thresh_i', 'prob_i']

LINE_DATA_VAR_FIELDS[TCDIAG] = VAR_DATA_FIELDS + ['diag_i', 'value_i']

for line_type in UC_LINE_TYPES_TCST:
    LINE_DATA_COLS_TCST[line_type] = \
        ALL_LINE_DATA_FIELDS_TCST + \
        COL_NUMS[0:(len(LINE_DATA_FIELDS[line_type]) - ALL_COUNT_TCST)]

    if line_type in VAR_LINE_TYPES_TCST:
        LINE_DATA_COLS_TCST[line_type] = [LINE_DATA_ID] + LINE_DATA_COLS_TCST[line_type]

    # For each line type, create insert queries
    VALUE_SLOTS = '%s, ' * len(LINE_DATA_FIELDS[line_type])
    VALUE_SLOTS = VALUE_SLOTS[:-2]

    line_table = LINE_TABLES_TCST[UC_LINE_TYPES_TCST.index(line_type)]

    i_line = "INSERT INTO " + line_table + " (" + ",".join(
        LINE_DATA_FIELDS[line_type]) + \
             ") VALUES (" + VALUE_SLOTS + ")"

    LINE_DATA_Q[line_type] = i_line

    if line_type in VAR_LINE_TYPES_TCST:
        VALUE_SLOTS = '%s, ' * len(LINE_DATA_VAR_FIELDS[line_type])
        VALUE_SLOTS = VALUE_SLOTS[:-2]

        var_line_table = LINE_DATA_VAR_TABLES[line_type]
        i_line = "INSERT INTO " + var_line_table + " (" + \
                 ",".join(LINE_DATA_VAR_FIELDS[line_type]) + \
                 ") VALUES (" + VALUE_SLOTS + ")"
        LINE_DATA_VAR_Q[line_type] = i_line

for line_type in UC_LINE_TYPES:
    # for each line type, create a list of the columns in the dataframe
    if line_type in COV_THRESH_LINE_TYPES:
        # both alpha and cov_thresh
        if line_type in ALPHA_LINE_TYPES:
            LINE_DATA_COLS[line_type] = \
                COVA_LINE_DATA_FIELDS[0:-1] + \
                COL_NUMS[0:(len(LINE_DATA_FIELDS[line_type]) - (ALL_COUNT + 2))]
        else:
            LINE_DATA_COLS[line_type] = \
                COV_LINE_DATA_FIELDS[0:-1] + \
                COL_NUMS[0:(len(LINE_DATA_FIELDS[line_type]) - (ALL_COUNT + 1))]
    elif line_type in ALPHA_LINE_TYPES:
        LINE_DATA_COLS[line_type] = \
            ALPH_LINE_DATA_FIELDS[0:-1] + \
            COL_NUMS[0:(len(LINE_DATA_FIELDS[line_type]) - (ALL_COUNT + 1))]
    else:
        LINE_DATA_COLS[line_type] = \
            ALL_LINE_DATA_FIELDS + \
            COL_NUMS[0:(len(LINE_DATA_FIELDS[line_type]) - ALL_COUNT)]

    if line_type in VAR_LINE_TYPES:
        LINE_DATA_COLS[line_type] = [LINE_DATA_ID] + LINE_DATA_COLS[line_type]

    # For each line type, create insert queries
    VALUE_SLOTS = '%s, ' * len(LINE_DATA_FIELDS[line_type])
    VALUE_SLOTS = VALUE_SLOTS[:-2]

    line_table = LINE_TABLES[UC_LINE_TYPES.index(line_type)]

    i_line = "INSERT INTO " + line_table + " (" + ",".join(
        LINE_DATA_FIELDS[line_type]) + \
             ") VALUES (" + VALUE_SLOTS + ")"
    LINE_DATA_Q[line_type] = i_line

    if line_type in VAR_LINE_TYPES:
        VALUE_SLOTS = '%s, ' * len(LINE_DATA_VAR_FIELDS[line_type])
        VALUE_SLOTS = VALUE_SLOTS[:-2]

        var_line_table = LINE_DATA_VAR_TABLES[line_type]

        i_line = "INSERT INTO " + var_line_table + " (" + \
                 ",".join(LINE_DATA_VAR_FIELDS[line_type]) + \
                 ") VALUES (" + VALUE_SLOTS + ")"
        LINE_DATA_VAR_Q[line_type] = i_line

LINE_DATA_COLS[PERC] = LINE_DATA_COLS[PERC][0:-2] + [FCST_PERC, OBS_PERC]

# column name of n_* after Total. phist, orank, and eclv have extra fields after Total
LINE_VAR_COUNTER[PCT] = '1'
LINE_VAR_COUNTER[PSTD] = '1'
LINE_VAR_COUNTER[PJC] = '1'
LINE_VAR_COUNTER[PRC] = '1'
LINE_VAR_COUNTER[MCTC] = '1'
LINE_VAR_COUNTER[RHIST] = '1'
LINE_VAR_COUNTER[RELP] = '1'
LINE_VAR_COUNTER[PHIST] = '2'
LINE_VAR_COUNTER[ORANK] = '11'
LINE_VAR_COUNTER[ECLV] = '3'
LINE_VAR_COUNTER[PROBRIRW] = '20'
LINE_VAR_COUNTER[TCDIAG] = '5'

# how many variables/fields appear after i_value (length of repeat)
LINE_VAR_REPEATS[PCT] = 3
LINE_VAR_REPEATS[PSTD] = 1
LINE_VAR_REPEATS[PJC] = 7
LINE_VAR_REPEATS[PRC] = 3
LINE_VAR_REPEATS[MCTC] = 1
LINE_VAR_REPEATS[RHIST] = 1
LINE_VAR_REPEATS[RELP] = 1
LINE_VAR_REPEATS[PHIST] = 1
LINE_VAR_REPEATS[ORANK] = 1
LINE_VAR_REPEATS[ECLV] = 2
LINE_VAR_REPEATS[PROBRIRW] = 2
LINE_VAR_REPEATS[TCDIAG] = 2

RHIST_OLD = ['V7.0', 'V6.1', 'V6.0', 'V5.2', 'V5.1', 'V5.0',
             'V4.2', 'V4.1', 'V4.0', 'V3.1', 'V3.0']

RHIST_5 = ['V5.2', 'V5.1']
RHIST_6 = ['V6.1', 'V6.0', 'V7.0']

X_POINTS_ECON = [0.952380952, 0.909090909, 0.800000000, 0.666666667, 0.500000000,
                 0.333333333, 0.200000000, 0.125000000, 0.100000000, 0.055555556,
                 0.037037037, 0.025000000, 0.016666667, 0.011111111, 0.007142857,
                 0.004761905, 0.002857143, 0.002000000]

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
TCST = 13

MTD_FILES = [MTD_2D, MTD_3D_PC, MTD_3D_PS, MTD_3D_SC, MTD_3D_SS]

# mode file fields
MODE_HEADER = 'mode_header'
MODE_HEADER_ID = 'mode_header_id'
MODE_OBJ_ID = 'mode_obj_id'
MODE_OBJ_OBS_ID = 'mode_obj_obs_id'
MODE_OBJ_FCST_ID = 'mode_obj_fcst_id'
MODE_CTS_T = 'mode_cts'
MODE_SINGLE_T = 'mode_obj_single'
MODE_PAIR_T = 'mode_obj_pair'
LINE_TYPE_LU_ID = 'line_type_lu_id'
LINENUMBER = 'linenumber'
FCST_VALID = 'fcst_valid'
FCST_ACCUM = 'fcst_accum'
FCST_RAD = 'fcst_rad'
FCST_THR = 'fcst_thr'
FCST_INIT = 'fcst_init'
OBS_VALID = 'obs_valid'
OBS_ACCUM = 'obs_accum'
OBS_RAD = 'obs_rad'
OBS_THR = 'obs_thr'
N_VALID = 'n_valid'
GRID_RES = 'grid_res'
OBJECT_ID = 'object_id'
OBJECT_CAT = 'object_cat'
FCST_FLAG = 'fcst_flag'
SIMPLE_FLAG = 'simple_flag'
MATCHED_FLAG = 'matched_flag'
F_OBJECT_ID = 'f_object_id'
O_OBJECT_ID = 'o_object_id'
F_OBJECT_CAT = 'f_object_cat'
O_OBJECT_CAT = 'o_object_cat'
ASPECT_DIFF = 'aspect_diff'
CURV_RATIO = 'curvature_ratio'
CENTROID_X = 'centroid_x'
CENTROID_Y = 'centroid_y'
CENTROID_LAT = 'centroid_lat'
CENTROID_LON = 'centroid_lon'
INTENSITY_10 = 'intensity_10'
INTENSITY_25 = 'intensity_25'
INTENSITY_50 = 'intensity_50'
INTENSITY_75 = 'intensity_75'
INTENSITY_90 = 'intensity_90'
INTENSITY_NN = 'intensity_nn'

MODE_HEADER_KEYS = [VERSION, MODEL, N_VALID, GRID_RES, DESCR,
                    FCST_LEAD, FCST_VALID, FCST_ACCUM, FCST_INIT,
                    OBS_LEAD, OBS_VALID, OBS_ACCUM, FCST_RAD, FCST_THR,
                    OBS_RAD, OBS_THR, FCST_VAR, FCST_UNITS, FCST_LEV,
                    OBS_VAR, OBS_UNITS, OBS_LEV]

MODE_HEADER_FIELDS = [MODE_HEADER_ID, LINE_TYPE_LU_ID, DATA_FILE_ID,
                      LINENUMBER] + MODE_HEADER_KEYS

MODE_CTS_FIELDS = [MODE_HEADER_ID, 'field', TOTAL_LC, FY_OY, FY_ON, FN_OY, FN_ON,
                   BASER, FMEAN, 'acc', 'fbias', 'pody', 'podn', 'pofd',
                   'far', 'csi', 'gss', 'hk', 'hss', 'odds']

MODE_SINGLE_FIELDS = [MODE_OBJ_ID, MODE_HEADER_ID, OBJECT_ID, OBJECT_CAT,
                      CENTROID_X, CENTROID_Y, CENTROID_LAT, CENTROID_LON,
                      'axis_avg', 'length', 'width', 'area', 'area_thresh',
                      'curvature', 'curvature_x', 'curvature_y', 'complexity',
                      INTENSITY_10, INTENSITY_25, INTENSITY_50, INTENSITY_75,
                      INTENSITY_90, INTENSITY_NN, 'intensity_sum',
                      FCST_FLAG, SIMPLE_FLAG, MATCHED_FLAG]

MODE_PAIR_FIELDS = [MODE_OBJ_OBS_ID, MODE_OBJ_FCST_ID, MODE_HEADER_ID, OBJECT_ID,
                    OBJECT_CAT, 'centroid_dist', 'boundary_dist', 'convex_hull_dist',
                    'angle_diff', ASPECT_DIFF, 'area_ratio',
                    'intersection_area', 'union_area', 'symmetric_diff',
                    'intersection_over_area', CURV_RATIO, 'complexity_ratio',
                    'percentile_intensity_ratio', 'interest', SIMPLE_FLAG, MATCHED_FLAG]

Q_MHEADER = "SELECT mode_header_id FROM mode_header WHERE " + \
            "=%s AND ".join(MODE_HEADER_KEYS) + "=%s"

QN_MHEADER = "SELECT mode_header_id FROM mode_header WHERE " + \
             "version=%s AND model=%s AND n_valid is NULL AND grid_res is NULL " + \
             "AND descr=%s AND fcst_lead=%s AND fcst_valid=%s AND fcst_accum=%s " + \
             "AND fcst_init=%s AND obs_lead=%s AND obs_valid=%s AND obs_accum=%s " + \
             "AND fcst_rad=%s AND fcst_thr=%s AND obs_rad=%s AND obs_thr=%s " + \
             "AND fcst_var=%s AND fcst_units=%s AND fcst_lev=%s AND obs_var=%s " + \
             "AND obs_units=%s AND obs_lev=%s"

INS_MHEADER = "INSERT INTO mode_header (" + ",".join(MODE_HEADER_FIELDS) + \
              ") VALUES (" + VALUE_SLOTS + ")"

C_VALUE_SLOTS = '%s, ' * len(MODE_CTS_FIELDS)
C_VALUE_SLOTS = C_VALUE_SLOTS[:-2]

INS_CHEADER = "INSERT INTO mode_cts (" + ",".join(MODE_CTS_FIELDS) + \
              ") VALUES (" + C_VALUE_SLOTS + ")"

S_VALUE_SLOTS = '%s, ' * len(MODE_SINGLE_FIELDS)
S_VALUE_SLOTS = S_VALUE_SLOTS[:-2]

INS_SHEADER = "INSERT INTO mode_obj_single (" + ",".join(MODE_SINGLE_FIELDS) + \
              ") VALUES (" + S_VALUE_SLOTS + ")"

P_VALUE_SLOTS = '%s, ' * len(MODE_PAIR_FIELDS)
P_VALUE_SLOTS = P_VALUE_SLOTS[:-2]

INS_PHEADER = "INSERT INTO mode_obj_pair (" + ",".join(MODE_PAIR_FIELDS) + \
              ") VALUES (" + P_VALUE_SLOTS + ")"

# MTD file fields
MTD_HEADER = 'mtd_header'
MTD_HEADER_ID = 'mtd_header_id'
REVISION_ID = 'revision_id'
MTD_2D_T = 'mtd_2d_obj'
MTD_SINGLE_T = 'mtd_3d_obj_single'
MTD_PAIR_T = 'mtd_3d_obj_pair'
T_DELTA = 't_delta'
FCST_T_BEG = 'fcst_t_beg'
FCST_T_END = 'fcst_t_end'
OBS_T_BEG = 'obs_t_beg'
OBS_T_END = 'obs_t_end'
TIME_INDEX = 'time_index'
AREA = 'area'
CENTROID_T = 'centroid_t'
AXIS_ANG = 'axis_ang'

MTD_HEADER_KEYS = [VERSION, MODEL, DESCR, FCST_LEAD, FCST_VALID, FCST_INIT,
                   OBS_LEAD, OBS_VALID, T_DELTA, FCST_T_BEG, FCST_T_END, FCST_RAD,
                   FCST_THR,
                   OBS_T_BEG, OBS_T_END, OBS_RAD, OBS_THR, FCST_VAR, FCST_UNITS,
                   FCST_LEV,
                   OBS_VAR, OBS_UNITS, OBS_LEV]

MTD_HEADER_FIELDS = [MTD_HEADER_ID, LINE_TYPE_LU_ID, DATA_FILE_ID,
                     REVISION_ID, LINENUMBER] + MTD_HEADER_KEYS

# MTD 2D files need the revision id to be unique
MTD_2D_HEADER_KEYS = [REVISION_ID] + MTD_HEADER_KEYS

MTD_2D_OBJ_FIELDS = [MTD_HEADER_ID, OBJECT_ID, OBJECT_CAT, TIME_INDEX, AREA,
                     CENTROID_X, CENTROID_Y, CENTROID_LAT, CENTROID_LON, AXIS_ANG,
                     INTENSITY_10, INTENSITY_25, INTENSITY_50, INTENSITY_75,
                     INTENSITY_90, INTENSITY_NN, FCST_FLAG, SIMPLE_FLAG, MATCHED_FLAG]

# Columns that need to be filled with the identical value to be a 2D revision file
MTD_2D_REV_FIELDS = [MODEL, DESCR, FCST_VALID, OBS_VALID, FCST_RAD, FCST_THR,
                     OBS_RAD, OBS_THR, FCST_LEV, OBS_LEV]

MTD_3D_OBJ_SINGLE_FIELDS = [MTD_HEADER_ID, OBJECT_ID, OBJECT_CAT,
                            CENTROID_X, CENTROID_Y, CENTROID_T, CENTROID_LAT,
                            CENTROID_LON,
                            'x_dot', 'y_dot', AXIS_ANG, 'volume',
                            'start_time', 'end_time', 'cdist_travelled',
                            INTENSITY_10, INTENSITY_25, INTENSITY_50, INTENSITY_75,
                            INTENSITY_90, INTENSITY_NN, FCST_FLAG, SIMPLE_FLAG,
                            MATCHED_FLAG]

MTD_3D_OBJ_PAIR_FIELDS = [MTD_HEADER_ID, OBJECT_ID, OBJECT_CAT,
                          'space_centroid_dist', 'time_centroid_delta', 'axis_diff',
                          'speed_delta', 'direction_diff', 'volume_ratio',
                          'start_time_delta', 'end_time_delta',
                          'intersection_volume', 'duration_diff', 'interest',
                          SIMPLE_FLAG, MATCHED_FLAG]

Q_MTDHEADER = "SELECT mtd_header_id FROM mtd_header WHERE " + \
              "=%s AND ".join(MTD_HEADER_KEYS) + "=%s"

INS_MTDHEADER = "INSERT INTO mtd_header (" + ",".join(MTD_HEADER_FIELDS) + \
                ") VALUES (" + VALUE_SLOTS + ")"

C_VALUE_SLOTS = '%s, ' * len(MTD_2D_OBJ_FIELDS)
C_VALUE_SLOTS = C_VALUE_SLOTS[:-2]

INS_M2HEADER = "INSERT INTO mtd_2d_obj (" + ",".join(MTD_2D_OBJ_FIELDS) + \
               ") VALUES (" + C_VALUE_SLOTS + ")"

S_VALUE_SLOTS = '%s, ' * len(MTD_3D_OBJ_SINGLE_FIELDS)
S_VALUE_SLOTS = S_VALUE_SLOTS[:-2]

INS_M3SHEADER = "INSERT INTO mtd_3d_obj_single (" + ",".join(
    MTD_3D_OBJ_SINGLE_FIELDS) + \
                ") VALUES (" + S_VALUE_SLOTS + ")"

P_VALUE_SLOTS = '%s, ' * len(MTD_3D_OBJ_PAIR_FIELDS)
P_VALUE_SLOTS = P_VALUE_SLOTS[:-2]

INS_M3PHEADER = "INSERT INTO mtd_3d_obj_pair (" + ",".join(MTD_3D_OBJ_PAIR_FIELDS) + \
                ") VALUES (" + P_VALUE_SLOTS + ")"

DROP_INDEXES_QUERIES = ["DROP INDEX stat_header_model_idx ON stat_header",
                        "DROP INDEX stat_header_fcst_var_idx ON stat_header",
                        "DROP INDEX stat_header_fcst_lev_idx ON stat_header",
                        "DROP INDEX stat_header_obtype_idx ON stat_header",
                        "DROP INDEX stat_header_vx_mask_idx ON stat_header",
                        "DROP INDEX stat_header_interp_mthd_idx ON stat_header",
                        "DROP INDEX stat_header_interp_pnts_idx ON stat_header",
                        "DROP INDEX stat_header_fcst_thresh_idx ON stat_header",
                        "DROP INDEX mode_header_model_idx ON mode_header",
                        "DROP INDEX mode_header_fcst_lead_idx ON mode_header",
                        "DROP INDEX mode_header_fcst_valid_idx ON mode_header",
                        "DROP INDEX mode_header_fcst_init_idx ON mode_header",
                        "DROP INDEX mode_header_fcst_rad_idx ON mode_header",
                        "DROP INDEX mode_header_fcst_thr_idx ON mode_header",
                        "DROP INDEX mode_header_fcst_var_idx ON mode_header",
                        "DROP INDEX mode_header_fcst_lev_idx ON mode_header",
                        "DROP INDEX mtd_header_model_idx ON mtd_header",
                        "DROP INDEX mtd_header_fcst_lead_idx ON mtd_header",
                        "DROP INDEX mtd_header_fcst_valid_idx ON mtd_header",
                        "DROP INDEX mtd_header_fcst_init_idx ON mtd_header",
                        "DROP INDEX mtd_header_fcst_rad_idx ON mtd_header",
                        "DROP INDEX mtd_header_fcst_thr_idx ON mtd_header",
                        "DROP INDEX mtd_header_fcst_var_idx ON mtd_header",
                        "DROP INDEX mtd_header_fcst_lev_idx ON mtd_header",
                        "DROP INDEX line_data_fho_fcst_lead_idx ON line_data_fho",
                        "DROP INDEX line_data_fho_fcst_valid_beg_idx ON line_data_fho",
                        "DROP INDEX line_data_fho_fcst_init_beg_idx ON line_data_fho",
                        "DROP INDEX line_data_ctc_fcst_lead_idx ON line_data_ctc",
                        "DROP INDEX line_data_ctc_fcst_valid_beg_idx ON line_data_ctc",
                        "DROP INDEX line_data_ctc_fcst_init_beg_idx ON line_data_ctc",
                        "DROP INDEX line_data_cts_fcst_lead_idx ON line_data_cts",
                        "DROP INDEX line_data_cts_fcst_valid_beg_idx ON line_data_cts",
                        "DROP INDEX line_data_cts_fcst_init_beg_idx ON line_data_cts",
                        "DROP INDEX line_data_cnt_fcst_lead_idx ON line_data_cnt",
                        "DROP INDEX line_data_cnt_fcst_valid_beg_idx ON line_data_cnt",
                        "DROP INDEX line_data_cnt_fcst_init_beg_idx ON line_data_cnt",
                        "DROP INDEX line_data_pct_fcst_lead_idx ON line_data_pct",
                        "DROP INDEX line_data_pct_fcst_valid_beg_idx ON line_data_pct",
                        "DROP INDEX line_data_pct_fcst_init_beg_idx ON line_data_pct",
                        "DROP INDEX line_data_pstd_fcst_lead_idx ON line_data_pstd",
                        "DROP INDEX line_data_pstd_fcst_valid_beg_idx ON "
                        "line_data_pstd",
                        "DROP INDEX line_data_pstd_fcst_init_beg_idx ON line_data_pstd",
                        "DROP INDEX line_data_pjc_fcst_lead_idx ON line_data_pjc",
                        "DROP INDEX line_data_pjc_fcst_valid_beg_idx ON line_data_pjc",
                        "DROP INDEX line_data_pjc_fcst_init_beg_idx ON line_data_pjc",
                        "DROP INDEX line_data_prc_fcst_lead_idx ON line_data_prc",
                        "DROP INDEX line_data_prc_fcst_valid_beg_idx ON line_data_prc",
                        "DROP INDEX line_data_prc_fcst_init_beg_idx ON line_data_prc",
                        "DROP INDEX line_data_sl1l2_fcst_lead_idx ON line_data_sl1l2",
                        "DROP INDEX line_data_sl1l2_fcst_valid_beg_idx ON "
                        "line_data_sl1l2",
                        "DROP INDEX line_data_sl1l2_fcst_init_beg_idx ON "
                        "line_data_sl1l2",
                        "DROP INDEX line_data_sal1l2_fcst_lead_idx ON line_data_sal1l2",
                        "DROP INDEX line_data_sal1l2_fcst_valid_beg_idx ON "
                        "line_data_sal1l2",
                        "DROP INDEX line_data_sal1l2_fcst_init_beg_idx ON "
                        "line_data_sal1l2",
                        "DROP INDEX line_data_vl1l2_fcst_lead_idx ON line_data_vl1l2",
                        "DROP INDEX line_data_vl1l2_fcst_valid_beg_idx ON "
                        "line_data_vl1l2",
                        "DROP INDEX line_data_vl1l2_fcst_init_beg_idx ON "
                        "line_data_vl1l2",
                        "DROP INDEX line_data_val1l2_fcst_lead_idx ON line_data_val1l2",
                        "DROP INDEX line_data_val1l2_fcst_valid_beg_idx ON "
                        "line_data_val1l2",
                        "DROP INDEX line_data_val1l2_fcst_init_beg_idx ON "
                        "line_data_val1l2",
                        "DROP INDEX line_data_mpr_fcst_lead_idx ON line_data_mpr",
                        "DROP INDEX line_data_mpr_fcst_valid_beg_idx ON line_data_mpr",
                        "DROP INDEX line_data_mpr_fcst_init_beg_idx ON line_data_mpr",
                        "DROP INDEX line_data_nbrctc_fcst_lead_idx ON line_data_nbrctc",
                        "DROP INDEX line_data_nbrctc_fcst_valid_beg_idx ON "
                        "line_data_nbrctc",
                        "DROP INDEX line_data_nbrctc_fcst_init_beg_idx ON "
                        "line_data_nbrctc",
                        "DROP INDEX line_data_nbrcts_fcst_lead_idx ON line_data_nbrcts",
                        "DROP INDEX line_data_nbrcts_fcst_valid_beg_idx ON "
                        "line_data_nbrcts",
                        "DROP INDEX line_data_nbrcts_fcst_init_beg_idx ON "
                        "line_data_nbrcts",
                        "DROP INDEX line_data_nbrcnt_fcst_lead_idx ON line_data_nbrcnt",
                        "DROP INDEX line_data_nbrcnt_fcst_valid_beg_idx ON "
                        "line_data_nbrcnt",
                        "DROP INDEX line_data_nbrcnt_fcst_init_beg_idx ON "
                        "line_data_nbrcnt",
                        "DROP INDEX line_data_isc_fcst_lead_idx ON line_data_isc",
                        "DROP INDEX line_data_isc_fcst_valid_beg_idx ON line_data_isc",
                        "DROP INDEX line_data_isc_fcst_init_beg_idx ON line_data_isc",
                        "DROP INDEX line_data_mctc_fcst_lead_idx ON line_data_mctc",
                        "DROP INDEX line_data_mctc_fcst_valid_beg_idx ON "
                        "line_data_mctc",
                        "DROP INDEX line_data_mctc_fcst_init_beg_idx ON line_data_mctc",
                        "DROP INDEX line_data_rhist_fcst_lead_idx ON line_data_rhist",
                        "DROP INDEX line_data_rhist_fcst_valid_beg_idx ON "
                        "line_data_rhist",
                        "DROP INDEX line_data_rhist_fcst_init_beg_idx ON "
                        "line_data_rhist",
                        "DROP INDEX line_data_orank_fcst_lead_idx ON line_data_orank",
                        "DROP INDEX line_data_orank_fcst_valid_beg_idx ON "
                        "line_data_orank",
                        "DROP INDEX line_data_orank_fcst_init_beg_idx ON "
                        "line_data_orank",
                        "DROP INDEX line_data_relp_fcst_lead_idx ON line_data_relp",
                        "DROP INDEX line_data_relp_fcst_valid_beg_idx ON "
                        "line_data_relp",
                        "DROP INDEX line_data_relp_fcst_init_beg_idx ON line_data_relp",
                        "DROP INDEX line_data_eclv_fcst_lead_idx ON line_data_eclv",
                        "DROP INDEX line_data_eclv_fcst_valid_beg_idx ON "
                        "line_data_eclv",
                        "DROP INDEX line_data_eclv_fcst_init_beg_idx ON line_data_eclv",
                        "DROP INDEX line_data_ssvar_fcst_lead_idx ON line_data_ssvar",
                        "DROP INDEX line_data_ssvar_fcst_valid_beg_idx ON "
                        "line_data_ssvar",
                        "DROP INDEX line_data_ssvar_fcst_init_beg_idx ON "
                        "line_data_ssvar",
                        "DROP INDEX line_data_enscnt_fcst_lead_idx ON line_data_enscnt",
                        "DROP INDEX line_data_enscnt_fcst_valid_beg_idx ON "
                        "line_data_enscnt",
                        "DROP INDEX line_data_enscnt_fcst_init_beg_idx ON "
                        "line_data_enscnt",
                        "DROP INDEX line_data_grad_fcst_lead_idx ON line_data_grad",
                        "DROP INDEX line_data_grad_fcst_valid_beg_idx ON "
                        "line_data_grad",
                        "DROP INDEX line_data_grad_fcst_init_beg_idx ON line_data_grad",
                        "DROP INDEX line_data_dmap_fcst_lead_idx ON line_data_dmap",
                        "DROP INDEX line_data_dmap_fcst_valid_beg_idx ON "
                        "line_data_dmap",
                        "DROP INDEX line_data_dmap_fcst_init_beg_idx ON line_data_dmap",
                        "DROP INDEX line_data_rps_fcst_lead_idx ON line_data_rps",
                        "DROP INDEX line_data_rps_fcst_valid_beg_idx ON line_data_rps",
                        "DROP INDEX line_data_rps_fcst_init_beg_idx ON line_data_rps",
                        "DROP INDEX line_data_ssidx_fcst_lead_idx ON line_data_ssidx",
                        "DROP INDEX line_data_ssidx_fcst_valid_beg_idx ON "
                        "line_data_ssidx",
                        "DROP INDEX line_data_ssidx_fcst_init_beg_idx ON "
                        "line_data_ssidx"]

CREATE_INDEXES_QUERIES = \
    ["CREATE INDEX stat_header_model_idx ON stat_header (model)",
     "CREATE INDEX stat_header_fcst_var_idx ON stat_header (fcst_var)",
     "CREATE INDEX stat_header_fcst_lev_idx ON stat_header (fcst_lev)",
     "CREATE INDEX stat_header_obtype_idx ON stat_header (obtype)",
     "CREATE INDEX stat_header_vx_mask_idx ON stat_header (vx_mask)",
     "CREATE INDEX stat_header_interp_mthd_idx ON stat_header (interp_mthd)",
     "CREATE INDEX stat_header_interp_pnts_idx ON stat_header (interp_pnts)",
     "CREATE INDEX stat_header_fcst_thresh_idx ON stat_header (fcst_thresh)",
     "CREATE INDEX mode_header_model_idx ON mode_header (model)",
     "CREATE INDEX mode_header_fcst_lead_idx ON mode_header (fcst_lead)",
     "CREATE INDEX mode_header_fcst_valid_idx ON mode_header (fcst_valid)",
     "CREATE INDEX mode_header_fcst_init_idx ON mode_header (fcst_init)",
     "CREATE INDEX mode_header_fcst_rad_idx ON mode_header (fcst_rad)",
     "CREATE INDEX mode_header_fcst_thr_idx ON mode_header (fcst_thr)",
     "CREATE INDEX mode_header_fcst_var_idx ON mode_header (fcst_var)",
     "CREATE INDEX mode_header_fcst_lev_idx ON mode_header (fcst_lev)",
     "CREATE INDEX mtd_header_model_idx ON mtd_header (model)",
     "CREATE INDEX mtd_header_fcst_lead_idx ON mtd_header (fcst_lead)",
     "CREATE INDEX mtd_header_fcst_valid_idx ON mtd_header (fcst_valid)",
     "CREATE INDEX mtd_header_fcst_init_idx ON mtd_header (fcst_init)",
     "CREATE INDEX mtd_header_fcst_rad_idx ON mtd_header (fcst_rad)",
     "CREATE INDEX mtd_header_fcst_thr_idx ON mtd_header (fcst_thr)",
     "CREATE INDEX mtd_header_fcst_var_idx ON mtd_header (fcst_var)",
     "CREATE INDEX mtd_header_fcst_lev_idx ON mtd_header (fcst_lev)",
     "CREATE INDEX line_data_fho_fcst_lead_idx ON line_data_fho (fcst_lead)",
     "CREATE INDEX line_data_fho_fcst_valid_beg_idx ON line_data_fho (fcst_valid_beg)",
     "CREATE INDEX line_data_fho_fcst_init_beg_idx ON line_data_fho (fcst_init_beg)",
     "CREATE INDEX line_data_ctc_fcst_lead_idx ON line_data_ctc (fcst_lead)",
     "CREATE INDEX line_data_ctc_fcst_valid_beg_idx ON line_data_ctc (fcst_valid_beg)",
     "CREATE INDEX line_data_ctc_fcst_init_beg_idx ON line_data_ctc (fcst_init_beg)",
     "CREATE INDEX line_data_cts_fcst_lead_idx ON line_data_cts (fcst_lead)",
     "CREATE INDEX line_data_cts_fcst_valid_beg_idx ON line_data_cts (fcst_valid_beg)",
     "CREATE INDEX line_data_cts_fcst_init_beg_idx ON line_data_cts (fcst_init_beg)",
     "CREATE INDEX line_data_cnt_fcst_lead_idx ON line_data_cnt (fcst_lead)",
     "CREATE INDEX line_data_cnt_fcst_valid_beg_idx ON line_data_cnt (fcst_valid_beg)",
     "CREATE INDEX line_data_cnt_fcst_init_beg_idx ON line_data_cnt (fcst_init_beg)",
     "CREATE INDEX line_data_pct_fcst_lead_idx ON line_data_pct (fcst_lead)",
     "CREATE INDEX line_data_pct_fcst_valid_beg_idx ON line_data_pct (fcst_valid_beg)",
     "CREATE INDEX line_data_pct_fcst_init_beg_idx ON line_data_pct (fcst_init_beg)",
     "CREATE INDEX line_data_pstd_fcst_lead_idx ON line_data_pstd (fcst_lead)",
     "CREATE INDEX line_data_pstd_fcst_valid_beg_idx ON line_data_pstd ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_pstd_fcst_init_beg_idx ON line_data_pstd (fcst_init_beg)",
     "CREATE INDEX line_data_pjc_fcst_lead_idx ON line_data_pjc (fcst_lead)",
     "CREATE INDEX line_data_pjc_fcst_valid_beg_idx ON line_data_pjc (fcst_valid_beg)",
     "CREATE INDEX line_data_pjc_fcst_init_beg_idx ON line_data_pjc (fcst_init_beg)",
     "CREATE INDEX line_data_prc_fcst_lead_idx ON line_data_prc (fcst_lead)",
     "CREATE INDEX line_data_prc_fcst_valid_beg_idx ON line_data_prc (fcst_valid_beg)",
     "CREATE INDEX line_data_prc_fcst_init_beg_idx ON line_data_prc (fcst_init_beg)",
     "CREATE INDEX line_data_sl1l2_fcst_lead_idx ON line_data_sl1l2 (fcst_lead)",
     "CREATE INDEX line_data_sl1l2_fcst_valid_beg_idx ON line_data_sl1l2 ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_sl1l2_fcst_init_beg_idx ON line_data_sl1l2 ("
     "fcst_init_beg)",
     "CREATE INDEX line_data_sal1l2_fcst_lead_idx ON line_data_sal1l2 (fcst_lead)",
     "CREATE INDEX line_data_sal1l2_fcst_valid_beg_idx ON line_data_sal1l2 ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_sal1l2_fcst_init_beg_idx ON line_data_sal1l2 ("
     "fcst_init_beg)",
     "CREATE INDEX line_data_vl1l2_fcst_lead_idx ON line_data_vl1l2 (fcst_lead)",
     "CREATE INDEX line_data_vl1l2_fcst_valid_beg_idx ON line_data_vl1l2 ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_vl1l2_fcst_init_beg_idx ON line_data_vl1l2 ("
     "fcst_init_beg)",
     "CREATE INDEX line_data_val1l2_fcst_lead_idx ON line_data_val1l2 (fcst_lead)",
     "CREATE INDEX line_data_val1l2_fcst_valid_beg_idx ON line_data_val1l2 ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_val1l2_fcst_init_beg_idx ON line_data_val1l2 ("
     "fcst_init_beg)",
     "CREATE INDEX line_data_mpr_fcst_lead_idx ON line_data_mpr (fcst_lead)",
     "CREATE INDEX line_data_mpr_fcst_valid_beg_idx ON line_data_mpr (fcst_valid_beg)",
     "CREATE INDEX line_data_mpr_fcst_init_beg_idx ON line_data_mpr (fcst_init_beg)",
     "CREATE INDEX line_data_nbrctc_fcst_lead_idx ON line_data_nbrctc (fcst_lead)",
     "CREATE INDEX line_data_nbrctc_fcst_valid_beg_idx ON line_data_nbrctc ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_nbrctc_fcst_init_beg_idx ON line_data_nbrctc ("
     "fcst_init_beg)",
     "CREATE INDEX line_data_nbrcts_fcst_lead_idx ON line_data_nbrcts (fcst_lead)",
     "CREATE INDEX line_data_nbrcts_fcst_valid_beg_idx ON line_data_nbrcts ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_nbrcts_fcst_init_beg_idx ON line_data_nbrcts ("
     "fcst_init_beg)",
     "CREATE INDEX line_data_nbrcnt_fcst_lead_idx ON line_data_nbrcnt (fcst_lead)",
     "CREATE INDEX line_data_nbrcnt_fcst_valid_beg_idx ON line_data_nbrcnt ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_nbrcnt_fcst_init_beg_idx ON line_data_nbrcnt ("
     "fcst_init_beg)",
     "CREATE INDEX line_data_isc_fcst_lead_idx ON line_data_isc (fcst_lead)",
     "CREATE INDEX line_data_isc_fcst_valid_beg_idx ON line_data_isc (fcst_valid_beg)",
     "CREATE INDEX line_data_isc_fcst_init_beg_idx ON line_data_isc (fcst_init_beg)",
     "CREATE INDEX line_data_mctc_fcst_lead_idx ON line_data_mctc (fcst_lead)",
     "CREATE INDEX line_data_mctc_fcst_valid_beg_idx ON line_data_mctc ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_mctc_fcst_init_beg_idx ON line_data_mctc (fcst_init_beg)",
     "CREATE INDEX line_data_rhist_fcst_lead_idx ON line_data_rhist (fcst_lead)",
     "CREATE INDEX line_data_rhist_fcst_valid_beg_idx ON line_data_rhist ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_rhist_fcst_init_beg_idx ON line_data_rhist ("
     "fcst_init_beg)",
     "CREATE INDEX line_data_orank_fcst_lead_idx ON line_data_orank (fcst_lead)",
     "CREATE INDEX line_data_orank_fcst_valid_beg_idx ON line_data_orank ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_orank_fcst_init_beg_idx ON line_data_orank ("
     "fcst_init_beg)",
     "CREATE INDEX line_data_relp_fcst_lead_idx ON line_data_relp (fcst_lead)",
     "CREATE INDEX line_data_relp_fcst_valid_beg_idx ON line_data_relp ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_relp_fcst_init_beg_idx ON line_data_relp (fcst_init_beg)",
     "CREATE INDEX line_data_eclv_fcst_lead_idx ON line_data_eclv (fcst_lead)",
     "CREATE INDEX line_data_eclv_fcst_valid_beg_idx ON line_data_eclv ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_eclv_fcst_init_beg_idx ON line_data_eclv (fcst_init_beg)",
     "CREATE INDEX line_data_ssvar_fcst_lead_idx ON line_data_ssvar (fcst_lead)",
     "CREATE INDEX line_data_ssvar_fcst_valid_beg_idx ON line_data_ssvar ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_ssvar_fcst_init_beg_idx ON line_data_ssvar ("
     "fcst_init_beg)",
     "CREATE INDEX line_data_enscnt_fcst_lead_idx ON line_data_enscnt (fcst_lead)",
     "CREATE INDEX line_data_enscnt_fcst_valid_beg_idx ON line_data_enscnt ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_enscnt_fcst_init_beg_idx ON line_data_enscnt ("
     "fcst_init_beg)",
     "CREATE INDEX line_data_grad_fcst_lead_idx ON line_data_grad (fcst_lead)",
     "CREATE INDEX line_data_grad_fcst_valid_beg_idx ON line_data_grad ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_grad_fcst_init_beg_idx ON line_data_grad (fcst_init_beg)",
     "CREATE INDEX line_data_dmap_fcst_lead_idx ON line_data_dmap (fcst_lead)",
     "CREATE INDEX line_data_dmap_fcst_valid_beg_idx ON line_data_dmap ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_dmap_fcst_init_beg_idx ON line_data_dmap (fcst_init_beg)",
     "CREATE INDEX line_data_rps_fcst_lead_idx ON line_data_rps (fcst_lead)",
     "CREATE INDEX line_data_rps_fcst_valid_beg_idx ON line_data_rps (fcst_valid_beg)",
     "CREATE INDEX line_data_rps_fcst_init_beg_idx ON line_data_rps (fcst_init_beg)",
     "CREATE INDEX line_data_ssidx_fcst_lead_idx ON line_data_ssidx (fcst_lead)",
     "CREATE INDEX line_data_ssidx_fcst_valid_beg_idx ON line_data_ssidx ("
     "fcst_valid_beg)",
     "CREATE INDEX line_data_ssidx_fcst_init_beg_idx ON line_data_ssidx ("
     "fcst_init_beg)"]

#
# Used by MET reformatter
# 1 more than the MET User's Guide to
# account for the FCST_INIT_BEG
#

NUM_STAT_FHO_COLS = 29
NUM_STAT_CNT_COLS = 125
NUM_STAT_ECNT_COLS = 52
NUM_STAT_VCNT_COLS = 99
NUM_STAT_CTC_COLS = 31
NUM_STAT_SL1L2_COLS = 32
NUM_STAT_SAL1L2_COLS = 32
NUM_STAT_VL1L2_COLS = 38
NUM_STAT_CTS_COLS = 122
NUM_STAT_MCTC_COLS = 28
NUM_STAT_MCTS_COLS = 45
NUM_STAT_MPR_COLS = 40
# Number of columns BEFORE the "variable" fields (i.e. VERSION, MODEL, ..., LINETYPE), including the
# FCST_INIT_BEG column .
# For PCT the "variable" fields are N_THRESH, THRESH_i, OY_i, ON_i,..., THRESH_N, where the
# number of THRESH_i, OY_i, and ON_i depend on the number of thresholds specified by the
# N_THRESH value.
NUM_STATIC_PCT_COLS = 26

# Number of columns BEFORE the "variable" fields (i.e. the VERSION, MODEL, ..., LINETYPE, TOTAL,
# INDEX precede the variable columns DIAG_i and VALUE_i).
# The "variable" fields are DIAG_i and VALUE_i, the number of these is dictated by the N_DIAG
# value.
NUM_STATIC_TCDIAG_COLS = 20

# Number of columns before the RANK_i variable columns (i.e. VERSION, MODEL,...,LINETYPE, TOTAL, N_RANK), including
# the FCST_INIT_BEG and TOTAL columns.
NUM_STATIC_RHIST_COLS = 26


DESC = "desc"
COMMON_STAT_HEADER = [VERSION, MODEL, DESC, FCST_LEAD, FCST_VALID_BEG, FCST_VALID_END,
                      FCST_INIT_BEG, OBS_LEAD, OBS_VALID_BEG,
                      OBS_VALID_END, FCST_VAR, FCST_UNITS, FCST_LEV, OBS_VAR, OBS_UNITS,
                      OBS_LEV, OBTYPE, VX_MASK,
                      INTERP_MTHD, INTERP_PNTS, FCST_THRESH, OBS_THRESH, COV_THRESH,
                      ALPHA, LINE_TYPE]
LC_COMMON_STAT_HEADER = [cur_stat_header.lower() for cur_stat_header in
                         COMMON_STAT_HEADER]

#
# POINT STAT/GRID STAT/ ENSEMBLE STAT linetypes, used in METreformat
#


#### FHO Line type ####
FHO_FULL_HEADER = LC_COMMON_STAT_HEADER + ['total', 'F_RATE', 'H_RATE', 'O_RATE']

###### CNT line type
LC_CNT_SPECIFIC = ['fbar', 'fbar_ncl', 'fbar_ncu', 'fbar_bcl', 'fbar_bcu',
                   'fstdev', 'fstdev_ncl', 'fstdev_ncu', 'fstdev_bcl', 'fstdev_bcu',
                   'obar', 'obar_ncl', 'obar_ncu', 'obar_bcl', 'obar_bcu',
                   'ostdev', 'ostdev_ncl', 'ostdev_ncu', 'ostdev_bcl', 'ostdev_bcu',
                   'pr_corr', 'pr_corr_ncl', 'pr_corr_ncu', 'pr_corr_bcl',
                   'pr_corr_bcu',
                   'sp_corr', 'kt_corr', 'ranks', 'frank_ties', 'orank_ties',
                   'me', 'me_ncl', 'me_ncu', 'me_bcl', 'me_bcu',
                   'estdev', 'estdev_ncl', 'estdev_ncu', 'estdev_bcl', 'estdev_bcu',
                   'mbias', 'mbias_bcl', 'mbias_bcu', 'mae', 'mae_bcl', 'mae_bcu',
                   'mse', 'mse_bcl', 'mse_bcu', 'bcmse', 'bcmse_bcl', 'bcmse_bcu',
                   'rmse', 'rmse_bcl', 'rmse_bcu', 'e10', 'e10_bcl', 'e10_bcu',
                   'e25', 'e25_bcl', 'e25_bcu', 'e50', 'e50_bcl', 'e50_bcu',
                   'e75', 'e75_bcl', 'e75_bcu', 'e90', 'e90_bcl', 'e90_bcu',
                   'eiqr', 'iqr_bcl', 'iqr_bcu', 'mad', 'mad_bcl', 'mad_bcu',
                   'anom_corr', 'anom_corr_ncl', 'anom_corr_ncu',
                   'anom_corr_bcl', 'anom_corr_bcu',
                   'me2', 'me2_bcl', 'me2_bcu', 'msess', 'msess_bcl', 'msess_bcu',
                   'rmsfa', 'rmsfa_bcl', 'rmsfa_bcu', 'rmsoa', 'rmsoa_bcl', 'rmsoa_bcu',
                   'anom_corr_uncntr', 'anom_corr_uncntr_bcl', 'anom_corr_uncntr_bcu',
                   'si', 'si_bcl', 'si_bcu']
CNT_SPECIFIC = [cur_stat_header.upper() for cur_stat_header in LC_CNT_SPECIFIC]
FULL_CNT_HEADER = LC_COMMON_STAT_HEADER + ['total'] + CNT_SPECIFIC
LC_CNT_STATISTICS_HEADERS = ['fbar',
                             'fstdev',
                             'obar',
                             'ostdev',
                             'pr_corr',
                             'sp_corr', 'kt_corr', 'ranks', 'frank_ties', 'orank_ties',
                             'me',
                             'estdev',
                             'mbias', 'mae',
                             'mse', 'bcmse',
                             'rmse', 'e10',
                             'e25', 'e50',
                             'e75', 'e90',
                             'eiqr', 'mad',
                             'anom_corr',
                             'me2', 'msess',
                             'rmsfa', 'rmsoa',
                             'anom_corr_uncntr',
                             'si']
CNT_STATISTICS_HEADERS = [cur_stat_header.upper() for cur_stat_header in
                          LC_CNT_STATISTICS_HEADERS]
LC_CNT_BOOTSTRAP_HEADERS = ['fbar_ncl', 'fbar_ncu', 'fbar_bcl', 'fbar_bcu',
                            'fstdev_ncl', 'fstdev_ncu', 'fstdev_bcl', 'fstdev_bcu',
                            'obar_ncl', 'obar_ncu', 'obar_bcl', 'obar_bcu',
                            'ostdev_ncl', 'ostdev_ncu', 'ostdev_bcl', 'ostdev_bcu',
                            'pr_corr_ncl', 'pr_corr_ncu', 'pr_corr_bcl', 'pr_corr_bcu',
                            'me_ncl', 'me_ncu', 'me_bcl', 'me_bcu',
                            'estdev_ncl', 'estdev_ncu', 'estdev_bcl', 'estdev_bcu',
                            'mbias_bcl', 'mbias_bcu', 'mae_bcl', 'mae_bcu',
                            'mse_bcl', 'mse_bcu', 'bcmse_bcl', 'bcmse_bcu',
                            'rmse_bcl', 'rmse_bcu', 'e10_bcl', 'e10_bcu',
                            'e25_bcl', 'e25_bcu', 'e50_bcl', 'e50_bcu',
                            'e75_bcl', 'e75_bcu', 'e90_bcl', 'e90_bcu',
                            'iqr_bcl', 'iqr_bcu', 'mad_bcl', 'mad_bcu',
                            'anom_corr_ncl', 'anom_corr_ncu',
                            'anom_corr_bcl', 'anom_corr_bcu',
                            'me2_bcl', 'me2_bcu', 'msess_bcl', 'msess_bcu',
                            'rmsfa_bcl', 'rmsfa_bcu', 'rmsoa_bcl', 'rmsoa_bcu',
                            'anom_corr_uncntr_bcl', 'anom_corr_uncntr_bcu',
                            'si_bcl', 'si_bcu']
CNT_BOOTSTRAP_HEADERS = [cur_stat_header.upper() for cur_stat_header in
                         LC_CNT_BOOTSTRAP_HEADERS]

# Column headers for the CNT line type's bootstrap confidence levels

CNT_NCL_HEADERS = ['FBAR_NCL', 'FSTDEV_NCL', 'OBAR_NCL', 'OSTDEV_NCL', 'PR_CORR_NCL',
                   'ME_NCL', 'ESTDEV_NCL',
                   'ANOM_CORR_NCL']

CNT_NCU_HEADERS = ['FBAR_NCU', 'FSTDEV_NCU', 'OBAR_NCU', 'OSTDEV_NCU', 'PR_CORR_NCU',
                   'ME_NCU', 'ESTDEV_NCU',
                   'ANOM_CORR_NCU']

CNT_BCL_HEADERS = ['FBAR_BCL', 'FSTDEV_BCL', 'OBAR_BCL', 'OSTDEV_BCL', 'PR_CORR_BCL',
                   'ME_BCL', 'ESTDEV_BCL',
                   'MBIAS_BCL', 'MAE_BCL', 'MSE_BCL', 'BCMSE_BCL', 'RMSE_BCL',
                   'E10_BCL', 'E25_BCL', 'E50_BCL',
                   'E75_BCL', 'E90_BCL', 'IQR_BCL', 'MAD_BCL', 'ANOM_CORR_BCL',
                   'ME2_BCL', 'MSESS_BCL',
                   'RMSFA_BCL', 'RMSOA_BCL', 'ANOM_CORR_UNCNTR_BCL', 'SI_BCL']

CNT_BCU_HEADERS = ['FBAR_BCU', 'FSTDEV_BCU', 'OBAR_BCU', 'OSTDEV_BCU', 'PR_CORR_BCU',
                   'ME_BCU', 'ESTDEV_BCU',
                   'MBIAS_BCU', 'MAE_BCU', 'MSE_BCU', 'BCMSE_BCU', 'RMSE_BCU',
                   'E10_BCU', 'E25_BCU', 'E50_BCU',
                   'E75_BCU', 'E90_BCU', 'IQR_BCU', 'MAD_BCU', 'ANOM_CORR_BCU',
                   'ME2_BCU', 'MSESS_BCU',
                   'RMSFA_BCU', 'RMSOA_BCU', 'ANOM_CORR_UNCNTR_BCU', 'SI_BCU']

# Column headers for the ECNT line type

LC_ECNT_SPECIFIC = ['n_ens', 'crps', 'crpss', 'ign', 'me', 'rmse', 'spread',
                          'me_oerr', 'rmse_oerr', 'spread_oerr', 'spread_plus_oerr',
                          'crpscl', 'crps_emp', 'crpscl_emp', 'crpss_emp',
                          'crps_emp_fair', 'spread_md', 'mae', 'mae_oerr',
                          'bias_ratio', 'n_ge_obs', 'me_ge_obs',
                          'n_lt_obs', 'me_lt_obs', 'ign_conv_oerr', 'ign_corr_oerr']
ECNT_STATISTICS_HEADERS = [cur_stat_header.upper() for cur_stat_header in
                         LC_ECNT_SPECIFIC]
ECNT_HEADERS = LC_COMMON_STAT_HEADER + ['total'] + ECNT_STATISTICS_HEADERS



###### VCNT line type
LC_VCNT_SPECIFIC = ['fbar', 'fbar_bcl', 'fbar_bcu',
                   'obar', 'obar_bcl', 'obar_bcu',
                   'fs_rms', 'fs_rms_bcl', 'fs_rms_bcu',
                    'os_rms', 'os_rms_bcl', 'os_rms_bcu',
                    'msve', 'msve_bcl', 'msve_bcu',
                    'rmsve', 'rmsve_bcl', 'rmsve_bcu',
                    'fstdev', 'fstdev_bcl', 'fstdev_bcu',
                    'ostdev', 'ostdev_bcl', 'ostdev_bcu',
                    'fdir', 'fdir_bcl', 'fdir_bcu',
                    'odir', 'odir_bcl', 'odir_bcu',
                    'fbar_speed', 'fbar_speed_bcl', 'fbar_speed_bcu'
                    'obar_speed', 'obar_speed_bcl', 'obar_speed_bcu',
                    'vdiff_speed', 'vdiff_speed_bcl', 'vdiff_speed_bcu',
                    'vdiff_dir', 'vdiff_dir_bcl', 'vdiff_dir_bcu',
                    'speed_err', 'speed_err_bcl', 'speed_err_bcu',
                    'speed_abserr', 'speed_abserr_bcl', 'speed_abserr_bcu',
                    'dir_err', 'dir_err_bcl', 'dir_err_bcu',
                    'dir_abserr', 'dir_abserr_bcl', 'dir_abserr_bcu',
                    'anom_corr', 'anom_corr_ncl', 'anom_corr_ncu', 'anom_corr_bcl',
                    'anom_corr_bcu',
                    'anom_corr_uncntr', 'anom_corr_uncntr_bcl', 'anom_corr_uncntr_bcu',
                    'dir_me', 'dir_me_bcl', 'dir_me_bcu',
                    'dir_mae', 'dir_mae_bcl', 'dir_mae_bcu',
                    'dir_mse', 'dir_mse_bcl', 'dir_mse_bcu',
                    'dir_rmse', 'dir_rmse_bcl', 'dir_rmse_bcu'

                ]
VCNT_SPECIFIC = [cur_stat_header.upper() for cur_stat_header in LC_VCNT_SPECIFIC]
FULL_VCNT_HEADER = LC_COMMON_STAT_HEADER + ['total'] + VCNT_SPECIFIC
LC_VCNT_STATISTICS_HEADERS =  ['fbar',
                    'obar',
                    'fs_rms',
                    'os_rms',
                    'msve',
                    'rmsve',
                    'fstdev',
                    'ostdev',
                    'fdir',
                    'odir',
                    'fbar_speed',
                    'obar_speed',
                    'vdiff_speed',
                    'vdiff_dir',
                    'speed_err',
                    'speed_abserr',
                    'dir_err',
                    'dir_abserr',
                    'anom_corr',
                    'anom_corr_uncntr',
                    'dir_me',
                    'dir_mae',
                    'dir_mse',
                    'dir_rmse'

                ]
VCNT_STATISTICS_HEADERS = [cur_stat_header.upper() for cur_stat_header in
                          LC_VCNT_STATISTICS_HEADERS]
LC_VCNT_BOOTSTRAP_HEADERS = ['fbar_bcl', 'fbar_bcu',
                             'obar_bcl', 'obar_bcu',
                             'fs_rms_bcl', 'fs_rms_bcu',
                             'os_rms_bcl', 'os_rms_bcu',
                             'msve_bcl', 'msve_bcu',
                             'rmsve_bcl', 'rmsve_bcu',
                             'fstdev_bcl', 'fstdev_bcu',
                             'ostdev_bcl', 'ostdev_bcu',
                             'fdir_bcl', 'fdir_bcu',
                             'odir_bcl', 'odir_bcu',
                             'fbar_speed_bcl', 'fbar_speed_bcu',
                             'obar_speed_bcl', 'obar_speed_bcu',
                             'vdiff_speed_bcl', 'vdiff_speed_bcu',
                             'vdiff_dir_bcl', 'vdiff_dir_bcu',
                             'speed_err_bcl', 'speed_err_bcu',
                             'speed_abserr_bcl', 'speed_abserr_bcu',
                             'dir_err_bcl', 'dir_err_bcu',
                             'dir_abserr', 'dir_abserr_bcl', 'dir_abserr_bcu',
                             'anom_corr_ncl', 'anom_corr_ncu', 'anom_corr_bcl',
                             'anom_corr_bcu',
                             'anom_corr_uncntr_bcl', 'anom_corr_uncntr_bcu',
                             'dir_me_bcl',  'dir_me_bcu',
                             'dir_mae_bcl', 'dir_mae_bcu',
                             'dir_mse_bcl', 'dir_mse_bcu',
                             'dir_rmse_bcl', 'dir_rmse_bcu'

                             ]
VCNT_BOOTSTRAP_HEADERS = [cur_stat_header.upper() for cur_stat_header in
                         LC_VCNT_BOOTSTRAP_HEADERS]

# Column headers for the VCNT line type's bootstrap confidence levels

VCNT_NCL_HEADERS = ['ANOM_CORR_NCL']

VCNT_NCU_HEADERS = ['ANOM_CORR_NCU']

VCNT_BCL_HEADERS = ['FBAR_BCL', 'OBAR_BCL','FS_RMS_BCL', 'OS_RMS_BCL',
                    'MSVE_BCL', 'RMSVE_BCL', 'FSTDEV_BCL', 'OSTDEV_BCL',
                    'FDIR_BCL', 'ODIR_BCL', 'FBAR_SPEED_BCL', 'OBAR_SPEED_BCL',
                    'VDIFF_SPEED_BCL', 'VDIFF_DIR_BCL', 'SPEED_ERR_BCL',
                    'SPEED_ABSERR_BCL', 'DIR_ERR_BCL', 'DIR_ABSERR_BCL',
                    'ANOM_CORR_BCL', 'ANOM_CORR_UNCNTR_BCL',
                    'DIR_ME_BCL', 'DIR_MAE_BCL', 'DIR_MSE_BCL', 'DIR_RMSE_BCL']

VCNT_BCU_HEADERS = ['FBAR_BCU', 'OBAR_BCU','FS_RMS_BCU', 'OS_RMS_BCU',
                    'MSVE_BCU', 'RMSVE_BCU', 'FSTDEV_BCU', 'OSTDEV_BCU',
                    'FDIR_BCU', 'ODIR_BCU', 'FBAR_SPEED_BCU', 'OBAR_SPEED_BCU',
                    'VDIFF_SPEED_BCU', 'VDIFF_DIR_BCU', 'SPEED_ERR_BCU',
                    'SPEED_ABSERR_BCU', 'DIR_ERR_BCU','DIR_ABSERR_BCU',
                    'ANOM_CORR_BCU', 'ANOM_CORR_UNCNTR_BCU',
                    'DIR_ME_BCU', 'DIR_MAE_BCU', 'DIR_MSE_BCU', 'DIR_RMSE_BCU'
                    ]



##### CTC Line type ######
LC_STAT_CTC_SPECIFIC = [FY_OY, FY_ON, FN_OY, FN_ON, EC_VALUE]
CTC_STATISTICS_HEADERS = [cur_stat_header.upper() for cur_stat_header in
                          LC_STAT_CTC_SPECIFIC]
CTC_HEADERS = LC_COMMON_STAT_HEADER + ['total'] + CTC_STATISTICS_HEADERS

##### MCTC Line type ######
LC_STAT_MCTC_SPECIFIC = [N_CAT, FI_OJ, EC_VALUE]
MCTC_STATISTICS_HEADERS = [cur_stat_header.upper() for cur_stat_header in
                           LC_STAT_MCTC_SPECIFIC]
MCTC_HEADERS = LC_COMMON_STAT_HEADER + ['total'] + MCTC_STATISTICS_HEADERS

#### SL1L2 Line type ####

LC_SL1L2_SPECIFIC = ['fbar', 'obar', 'fobar', 'ffbar', 'oobar', 'mae']
SL1L2_STATISTICS_HEADERS = [cur_stat_header.upper() for cur_stat_header in
                            LC_SL1L2_SPECIFIC]
SL1L2_HEADERS = LC_COMMON_STAT_HEADER + ['total'] + SL1L2_STATISTICS_HEADERS

#### VL1L2 Line type ####

LC_VL1L2_SPECIFIC = ['ufbar', 'vfbar', 'uobar', 'vobar', 'uvfobar', 'uvffbar',
                     'uvoobar', 'f_speed_bar', 'o_speed_bar', 'dir_me', 'dir_mae', 'dir_mse']
VL1L2_STATISTICS_HEADERS = [cur_stat_header.upper() for cur_stat_header in
                            LC_VL1L2_SPECIFIC]
VL1L2_HEADERS = LC_COMMON_STAT_HEADER + ['total'] + VL1L2_STATISTICS_HEADERS


#### CTS Line type ####
LC_CTS_SPECIFIC = [BASER, 'baser_ncl', 'baser_ncu', 'baser_bcl', 'baser_bcu',
                   FMEAN, 'fmean_ncl', 'fmean_ncu', 'fmean_bcl', 'fmean_bcu',
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
                   'bagss', 'bagss_bcl', 'bagss_bcu', 'hss_ec', 'hss_ec_bcl',
                   'hss_ec_bcu', EC_VALUE]
CTS_SPECIFIC = [cur_stat_header.upper() for cur_stat_header in LC_CTS_SPECIFIC]
CTS_SPECIFIC_HEADERS = LC_COMMON_STAT_HEADER + ['total'] + CTS_SPECIFIC
CTS_STATS_ONLY = [BASER, FMEAN, 'acc', 'fbias', 'pody', 'podn', 'pofd', 'far', 'csi',
                  'gss', 'hk', 'hss', 'odds', 'lodds', 'orss', 'eds', 'seds', 'edi',
                  'sedi', 'bagss', 'hss_ec', EC_VALUE]
CTS_STATS_ONLY_HEADERS = [cur_stat_header.upper() for cur_stat_header in CTS_STATS_ONLY]
CTS_HEADERS = LC_COMMON_STAT_HEADER + ['total'] + CTS_STATS_ONLY_HEADERS

#### MCTS Line type ####
LC_MCTS_SPECIFIC = [ N_CAT, 'acc', 'acc_ncl', 'acc_ncu', 'acc_bcl', 'acc_bcu',
                    'hk', 'hk_bcl', 'hk_bcu', 'hss', 'hss_bcl', 'hss_bcu', 'ger',
                    'ger_bcl', 'ger_bcu', 'hss_ec', 'hss_ec_bcl', 'hss_ec_bcu',
                    EC_VALUE]
MCTS_SPECIFIC = [cur_stat_header.upper() for cur_stat_header in LC_MCTS_SPECIFIC]
MCTS_SPECIFIC_HEADERS = LC_COMMON_STAT_HEADER + ['total'] + MCTS_SPECIFIC
MCTS_STATS_ONLY = [N_CAT, 'acc', 'hk', 'hss', 'ger', 'hss_ec', EC_VALUE]
MCTS_STATS_ONLY_HEADERS = [cur_stat_header.upper() for cur_stat_header in
                           MCTS_STATS_ONLY]
MCTS_HEADERS = LC_COMMON_STAT_HEADER + ['total'] + MCTS_STATS_ONLY_HEADERS


#### PCT line type ####
LC_PCT_VARIABLE_HEADERS = ['thresh','oy', 'on']

#### RHIST line type ####
LC_RHIST_VARIABLE_HEADERS = ['rank']

#### TCDIAG line type ####
# These are the header names assigned by the METdbLoad
# read_data_files module
TC_DIAG_TOTAL_COLNAME = '0'
TC_DIAG_INDEX_COLNAME = '1'
TCDIAG_DIAG_SOURCE_COLNAME = '2'
TCDIAG_TRACK_SOURCE_COLNAME = '3'
TCDIAG_FIELD_SOURCE_COLNAME = '4'
TCDIAG_N_DIAG_COLNAME = LINE_VAR_COUNTER[TCDIAG]
# Common name for identical fields with different identifiers (due to different DIAG_SOURCE)
# Used for cleaning up reformatted output
SHMAG = 'SHEAR_MAGNITUDE'
DLAND = 'DIST_TO_LAND'
SSPEED = 'STORM_SPEED'
TCDIAG_COMMON_NAMES = {'SHR_MAG':SHMAG, 'SHRD':SHMAG, 'LAND':DLAND, 'DTL':DLAND, 'STM_SPD':SSPEED }


# MPR
LC_MPR_SPECIFIC = ['index', 'obs_sid', 'obs_lat', 'obs_lon', 'obs_lvl',
                    'obs_elv', 'fcst', 'obs', 'obs_qc',
                    'obs_climo_mean', 'obs_climo_stdev', 'obs_climo_cdf',
                   'fcst_climo_mean', 'fcst_climo_stdev']

MPR_SPECIFIC = [cur_stat_header.upper() for cur_stat_header in LC_MPR_SPECIFIC]

MPR_HEADERS = LC_COMMON_STAT_HEADER + ['total'] + LC_MPR_SPECIFIC
