#!/usr/bin/env python3
"""Define constants."""

# pylint:disable=no-member
# constants exist in constants.py

# name to use for a group when no group tag is included in load_spec
DEFAULT_DATABASE_GROUP = "NO GROUP"

# Maxiumum number of columns
MAX_COL = 120

# line types
FHO = "fho"
CTC = "ctc"
CTS = "cts"
MCTC = "mctc"
MCTS = "mcts"
CNT = "cnt"
SL1L2 = "sl1l2"
SAL1L2 = "sal1l2"
VL1L2 = "vl1l2"
VAL1L2 = "val1l2"
PCT = "pct"
PSTD = "pstd"
PJC = "pjc"
PRC = "prc"
ECLV = "eclv"
MPR = "mpr"
NBRCTC = "nbrctc"
NBRCTS = "nbrcts"
NBRCNT = "nbrcnt"
ISC = "isc"
RHIST = "rhist"
PHIST = "phist"
ORANK = "orank"
SSVAR = "ssvar"
GRAD = "grad"
VCNT = "vcnt"
RELP = "relp"
ECNT = "ecnt"
ENSCNT = "enscnt"
PERC = "perc"

LINE_TYPES = [FHO, CTC, CTS, MCTC, MCTS, CNT, SL1L2, SAL1L2, VL1L2, VAL1L2,
              PCT, PSTD, PJC, PRC, ECLV, MPR, NBRCTC, NBRCTS, NBRCNT, ISC,
              RHIST, PHIST, ORANK, SSVAR, GRAD, VCNT, RELP, ECNT, ENSCNT, PERC]

UC_LINE_TYPES = [ltype.upper() for ltype in LINE_TYPES]

# Lower Case true and false
LCTRUE = "true"
LCFALSE = "false"

# Not Available - NA
NOTAV = "NA"

# column names
VERSION = "VERSION"
MODEL = "MODEL"
DESC = "DESC"
FCST_LEAD = "FCST_LEAD"
FCST_VALID_BEG = "FCST_VALID_BEG"
FCST_VALID_END = "FCST_VALID_END"
OBS_LEAD = "OBS_LEAD"
OBS_VALID_BEG = "OBS_VALID_BEG"
OBS_VALID_END = "OBS_VALID_END"
FCST_VAR = "FCST_VAR"
FCST_UNITS = "FCST_UNITS"
FCST_LEV = "FCST_LEV"
OBS_VAR = "OBS_VAR"
OBS_UNITS = "OBS_UNITS"
OBS_LEV = "OBS_LEV"
OBTYPE = "OBTYPE"
VX_MASK = "VX_MASK"
INTERP_MTHD = "INTERP_MTHD"
INTERP_PNTS = "INTERP_PNTS"
FCST_THRESH = "FCST_THRESH"
OBS_THRESH = "OBS_THRESH"
COV_THRESH = "COV_THRESH"
ALPHA = "ALPHA"
LINE_TYPE = "LINE_TYPE"
FCST_INIT_BEG = "FCST_INIT_BEG"

FCST_LEAD_HR = "FCST_LEAD_HR"

# After units added in MET 8.1
LONG_HEADER = [VERSION, MODEL, DESC, FCST_LEAD, FCST_VALID_BEG, FCST_VALID_END,
               OBS_LEAD, OBS_VALID_BEG, OBS_VALID_END, FCST_VAR, FCST_UNITS, FCST_LEV,
               OBS_VAR, OBS_UNITS, OBS_LEV, OBTYPE, VX_MASK, INTERP_MTHD, INTERP_PNTS,
               FCST_THRESH, OBS_THRESH, COV_THRESH, ALPHA, LINE_TYPE]

# Contains DESC but not UNITS
MID_HEADER = LONG_HEADER[0:10] + LONG_HEADER[11:13] + LONG_HEADER[14:]

# No DESC and no UNITS
SHORT_HEADER = MID_HEADER[0:2] + MID_HEADER[3:]

COL_NUMS = [str(x) for x in range(MAX_COL - 24)]
