from pathlib import Path
from argparse import Namespace
import os


def abs_path(rel_path):
    """Turn a relative path into abs path"""
    return str(Path(str(Path(__file__).parents[2])) / rel_path)

# XML Schema for the load specification XML
LOAD_SPECIFICATION_SCHEMA = os.path.join(abs_path("METdbLoad/ush/"), "load_specification_schema.xsd")

# Use data from METreformat where available
ENSEMBLE_STAT_DATA_DIR = abs_path("METreformat/test/data/ensemble_stat")
GRID_STAT_DATA_DIR = abs_path("METreformat/test/data/grid_stat/mctc_mcts")
MPR_DATA_DIR = abs_path("METreformat/test/data/mpr/climo_data")
POINT_STAT_DATA_DIR = abs_path("METreformat/test/data/point_stat")
TCDIAG_DATA_DIR = abs_path("METreformat/test/data/tcdiag_tcmpr")
ONE_EMPTY_DIR = abs_path("METreformat/test/data/point_stat/one_empty")
TCSTAT_DIR = abs_path("METreformat/test/data/tc_stat_rirw_cts_ctc")
TCSTAT_NO_HEADER = abs_path("METreformat/test/data/tc_stat_rirw_cts_ctc/tc_stat_no_header")

# Data in METdbLoad
EMPTY_DIR = abs_path("METdbLoad/test/data/empty")
VSDB_DIR = abs_path("METdbLoad/test/data/vsdb")
VSDB_NO_EQUALS_DIR = abs_path("METdbLoad/test/data/vsdb/vsdb_no_equals")
VSDB_EMPTY = abs_path("METdbLoad/test/data/empty/empty_vsdb")
VSDB_ONE_EMPTY = abs_path("METdbLoad/test/data/empty/empty_vsdb/one_empty")
MODE_CTS_EMPTY = abs_path("METdbLoad/test/data/empty/empty_mode_cts/")
MODE_OBJ_EMPTY = abs_path("METdbLoad/test/data/empty/empty_mode_obj/")
MODE_NO_HEADER = abs_path("METdbLoad/test/data/mode/no_header")
MODE_ONLY_CTS = abs_path("METdbLoad/test/data/mode/only_cts")
MODE_EMPTY = abs_path("METdbLoad/test/data/empty/empty_mode")
MTD_EMPTY = abs_path("METdbLoad/test/data/empty/empty_mtd")
MTD_DIR = abs_path("METdbLoad/test/data/mtd")
MTD_HEADER_NO_DATA = abs_path("METdbLoad/test/data/mtd/mtd_header_only")
MTD_INTENSITY_90_LAST_COL = abs_path("METdbLoad/test/data/mtd/intensity_90_last_col")
MTD_NO_FCST_T_BEG = abs_path("METdbLoad/test/data/mtd/no_fcst_t_beg")


# This data is copied from MET test data
# https://hub.docker.com/r/dtcenter/met-data-output
MTD_DATA_DIR = abs_path("METdbLoad/test/data/mtd/")
MODE_DATA_DIR = abs_path("METdbLoad/test/data/mode/")

# Very small data sample for testing
VSDB_DATA_DIR = abs_path("METdbLoad/test/data/vsdb/")
RHIST_DATA_DIR = abs_path("METdbLoad/test/data/rhist")

DEFAULT_LOAD_FLAGS = {
    "stat_header_db_check": "true",
    "mode_header_db_check": "false",
    "mtd_header_db_check": "false",
    "drop_indexes": "false",
    "apply_indexes": "false",
    "load_stat": "true",
    "load_mode": "true",
    "load_mtd": "true",
    "load_mpr": "true",
    "load_orank": "true",
    "force_dup_file": "true",
}


def _dict_to_xml(flags_dict):
    flags = [f"<{flag}>{value}</{flag}>" for flag, value in flags_dict.items()]
    return "\n    ".join(flags)


def populate_xml_load_spec(met_data_dir, met_tool, load_flags=DEFAULT_LOAD_FLAGS, local_infile=True):
    """Return the xml load specification with substitute values.

    Args:
        met_data_dir (str): directory containing MET files to load
        met_tool (str): Name of MET tool that generated files, e.g. "point_stat"
        load_flags (dict): Optional. Specify any load flags to use. All flags default to the values in DEFAULT_LOAD_FLAGS.

    Returns:
        str
    """

    # combine user supplied and default load_flags
    load_flags = {**DEFAULT_LOAD_FLAGS, **load_flags}
    flags = _dict_to_xml(load_flags)

    return f"""<load_spec>
    <connection>
        <management_system>mysql</management_system>
        <host>localhost:3306</host>
        <database>mv_test</database>
        <user>root</user>
        <password>root_password</password>
        <local_infile>{local_infile}</local_infile>
    </connection>

    <verbose>true</verbose>
    <insert_size>1</insert_size>
    {flags}
    <group>Testing</group>
    <description>testing with pytest</description>
    <folder_tmpl>{met_data_dir}</folder_tmpl>
    <load_val>
        <field name="met_tool">
        <val>{met_tool}</val>
        </field>
    </load_val>
    </load_spec>"""


def get_xml_test_file(tmp_path, met_data_dir, met_tool, load_flags={}, local_infile=True):
    """Write test_load_specification.xml and return path

    Args:
        tmp_path (Path): Path to write test file to.
        met_data_dir (str): directory containing MET files to load
        met_tool (str): Name of MET tool that generated files, e.g. "point_stat"
        load_flags (dict): Optional.

    Returns:
        str
    """
    xml_path = tmp_path / "test_load_specification.xml"
    with open(xml_path, "w") as text_file:
        text_file.write(populate_xml_load_spec(met_data_dir, met_tool, load_flags, local_infile))

    return xml_path


def dict_to_args(args_dict):
    """Convert a dict to an argparse Namespace
    
    Args:
        args_dict (dict): key value pairs to be converted to
        argparse Namespace.
    
    Returns:
        argparse.Namespace
    """
    test_args = Namespace()
    for k, v in args_dict.items():
        setattr(test_args, k, v)
    return test_args
