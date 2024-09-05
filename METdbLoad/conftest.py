import pytest
import sys
import os
import pymysql
import logging
from pathlib import Path
from unittest.mock import patch

from METdataio.METdbLoad.ush.read_data_files import ReadDataFiles
from METdataio.METdbLoad.ush.run_sql import RunSql


# add METdataio directory to path so packages can be found
TOP_DIR = str(Path(__file__).parents[1])
sys.path.insert(0, os.path.abspath(TOP_DIR))

def parse_sql(filename):
    """Parse a .sql file and return a list of SQL statements"""
    data = open(filename, 'r').readlines()
    stmts = []
    DELIMITER = ';'
    stmt = ''

    for line in data:
        if not line.strip():
            continue

        if line.startswith('--'):
            continue

        if (DELIMITER not in line):
            stmt += line
            continue

        if stmt:
            stmt += line
            stmts.append(stmt.strip())
            stmt = ''
        else:
            stmts.append(line.strip())
    return stmts


def maria_conn():
    """A databaseless connection to mariaDB server.
    This will work even if no database has been created.
    """
    try:
        conn = pymysql.connect(
            host='localhost',
            port=3306,
            user='root',
            password='root_password',
        )

    except Exception as e:
        # Test run will fail if db is not found.
        # TODO: If we want to run tests that don't require a db when db is missing
        # we could put pytest.skip here instead of raising the exception.
        raise e

    return conn


@pytest.fixture
def emptyDB():
    """Drop and recreate the database.
    Including this fixture in a test will DELETE all data from mv_test.
    """

    conn = maria_conn()
    with conn.cursor() as cur:
        cur.execute("DROP DATABASE IF EXISTS mv_test;")
        cur.execute("CREATE DATABASE mv_test;")
    conn.commit()
    conn.close()

    db_conn = pymysql.connect(
            host='localhost',
            port=3306,
            user='root',
            password='root_password',
            database='mv_test',
            autocommit=True,
        )

    sql_statements = parse_sql(Path(TOP_DIR) / 'METdbLoad/sql/mv_mysql.sql')

    with db_conn.cursor() as cur:
        for stm in sql_statements:
            cur.execute(stm)

    db_conn.close()


@pytest.fixture
def testRunSql():
    """Return an instance of RunSql with a connection.
    """
    connection = {
            'db_host': 'localhost',
            'db_port': 3306,
            'db_user': 'root',
            'db_password': 'root_password',
            'db_database': 'mv_test',
    }

    testRunSql = RunSql()
    testRunSql.sql_on(connection)
    return testRunSql


# This is a sample of data copied from test file point_stat_DUP_SINGLE_120000L_20120409_120000V.stat
# found in the METviewer test data.
# TODO: expand this to include other data (e.g. linetypes, met tools, etc.). Probably need to load this from
# disk rather than storing here as a string.
POINT_STAT_DATA = """VERSION MODEL FCST_LEAD FCST_VALID_BEG  FCST_VALID_END  OBS_LEAD OBS_VALID_BEG   OBS_VALID_END   FCST_VAR FCST_LEV OBS_VAR OBS_LEV OBTYPE VX_MASK INTERP_MTHD INTERP_PNTS FCST_THRESH OBS_THRESH COV_THRESH   ALPHA LINE_TYPE
V4.2    WRF   120000    20120409_120000 20120409_120000 000000   20120409_120000 20120409_120000 TMP      Z2       TMP     Z2      ADPSFC FULL    UW_MEAN               1          NA         NA         NA      NA MPR       2         1       001  43.00000 -89.00000 NA 2.00000 275.71640 293.00000 NA NA
V4.2    WRF   120000    20120409_120000 20120409_120000 000000   20120409_120000 20120409_120000 TMP      Z2       TMP     Z2      ADPSFC FULL    UW_MEAN               1          NA         NA         NA      NA MPR       2         2       002  46.00000 -92.00000 NA 2.00000 272.71640 293.00000 NA NA
V4.2    WRF   120000    20120409_120000 20120409_120000 000000   20120409_103000 20120409_133000 TMP      Z2       TMP     Z2      ADPSFC FULL    UW_MEAN               1     >=5.000    >=5.000         NA      NA FHO       2   1.00000   1.00000   1.00000
V4.2    WRF   120000    20120409_120000 20120409_120000 000000   20120409_103000 20120409_133000 TMP      Z2       TMP     Z2      ADPSFC FULL    UW_MEAN               1     >=5.000    >=5.000         NA      NA CTC       2         2         0         0         0
V4.2    WRF   120000    20120409_120000 20120409_120000 000000   20120409_103000 20120409_133000 TMP      Z2       TMP     Z2      ADPSFC FULL    UW_MEAN               1     >=5.000    >=5.000         NA 0.05000 CTS       2   1.00000   0.34238   1.00000        NA NA 1.00000   0.34238   1.00000 NA NA   1.00000   0.34238   1.00000 NA NA 1.00000      NA      NA 1.00000 0.34238 1.00000 NA NA NA NA NA NA NA NA NA        NA        NA      NA 0.00000 0.00000 0.65762      NA       NA 1.00000 0.34238 1.00000 NA NA       NA NA NA        NA NA NA      NA NA NA       NA NA NA        NA NA NA        NA NA NA        NA NA NA        NA NA NA        NA NA NA      NA NA NA      NA NA NA NA NA NA NA NA NA NA NA NA NA NA NA NA NA NA NA
V4.2    WRF   120000    20120409_120000 20120409_120000 000000   20120409_103000 20120409_133000 TMP      Z2       TMP     Z2      ADPSFC FULL    UW_MEAN               1          NA         NA         NA 0.05000 CNT       2 274.21640 255.15709 293.27571        NA NA 2.12132   0.94643  67.69167 NA NA 293.00000 293.00000 293.00000 NA NA 0.00000 0.00000 0.00000      NA      NA      NA NA NA NA NA NA NA  0  0  0 -18.78360 -37.84291 0.27571      NA      NA 2.12132 0.94643 67.69167      NA      NA 0.93589 NA NA 18.78360 NA NA 355.07362 NA NA 2.25000 NA NA 18.84340 NA NA -19.98360 NA NA -19.53360 NA NA -18.78360 NA NA -18.03360 NA NA -17.58360 NA NA 1.50000 NA NA 1.50000 NA NA
"""


def _populate_xml_load_spec(met_data_dir,
                            met_tool="point_stat",
                            host="localhost"):
    """Return the xml load specification with substitute values.
    """
    #TODO: determine if other tags require substitution as well
    return f"""<load_spec>
    <connection>
        <management_system>mysql</management_system>
        <host>{host}:3306</host>
        <database>mv_test</database>
        <user>root</user>
        <password>root_password</password>
    </connection>

    <folder_tmpl>{met_data_dir}</folder_tmpl>
    <verbose>true</verbose>
    <insert_size>1</insert_size>
    <stat_header_db_check>true</stat_header_db_check>
    <mode_header_db_check>false</mode_header_db_check>
    <mtd_header_db_check>false</mtd_header_db_check>
    <drop_indexes>false</drop_indexes>
    <apply_indexes>false</apply_indexes>
    <load_stat>true</load_stat>
    <load_mode>true</load_mode>
    <load_mtd>true</load_mtd>
    <load_mpr>true</load_mpr>
    <load_orank>true</load_orank>
    <force_dup_file>true</force_dup_file>
    <load_val>
        <field name="met_tool">
        <val>{met_tool}</val>
        </field>
    </load_val>
    <group>Testing</group>
    <description>testing DB load</description>
    </load_spec>"""


# TODO: give access to the other test data
@pytest.fixture
def point_stat_file_dir(tmp_path):
    """Write test stat file and return parent dir."""
    return str(Path(TOP_DIR) / 'METreformat/test/data/point_stat' )


#TODO: see if we can restrict the scope of this fixture.
@pytest.fixture
def get_xml_test_file(tmp_path, point_stat_file_dir):
    """Write test_load_specification.xml and return path"""
    xml_path = tmp_path / "test_load_specification.xml"
    with open(xml_path, "w") as text_file:
        text_file.write(_populate_xml_load_spec(point_stat_file_dir))
    return xml_path



@pytest.fixture
def get_xml_loadfile(get_xml_test_file):
    def load_and_read_xml():
        from METdataio.METdbLoad.ush.read_load_xml import XmlLoadFile

        XML_FILE = get_xml_test_file
        XML_LOADFILE = XmlLoadFile(XML_FILE)
        XML_LOADFILE.read_xml()
        return XML_LOADFILE

    return load_and_read_xml
