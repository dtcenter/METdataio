import pytest
import sys
import os
import pymysql
from pathlib import Path
from unittest.mock import patch

from METdataio.METdbLoad.ush.run_sql import RunSql
from METdataio.METdbLoad.test.utils import (
    get_xml_test_file,
    POINT_STAT_DATA_DIR,
)


# add METdataio directory to path so packages can be found
TOP_DIR = str(Path(__file__).parents[1])
sys.path.insert(0, os.path.abspath(TOP_DIR))


def parse_sql(filename):
    """Parse a .sql file and return a list of SQL statements"""
    data = open(filename, "r").readlines()
    stmts = []
    DELIMITER = ";"
    stmt = ""

    for line in data:
        if not line.strip():
            continue

        if line.startswith("--"):
            continue

        if DELIMITER not in line:
            stmt += line
            continue

        if stmt:
            stmt += line
            stmts.append(stmt.strip())
            stmt = ""
        else:
            stmts.append(line.strip())
    return stmts


def maria_conn():
    """A databaseless connection to mariaDB server.
    This will work even if no database has been created.
    """
    try:
        conn = pymysql.connect(
            host="localhost",
            port=3306,
            user="root",
            password="root_password",
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
        host="localhost",
        port=3306,
        user="root",
        password="root_password",
        database="mv_test",
        autocommit=True,
    )

    sql_statements = parse_sql(Path(TOP_DIR) / "METdbLoad/sql/mv_mysql.sql")

    with db_conn.cursor() as cur:
        for stm in sql_statements:
            cur.execute(stm)

    db_conn.close()


@pytest.fixture
def testRunSql():
    """Return an instance of RunSql with a connection."""
    connection = {
        "db_host": "localhost",
        "db_port": 3306,
        "db_user": "root",
        "db_password": "root_password",
        "db_database": "mv_test",
    }

    testRunSql = RunSql()
    testRunSql.sql_on(connection)
    return testRunSql


@pytest.fixture
def get_xml_loadfile():
    def load_and_read_xml(
        tmp_path, data_dir=POINT_STAT_DATA_DIR, met_tool="point_stat"
    ):
        from METdataio.METdbLoad.ush.read_load_xml import XmlLoadFile

        XML_FILE = get_xml_test_file(tmp_path, data_dir, met_tool)
        XML_LOADFILE = XmlLoadFile(XML_FILE)
        XML_LOADFILE.read_xml()
        return XML_LOADFILE

    return load_and_read_xml
