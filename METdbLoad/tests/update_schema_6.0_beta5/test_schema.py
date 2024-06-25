import pytest
import pymysql
import yaml
from dataclasses import make_dataclass

#######################################################################
# These tests can only be run on the host where the database is running.
# Pre-condition:
#     The data in the accompanying data directory ./Data, should
#     already be loaded in the database using the corresponding
#     schema: mv_mysql.sql and the appropriate xml specification file.
#     This is to avoid having the password visible in the test code.
#

CONST_LOAD_DB_CMD = "use mv_load_test"


@pytest.fixture
def setup_db():
    """
          Read in the config file to retrieve the database login information.

    """
    config_file = 'test_loading.yaml'
    with open(config_file, 'r') as stream:
        try:
            parms: dict = yaml.load(stream, Loader=yaml.FullLoader)
            # pathlib.Path(parms['output_dir']).mkdir(parents=True, exist_ok=True)
        except yaml.YAMLError as exc:
            print(exc)

    # Create a dataclass of the database information
    # TCDiag = make_dataclass("TCDiag", ["total", "index", "diag_src", "diag_val"], frozen=True)
    # orig = TCDiag(orig_total, orig_index, orig_diag_src, orig_diag_val)
    DBS = make_dataclass("DBS", ["hostname", "username", "password", "dbname"])
    db_settings = DBS(parms['hostname'], parms['username'], parms['password'], parms['dbname'])

    # Return the db connection object

    ## settings (hostname, username, etc.)
    ## yield db_settings
    conn = pymysql.connect(
        host=db_settings.hostname,
        user=db_settings.username,
        password=db_settings.password,
        db=db_settings.dbname,
        charset='utf8mb4'
    )
    yield conn

    # Tear-down
    conn.close()


def test_tables_created(setup_db):

    # log into the database and verify the VCNT, VL1L2, and VAL1L2 tables exist

    try:
        with setup_db.cursor() as cursor:
            # Check that the  line_data_vcnt, line_data_vl1l2, and
            # line_data_val1l2 tables were created
            cursor.execute(CONST_LOAD_DB_CMD)

            check_tables_exist = "show tables;"
            cursor.execute(check_tables_exist)

            # Get all rows
            rows = cursor.fetchall()
            list_of_rows = [r[0] for r in rows]
            assert 'line_data_vcnt' in list_of_rows
            assert 'line_data_vl1l2' in list_of_rows
            assert 'line_data_val1l2' in list_of_rows

    finally:
        setup_db.close()