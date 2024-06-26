import pytest
import pymysql
import yaml
from dataclasses import make_dataclass

#######################################################################
# These tests can only be run on the host where the database is running.
# Pre-condition:
#     The data in the accompanying data directory ./Data, should
#     already be loaded in the database using the corresponding
#     schema: mv_mysql.sql and the appropriate xml specification file
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
    DBS = make_dataclass("DBS", ["hostname", "port", "username", "password", "dbname"])
    db_settings = DBS(parms['hostname'], parms['port'], parms['username'], parms['password'], parms['dbname'])

    # Return the db settings (hostname, username, etc.)
    conn = pymysql.connect(
        host=db_settings.hostname,
        port=db_settings.port,
        user=db_settings.username,
        password=db_settings.password,
        db=db_settings.dbname,
        charset='utf8mb4'
    )

    yield conn
    
   
def test_ecnt_db_created(setup_db):

    # log into the database and verify the database exists, tables exist, new columns for each affected
    # table exists, and check that for specific ign_conv_oerr and ign_corr_oer values, only one row is
    # found.


    try:
        with setup_db.cursor() as cursor:
            # Check that the mv_load_test database was created
            check_db_exists_query = "show databases;"
            cursor.execute(check_db_exists_query)

            # Get all rows
            rows = cursor.fetchall()
            list_of_rows = [r[0] for r in rows]

            #Results
            assert 'mv_load_test' in list_of_rows



    finally:
        setup_db.close()

def test_tables_created(setup_db):

    # log into the database and verify the ECNT, VCNT, VL1L2, and VAL1L2 tables exist

    try:
        with setup_db.cursor() as cursor:
            # Check that the line_data_ecnt, line_data_vcnt, line_data_vl1l2, and
            # line_data_val1l2 tables were created
            cursor.execute(CONST_LOAD_DB_CMD)

            check_tables_exist = "show tables;"
            cursor.execute(check_tables_exist)

            # Get all rows
            rows = cursor.fetchall()
            list_of_rows = [r[0] for r in rows]
            assert 'line_data_ecnt' in list_of_rows
            assert 'line_data_vcnt' in list_of_rows
            assert 'line_data_vl1l2' in list_of_rows
            assert 'line_data_val1l2' in list_of_rows

    finally:
        setup_db.close()


def test_ecnt_columns(setup_db):
   # log into the database and verify the ign_conv_oerr and ign_corr_oerr columns are in the
   # list_data_ecnt database table.

    try:
        with setup_db.cursor() as cursor:
            # Check that the line_data_ecnt, line_data_vcnt, line_data_vl1l2, and
            # line_data_val1l2 tables were created
            cursor.execute(CONST_LOAD_DB_CMD)


            check_columns_exist = "desc line_data_ecnt;"
            cursor.execute(check_columns_exist)

            # Get all rows
            rows = cursor.fetchall()
            list_of_rows = [r[0] for r in rows]
            assert 'ign_conv_oerr' in list_of_rows
            assert 'ign_corr_oerr' in list_of_rows

    finally:
        setup_db.close()

def test_vcnt_columns(setup_db):
   # log into the database and verify the dir_me, dir_me_bcl, dir_me_bcu, ..., etc. columns are in the
   # list_data_ecnt database table.

    expected_cols = ['dir_me', 'dir_me_bcl', 'dir_me_bcu',
                     'dir_mae', 'dir_mae_bcl', 'dir_mae_bcu',
                     'dir_mse', 'dir_mse_bcl', 'dir_mse_bcu',
                     'dir_rmse', 'dir_rmse_bcl', 'dir_rmse_bcu'
                     ]

    try:
        with setup_db.cursor() as cursor:
            # Check that the  line_data_vcnt expected columns were created
            cursor.execute(CONST_LOAD_DB_CMD)

            check_columns_exist = "desc line_data_vcnt;"
            cursor.execute(check_columns_exist)

            # Get all rows
            rows = cursor.fetchall()
            list_of_rows = [r[0] for r in rows]
            for expected in expected_cols:
                assert expected in list_of_rows
                assert expected in list_of_rows

    finally:
        setup_db.close()

def test_vl1l2_columns(setup_db):
   # log into the database and verify the dir_me, dir_mae, and dir_mse columns are in the
   # list_data_vl1l2 database table.

    expected_cols = ['dir_me', 'dir_mae', 'dir_mse']

    try:
        with setup_db.cursor() as cursor:
            # Check that the  line_data_vl1l2 table has the expected columns
            cursor.execute(CONST_LOAD_DB_CMD)

            check_columns_exist = "desc line_data_vl1l2;"
            cursor.execute(check_columns_exist)

            # Get all rows
            rows = cursor.fetchall()
            list_of_rows = [r[0] for r in rows]
            for expected in expected_cols:
                assert expected in list_of_rows
                assert expected in list_of_rows

    finally:
        setup_db.close()

def test_val1l2_columns(setup_db):
   # log into the database and verify the dira_me, dira_mae, and dira_mse columns are in the
   # list_data_val1l2 database table.

    expected_cols = ['dira_me', 'dira_mae', 'dira_mse']

    try:
        with setup_db.cursor() as cursor:
            # Check that the line_data_vl1l2 table has the expected columns
            cursor.execute(CONST_LOAD_DB_CMD)

            check_columns_exist = "desc line_data_val1l2;"
            cursor.execute(check_columns_exist)

            # Get all rows
            rows = cursor.fetchall()
            list_of_rows = [r[0] for r in rows]
            for expected in expected_cols:
                assert expected in list_of_rows
                assert expected in list_of_rows

    finally:
        setup_db.close()


def test_ecnt_vals(setup_db):
   # log into the database and verify the ECNT values for ign_conv_oerr and
   # ign_corr_oerr result in a result. This verifies that the input data
   # was correctly loaded for the ECNT data.

    ign_conv_oerr = "33.41424"
    ign_corr_oerr = "440.06905"

    try:
        with setup_db.cursor() as cursor:
            # Check that the line_data_vl1l2 table has the expected columns
            cursor.execute(CONST_LOAD_DB_CMD)

            check_columns_exist = "select * from line_data_ecnt where ign_conv_oerr = " + ign_conv_oerr + \
                                  " AND ign_corr_oerr = " + ign_corr_oerr +  ";"
            cursor.execute(check_columns_exist)

            # Get all rows
            rows = cursor.fetchall()
            # Only one row should correspond to this query
            assert len(rows) == 1


    finally:
        setup_db.close()


