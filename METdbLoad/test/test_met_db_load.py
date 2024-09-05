import argparse
from METdbLoad.conftest import TOP_DIR
from METdbLoad.ush.met_db_load import main as load_main
from METdbLoad.ush.run_sql import RunSql

def test_met_db_load(emptyDB, get_xml_test_file, testRunSql, tmp_path):

    # TODO: parameterize this test data
    test_data = {
        "xmlfile": str(get_xml_test_file),
        "index": True,
        "tmpdir": [str(tmp_path)],
    }
    test_args = argparse.Namespace()
    for k,v in test_data.items():
        setattr(test_args, k, v)

    load_main(test_args)

    # Check the correct number of rows written
    testRunSql.cur.execute("SELECT * FROM line_data_cts")
    cts_data = testRunSql.cur.fetchall()

    assert len(cts_data) == 24

    #TODO: check all the other metrics and some values.

