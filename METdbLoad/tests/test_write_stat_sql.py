#!/usr/bin/env python3
"""Test reading data files."""

# pylint:disable=import-error
# imported modules exist

from read_load_xml import XmlLoadFile
from read_data_files import ReadDataFiles
from run_sql import RunSql
from write_file_sql import WriteFileSql
from write_stat_sql import WriteStatSql

# Read in the XML load file
XML_FILE = '/Users/venita.hagerty/metviewer/testloadv10fewp3.xml'

XML_LOADFILE = XmlLoadFile(XML_FILE)
XML_LOADFILE.read_xml()

# Read all of the data from the data files into a dataframe
FILE_DATA = ReadDataFiles()

# read in the data files, with options specified by XML flags
FILE_DATA.read_data(XML_LOADFILE.flags,
                    XML_LOADFILE.load_files,
                    XML_LOADFILE.line_types)

# Connect to the database
sql_run = RunSql()
sql_run.sql_on(XML_LOADFILE.connection)

# clear the data if it exists
sql_run.cur.execute("delete from data_file;")
sql_run.cur.execute("delete from line_data_cnt;")
sql_run.cur.execute("delete from line_data_ctc;")
sql_run.cur.execute("delete from line_data_cts;")
sql_run.cur.execute("delete from line_data_cts;")
sql_run.cur.execute("delete from line_data_eclv;")
sql_run.cur.execute("delete from line_data_eclv_pnt;")
sql_run.cur.execute("delete from line_data_ecnt;")
sql_run.cur.execute("delete from line_data_enscnt;")
sql_run.cur.execute("delete from line_data_fho;")
sql_run.cur.execute("delete from line_data_grad;")
sql_run.cur.execute("delete from line_data_isc;")
sql_run.cur.execute("delete from line_data_mctc;")
sql_run.cur.execute("delete from line_data_mctc_cnt;")
sql_run.cur.execute("delete from line_data_mcts;")
sql_run.cur.execute("delete from line_data_mpr;")
sql_run.cur.execute("delete from line_data_nbrcnt;")
sql_run.cur.execute("delete from line_data_nbrctc;")
sql_run.cur.execute("delete from line_data_nbrcts;")
sql_run.cur.execute("delete from line_data_orank;")
sql_run.cur.execute("delete from line_data_orank_ens;")
sql_run.cur.execute("delete from line_data_pct;")
sql_run.cur.execute("delete from line_data_pct_thresh;")
sql_run.cur.execute("delete from line_data_perc;")
sql_run.cur.execute("delete from line_data_phist;")
sql_run.cur.execute("delete from line_data_phist;")
sql_run.cur.execute("delete from line_data_phist_bin;")
sql_run.cur.execute("delete from line_data_pjc;")
sql_run.cur.execute("delete from line_data_pjc_thresh;")
sql_run.cur.execute("delete from line_data_prc;")
sql_run.cur.execute("delete from line_data_prc_thresh;")
sql_run.cur.execute("delete from line_data_pstd;")
sql_run.cur.execute("delete from line_data_pstd_thresh;")
sql_run.cur.execute("delete from line_data_relp;")
sql_run.cur.execute("delete from line_data_relp_ens;")
sql_run.cur.execute("delete from line_data_rhist;")
sql_run.cur.execute("delete from line_data_rhist_rank;")
sql_run.cur.execute("delete from line_data_sl1l2;")
sql_run.cur.execute("delete from line_data_sal1l2;")
sql_run.cur.execute("delete from line_data_ssvar;")
sql_run.cur.execute("delete from line_data_vl1l2;")
sql_run.cur.execute("delete from line_data_val1l2;")
sql_run.cur.execute("delete from line_data_vcnt;")
sql_run.cur.execute("delete from stat_header;")
sql_run.cur.execute("delete from instance_info;")
sql_run.cur.execute("delete from metadata;")

write_file = WriteFileSql()
updated_data = write_file.write_file_sql(XML_LOADFILE.flags,
                                         FILE_DATA.data_files,
                                         FILE_DATA.stat_data,
                                         FILE_DATA.mode_cts_data,
                                         FILE_DATA.mode_obj_data,
                                         sql_run.cur,
                                         sql_run.local_infile)

FILE_DATA.data_files = updated_data[0]
FILE_DATA.stat_data = updated_data[1]

STAT_LINES = WriteStatSql()

STAT_LINES.write_sql_data(XML_LOADFILE.flags,
                          FILE_DATA.stat_data,
                          sql_run.cur,
                          sql_run.local_infile)

write_file.write_metadata_sql(XML_LOADFILE.flags,
                              FILE_DATA.data_files,
                              XML_LOADFILE.group,
                              XML_LOADFILE.description,
                              XML_LOADFILE.load_note,
                              XML_LOADFILE.xml_str,
                              sql_run.cur)

def test_counts():
    """Count lines in database tables."""

    # Count the number of instance_info records created
    sql_run.cur.execute("SELECT COUNT(*) from instance_info;")
    result = sql_run.cur.fetchone()
    assert result[0] == 1

    # Count the number of metadata records created
    sql_run.cur.execute("SELECT COUNT(*) from metadata;")
    result = sql_run.cur.fetchone()
    assert result[0] == 1

    # Count the number of data_file records created
    sql_run.cur.execute("SELECT COUNT(*) from data_file;")
    result = sql_run.cur.fetchone()
    assert result[0] == 7

    # Count the number of stat_header records created
    sql_run.cur.execute("SELECT COUNT(*) from stat_header;")
    result = sql_run.cur.fetchone()
    assert result[0] == 368

    # Count the number of line_data_fho records created
    sql_run.cur.execute("SELECT COUNT(*) from line_data_fho;")
    result = sql_run.cur.fetchone()
    assert result[0] == 163

    # Count the number of line_data_ctc records created
    sql_run.cur.execute("SELECT COUNT(*) from line_data_ctc;")
    result = sql_run.cur.fetchone()
    assert result[0] == 163

    # Count the number of line_data_cnt records created
    sql_run.cur.execute("SELECT COUNT(*) from line_data_cnt;")
    result = sql_run.cur.fetchone()
    assert result[0] == 162

    # Count the number of line_data_sl1l2 records created
    sql_run.cur.execute("SELECT COUNT(*) from line_data_sl1l2;")
    result = sql_run.cur.fetchone()
    assert result[0] == 10945

    # Count the number of line_data_cts records created
    sql_run.cur.execute("SELECT COUNT(*) from line_data_cts;")
    result = sql_run.cur.fetchone()
    assert result[0] == 326

    # Count the number of line_data_ecnt records created
    sql_run.cur.execute("SELECT COUNT(*) from line_data_ecnt;")
    result = sql_run.cur.fetchone()
    assert result[0] == 27

    # Count the number of line_data_grad records created
    sql_run.cur.execute("SELECT COUNT(*) from line_data_grad;")
    result = sql_run.cur.fetchone()
    assert result[0] == 3

    sql_run.cur.close()
    sql_run.conn.close()
