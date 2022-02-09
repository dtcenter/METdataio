#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Program Name: test_tables.py
Contact(s): Venita Hagerty
Abstract: Compare two MySQL databases - typically one loaded by MVLoad,
and one loaded by METdbLoad using the same XML file
History Log:  Created on Fri Jan 24 09:33:51 2020
Usage: Reads two different MySQL databases
Parameters: N/A
Input Files: N/A
Output Files: N/A
Copyright 2020 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

import sys
import math
import pymysql

# *** Connect to a "production/old" database
DB2 = 'mv_ci_prod'

cnf_file = "/home/runner/work/METdatadb/METdatadb/headnew/METdbLoad/tests/gha.cnf"

conn2 = pymysql.connect(read_default_file=cnf_file, db=DB2)
cur2 = conn2.cursor()

# *** Connect to a "test/new" database
DB3 = 'mv_ci_new'

conn3 = pymysql.connect(read_default_file=cnf_file, db=DB3)
cur3 = conn3.cursor()


# ******************************************************

def run_query(query_txt, sql_cur):
    """
    *** Given text of query and cursor, run the query, printing row count
    """
    sql_cur.execute(query_txt)
    print(sql_cur.rowcount)
    return sql_cur.fetchall()


def different(data1, data2):
    """
    *** Compare two rows to see if they are different
    """
    # see if the whole row matches
    if data1 == data2:
        return False

    row_num = 0
    # check item by item
    for item in data1:
        if not item == data2[row_num]:
            try:
                # check for close enough match of floating point numbers
                if not math.isclose(item, data2[row_num]):
                    return True
            except (RuntimeError, TypeError, NameError, KeyError):
                return True
        row_num += 1
    return False


def count_rows(query2, query3):
    """
    *** Given 2 queries that give a list of tables with their row counts,
    *** compare the counts, and only print if they are different
    """
    cur2.execute(query2)
    result2 = cur2.fetchall()
    cur3.execute(query3)
    result3 = cur3.fetchall()

    row_num = 0
    row_same = True
    for row in result2:
        if row[1] != result3[row_num][1]:
            row_same = False
            print(row[0], row[1], result3[row_num][1])
        row_num += 1
    return row_same


# ******************************************************

# *** Adjust the number of records tested for each table
QUERY_COUNT = 40

# *** Track differences
table_diff = 0

# *** Check to see if all full row counts match

q_header = 'SELECT count(*) from stat_header'
cur2.execute(q_header)
cur3.execute(q_header)

print(DB2 + ' has ' + str(cur2.fetchone()[0]) + ' stat_header records and')
print(DB3 + ' has ' + str(cur3.fetchone()[0]) + ' stat_header records\n')

# *** Count line_data table rows
q_line2 = "SELECT table_name, table_rows FROM information_schema.tables " + \
          "WHERE table_schema = '" + DB2 + "' AND " + \
          "table_name LIKE 'line_data_%';"

q_line3 = "SELECT table_name, table_rows FROM information_schema.tables " + \
          "WHERE table_schema = '" + DB3 + "' AND " + \
          "table_name LIKE 'line_data_%';"

same = count_rows(q_line2, q_line3)

if same:
    print("%%% No differences in line_data row counts")
else:
    table_diff += 1

# *** stat_header records
q_header = 'SELECT * from stat_header ' + \
           'order by model, fcst_var, fcst_lev, vx_mask, fcst_thresh ' + \
           'limit ' + str(QUERY_COUNT) + ';'

# show row counts
print("\n*** Row counts for stat_header tables")

result2 = run_query(q_header, cur2)
result3 = run_query(q_header, cur3)

i = 0
same = True

# compare stat header lines from the two databases
for row in result2:

    if not row[1:] == result3[i][1:]:

        if i > QUERY_COUNT:
            continue
        same = False
        print("*** Different ***")
        print(row)
        print(result3[i])

    i += 1

if same:
    print("No differences for stat_header")
else:
    table_diff += 1

# *** line data records
line_types = ["cnt", "ctc", "cts", "dmap", "eclv", "ecnt", "enscnt",
              "fho", "grad", "isc", "mctc", "mcts", "mpr", "nbrcnt",
              "nbrctc", "nbrcts", "orank", "pct", "perc", "phist",
              "pjc", "prc", "pstd", "relp", "rhist", "rps", "sl1l2",
              "sal1l2", "vl1l2", "val1l2", "ssidx", "ssvar", "vcnt"]

for ltype in line_types:

    # rows can be created in different orders - match by data file
    q_line = 'SELECT * from line_data_' + ltype + \
             ', data_file where ' + \
             'line_data_' + ltype + \
             '.data_file_id = data_file.data_file_id ' + \
             'order by filename, line_num limit ' + \
             str(QUERY_COUNT) + ';'

    # show row counts
    print('\n*** Row counts for line_data_' + ltype + ' tables')

    result2 = run_query(q_line, cur2)
    result3 = run_query(q_line, cur3)

    if cur2.rowcount > 0 and cur3.rowcount > 0:

        i = 0
        same = True
        first = True

        # compare rows from the two databases
        for row in result2:

            if different(row[3:-6], row[3:-6]):
                same = False
                print("*** Different ***")
                if first:
                    for j in range(len(row) - 6):
                        print(str(row[j]) + " :: " + str(result3[i][j]))
                    first = False
                else:
                    print(row[3:-6])
                    print(" ")
                    print(result3[i][3:-6])

            i += 1

        if same:
            print("No differences for line_data_" + ltype)
        else:
            table_diff += 1

    else:
        print("One or both tables are empty")

# variable length records
vline_types = ["eclv_pnt", "mctc_cnt", "orank_ens",
               "pct_thresh", "phist_bin", "pstd_thresh", "pjc_thresh",
               "prc_thresh", "relp_ens", "rhist_rank"]

for ltype in vline_types:

    # get the name of matching line data table
    ptype = ltype.split('_')[0]

    field_counter = 'i_value'
    if ltype == "mctc_cnt":
        field_counter = 'i_value, j_value'

    # rows can be created in different orders - match by data file
    q_vline = 'SELECT line_data_' + ltype + \
              '.* from line_data_' + ptype + \
              ', line_data_' + ltype + ', data_file WHERE ' + \
              'line_data_' + ptype + \
              '.data_file_id = data_file.data_file_id AND ' + \
              'line_data_' + ptype + '.line_data_id = line_data_' + \
              ltype + '.line_data_id ' + \
              ' ORDER BY filename, line_num, line_data_' + ltype + \
              '.line_data_id, ' + field_counter + ' limit ' + \
              str(QUERY_COUNT) + ';'

    # show row counts
    print('\n*** Row count for line_data_' + ltype + ' tables')

    result2 = run_query(q_vline, cur2)
    result3 = run_query(q_vline, cur3)

    if cur2.rowcount > 0 and cur3.rowcount > 0:

        i = 0
        same = True

        # compare rows from the two databases
        for row in result2:

            if different(row[1:], result3[i][1:]):
                same = False
                print("*** Different ***")
                print(row[1:])
                print(result3[i][1:])

            i += 1

        if same:
            print("No differences for line_data_" + ltype)
        else:
            table_diff += 1

    else:
        print("One or both tables are empty")

# *** Count mode table rows
q_line2 = "SELECT table_name, table_rows FROM information_schema.tables " + \
          "WHERE table_schema = '" + DB2 + "' AND " + \
          "table_name LIKE 'mode\_%';"

q_line3 = "SELECT table_name, table_rows FROM information_schema.tables " + \
          "WHERE table_schema = '" + DB3 + "' AND " + \
          "table_name LIKE 'mode\_%';"

same = count_rows(q_line2, q_line3)

if same:
    print("\n%%% No differences in mode table row counts")
else:
    table_diff += 1

# *** mode_header records
q_header = 'SELECT mode_header.* from mode_header, data_file ' + \
           'where mode_header.data_file_id = data_file.data_file_id ' + \
           'order by path, filename, linenumber limit ' + \
           str(QUERY_COUNT) + ';'

# show row counts
print("\n*** Row counts for mode_header tables")

result2 = run_query(q_header, cur2)
result3 = run_query(q_header, cur3)

i = 0
same = True

# compare mode header lines from the two databases
for row in result2:

    rowl = row[3:]
    rowr = result3[i][3:]

    if not rowl == rowr:

        if i > QUERY_COUNT:
            continue
        same = False
        print("*** Different ***")
        print(rowl)
        print(rowr)

    i += 1

if same:
    print("No differences for mode_header")
else:
    table_diff += 1

# *** mode_cts records
q_text = 'SELECT * from mode_cts ' + \
         'order by total, fy_oy, field limit ' + \
         str(QUERY_COUNT) + ';'

# show row counts
print("\n*** Row counts for mode_cts tables")

result2 = run_query(q_text, cur2)
result3 = run_query(q_text, cur3)

i = 0
same = True

# compare mode cts lines from the two databases
for row in result2:

    rowl = row[1:]
    rowr = result3[i][1:]

    if different(rowl, rowr):

        if i > QUERY_COUNT:
            continue
        same = False
        print("*** Different ***")
        print(rowl)
        print(rowr)

    i += 1

if same:
    print("No differences for mode_cts")
else:
    table_diff += 1

# *** mode_obj_single records
q_text = 'SELECT * from mode_obj_single ' + \
         'order by object_id, object_cat, centroid_lat, centroid_lon limit ' + \
         str(QUERY_COUNT) + ';'

# show row counts
print("\n*** Row counts for mode_obj_single tables")

result2 = run_query(q_text, cur2)
result3 = run_query(q_text, cur3)

i = 0
same = True

# compare mode obj single lines from the two databases
for row in result2:

    rowl = row[2:]
    rowr = result3[i][2:]

    if different(rowl, rowr):

        if i > QUERY_COUNT:
            continue
        same = False
        print("*** Different ***")
        print(rowl)
        print(rowr)

    i += 1

if same:
    print("\nNo differences for mode_obj_single")
else:
    table_diff += 1
    
# *** mode_obj_pair records
q_text = 'SELECT * from mode_obj_pair ' + \
         'order by mode_header_id, mode_obj_fcst_id, ' + \
         'mode_obj_obs_id limit ' + \
         str(QUERY_COUNT) + ';'

# show row counts
print("\n*** Row counts for mode_obj_pair tables")

result2 = run_query(q_text, cur2)
result3 = run_query(q_text, cur3)

i = 0
same = True

# compare mode obj pair lines from the two databases
for row in result2:

    rowl = (row[1],) + row[3:]
    rowr = (result3[i][1],) + result3[i][3:]

    if different(rowl[3:], rowr[3:]):

        if i > QUERY_COUNT:
            continue
        same = False
        print("*** Different ***")
        print(rowl)
        print(rowr)

    i += 1

if same:
    print("No differences for mode_obj_pair")
else:
    table_diff += 1
    
# *** Count mtd table rows
q_line2 = "SELECT table_name, table_rows FROM information_schema.tables " + \
          "WHERE table_schema = '" + DB2 + "' AND " + \
          "table_name LIKE 'mtd\_%';"

q_line3 = "SELECT table_name, table_rows FROM information_schema.tables " + \
          "WHERE table_schema = '" + DB3 + "' AND " + \
          "table_name LIKE 'mtd\_%';"

same = count_rows(q_line2, q_line3)

if same:
    print("\n%%% No differences in mtd table row counts")
else:
    table_diff += 1
    
#
# *** mtd_header records
#
q_header = 'SELECT * from mtd_header ' + \
           'order by model, fcst_lead, fcst_valid, fcst_var, revision_id limit ' + \
           str(QUERY_COUNT) + ';'

# show row counts
print("\n*** Row counts for mtd_header tables")

result2 = run_query(q_header, cur2)
result3 = run_query(q_header, cur3)

i = 0
same = True

# compare mtd header lines from the two databases
for row in result2:

    rowl = row[4:]
    rowr = result3[i][4:]

    if not rowl == rowr:

        if i > QUERY_COUNT:
            continue
        same = False
        print("*** Different ***")
        print(rowl)
        print(rowr)

    i += 1

if same:
    print("No differences for mtd_header")
else:
    table_diff += 1

#
# *** mtd_2d_obj records
#
q_text = 'SELECT mtd_2d_obj.* from mtd_2d_obj, mtd_header, data_file ' + \
         'where mtd_header.data_file_id = data_file.data_file_id and ' + \
         'mtd_2d_obj.mtd_header_id = mtd_header.mtd_header_id ' + \
         'order by path, filename, linenumber, object_id, time_index ' + \
         'limit ' + str(QUERY_COUNT) + ';'

# show row counts
print("\n*** Row counts for mtd_2d_obj tables")

result2 = run_query(q_text, cur2)
result3 = run_query(q_text, cur3)

i = 0
same = True

# compare mtd_2d_obj lines from the two databases
for row in result2:

    rowl = row[1:]
    rowr = result3[i][1:]

    if different(rowl, rowr):

        if i > QUERY_COUNT:
            continue
        same = False
        print("*** Different ***")
        print(rowl)
        print(rowr)

    i += 1

if same:
    print("No differences for mtd_2d_obj")
else:
    table_diff += 1
    
#
# *** mtd_3d_obj_single records
#
q_text = 'SELECT * from mtd_3d_obj_single ' + \
         'order by object_id, object_cat, centroid_lat, centroid_lon ' + \
         'limit ' + str(QUERY_COUNT) + ';'

# show row counts
print("\n*** Row counts for mtd_3d_obj_single tables")

result2 = run_query(q_text, cur2)
result3 = run_query(q_text, cur3)

i = 0
same = True

# compare mtd_3d_obj_single lines from the two databases
for row in result2:

    rowl = (row[1],) + row[3:]
    rowr = (result3[i][1],) + result3[i][3:]

    if different(rowl[1:], rowr[1:]):

        if i > QUERY_COUNT:
            continue
        same = False
        print("*** Different ***")
        print(rowl)
        print(rowr)

    i += 1

if same:
    print("No differences for mtd_3d_obj_single")
else:
    table_diff += 1
    
#
# *** mtd_3d_obj_pair records
#
q_text = 'SELECT * from mtd_3d_obj_pair ' + \
         'order by object_id, object_cat limit ' + \
         str(QUERY_COUNT) + ';'

# show row counts
print("\n*** Row counts for mtd_3d_obj_pair tables")

result2 = run_query(q_text, cur2)
result3 = run_query(q_text, cur3)

i = 0
same = True

# compare mtd_3d_obj_pair lines from the two databases
for row in result2:

    rowl = (row[1],) + row[3:]
    rowr = (result3[i][1],) + result3[i][3:]

    if different(rowl[1:], rowr[1:]):

        if i > QUERY_COUNT:
            continue
        same = False
        print("*** Different ***")
        print(rowl)
        print(rowr)

    i += 1

if same:
    print("No differences for mtd_3d_obj_pair")
else:
    table_diff += 1
    
cur2.close
conn2.close()
cur3.close
conn3.close()
sys.exit(table_diff)
