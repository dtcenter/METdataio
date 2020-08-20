=====================
Testing for METdbLoad
=====================

There is a program called test_tables.py in the METdbLoad/tests folder in the METdatadb repository. It will compare one MySQL subdatabase to another, comparing the row counts and field values. It will compare specified header tables and line type tables.

Generally, if two different programs (or two versions of the same program) are used to load data (such as MVLoad and METdbLoad), then row counts and the data in rows should be the same. Because of floating point errors, a Python 3.6 and later function (math.isclose) is used to compare floating point numbers such as 1.2 and 1.19999999.

This is a flexible testing approach, that can be used with any load data. The data should be the same for both loads. The load_spec should be the same except for the subdatabase name. Tatiana Burek of NCAR has a large set of test data that exercises most of the existing line types. This test data is updated as needed when new line types are added to MET. Usually the schema of the two subdatabases should be the same, except when a new field or line type has been added and an older version is being compared to a newer version.

Testing with MySQL requires one or two configuration files - specified for each MySQL subdatabase. Edit the name of the configuration file in the value of conn2 for the first subdatabase, and conn3 for the second subdatabase. Set DB2 and DB3 to the names of the subdatabases.

The variable line_types holds the names of the line types in lower case. These are added to 'line\_data\_' to make the names of the tables holding the line types. If a new line type is created, add the name to this variable. They are currently in alphabetical order, and will print out in the order in the variable.

The variable vline\_types holds the part of the variable line table names after 'line\_data\_'. If a new variable line type is created, add the name to this variable.

There is a variable QUERY_COUNT which limits the number of rows to compare. If a different number of lines is desired, adjust this number.
