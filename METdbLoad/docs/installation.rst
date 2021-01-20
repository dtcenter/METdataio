.. _install:

METdbload Installation Guide
============================

Introduction
------------

This chapter describes how to install METdbload.
METdbload has been developed and tested on Mac and Linux operating systems.
Support for additional platforms may be added in future releases.
METdbload is a Python 3 program that requires some extra packages to be available on the user's computer prior to installation.
METdbload is part of the METdatadb database package. Additional METdatadb programs that work with the data are planned.

Installing METdbload
--------------------

METdbload relies on the following tools. These must be installed and tested prior to installing METdbload:

**MySQL** - download and install the latest version. This can be on a separate computer. Use "SET GLOBAL max_allowed_packet=110000000;" by typing the command in MySQL and/or make the corresponding edit to /etc/my.cnf, so that the change persists after the next reboot.

**Python 3.6+** - Python 3.6 or higher must be installed. METdbload also requires the Python packages pymysql, pandas, numpy, and lxml.

Download the application:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Download METdatadb into an installation directory. Programs can be run from the ush subdirectory.

.. code-block:: ini

  cd [install]/METdatadb/METdbLoad/ush

Create a METdatadb database:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Data must be loaded into a MySQL database which has the prefix \'\mv_\'\, e.g. mv_met_data. This database must be structured with the METviewer mv_mysql.sql schema:

.. code-block:: ini

  cd [install]/metviewer
  mysql -u[db_username] -p[db_password] -e'create database [db_name];'
  mysql -u[db_username] -p[db_password] [db_name] < sql/mv_mysql.sql

Create an XML load specification document which contains information about your MET data. Run METdbload using the XML load specification as input (called [load_xml] here) and monitor progress:

.. code-block:: ini

  python met_db_load.py [load_xml] [optional redirection of output, e.g. &> log/load_[date].log &]'

Install test directory (for development, optional):
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Check out test_data (.../apps/verif/metviewer_test_data/test_data/) from CVS and move test_data directory to /d3/projects/METviewer/: create links to R script and sql files.

 .. code-block:: ini

  cd /d3/projects/METviewer/test_data $ ln -s /d3/projects/METviewer/src_dev/apps/verif/metviewer/R_tmpl R_tmpl $ mkdir R_work
  cd R_work $ mkdir data $ mkdir plots $ mkdir scripts $ ln -s /d3/projects/METviewer/src_dev/apps/verif/metviewer/R_work/include/ include
  cd /d3/projects/METviewer/test_data/load_data/load $ ln -s /d3/projects/METviewer/src_dev/apps/verif/metviewer/sql/mv_mysql.sql mv_mysql.sql

Making a Database Accessible in the METviewer Web Application:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To make a new database accessible in the METviewer Web Application click on "Reload list of databases" button in the upper right corner of the main JSP page. The list of available databases should be updated and a new database should be in it.