***************
Using METdbLoad
***************

The METdbLoad module provides support for inserting MET output data (as .stat files) into a relational database
(mysql, mariadb, or aurora).


Create Database
---------------

Before using the METdbLoad module, the database **must** exist and have the proper permissions
(i.e. grant privileges to insert, delete, update, and index).


Data must be loaded into a database which has the prefix \'\mv_\'\,
(e.g. mv_met_data). This database must be structured with the METviewer
mv_mysql.sql schema:

.. code-block:: ini

  cd [install]/metviewer
  mysql -u[db_username] -p[db_password] -e'create database [db_name];'
  mysql -u[db_username] -p[db_password] [db_name] < sql/mv_mysql.sql


- Replace *db_usenamer* with the username

- Replace *db_passwd* with the password

- Replace *db_name* with the appropriate database name that begins with **mv** (e.g. mv_xyz )

- Repace *database* with the database name


.. note::
 Use the mysql command line client (https://dev.mysql.com/doc/refman/en/mysql.html) to run the above instructions.

 The *mv_mysql.sql* file is located in the METdataio/METdbLoad/sql/ directory.

.. dropdown:: Create MET linetype tables

 mysql -udbuser -pdbpasswd dbtable_name < /path-to-METdataio/METdbLoad/sql/mv_mysql.sql



    - Replace *path-to-METdataio* with the path where the METdataio source code is saved
    - Note the use of the redirection symbol '<' in the command

Create an XML Load Specification Document
-----------------------------------------

The XML load specification document contains information about the MET data, database connection information, and the location of data to be loaded.
This is used by the METdbLoad script *met_db_load.py* to load the MET data. The XML load specification is validated against a schema to check
that the file is valid.  This validation is necessary to prevent extremely large payloads or recursive
payloads that can compromise the loading of data.
The elements in the XML load specification file **must** adhere to the **order specified** by the
XML schema and conform to size and number of element limitations.
Click to expand the example XML load specifications and the XML validation schema below.

.. dropdown:: An **example XML specification file** that is **valid**

    For defining data organized by dates using the folder_tmpl element:

    .. literalinclude:: ../../METdbLoad/test/Examples/example_load_specification.xml

.. dropdown:: Another **example XML specification file** that is **valid**

    For specifying a list of input data files using the load_files element:

    .. literalinclude:: ../../METdbLoad/test/Examples/example_load_specific_files.xml



.. dropdown:: The **XML schema** that is used to **validate the XML specification file**

    .. literalinclude:: ../../METdbLoad/ush/load_specification_schema.xsd

Create the XML Specification File
===================================

The XML load specification file contains the database connection information, data file location, and other instructions for
loading the data into the database.


Create the  XML load specification file by copying the example specification file
*METdataio/METdbLoad/test/Examples/example_load_specification.xml* file to the workspace location.

**Do not save this XML specification file where it can be read by anyone who should not have access to this information,
as this file contains the database password.**

.. code-block:: ini

   cp $METDATAIO_HOME/METdbLoad/test/Examples/example_load_specification.xml  path-to-your-dir/load_specification.xml


- $METDATAIO is the path to the location of the cloned or forked METdataio source code.

- Replace the *path-to-your-dir* with the actual path to where this file will be saved.

Change directory to the location where the *example_load_specification.xml* file was copied.

.. code-block:: ini

   cd path-to-your-dir/load_specification.xml

- Replace *path-to-your-dir* with the full path where the XML specification file will be saved.

Make the necessary edits to the required elements and delete any optional, unused/irrelevant elements, based on the explanation below (click to
expand). Remember to update the username and password that is applicable to the database.


.. note::
 The **order of the elements** in the XML load specification file is crucial. **DO NOT** modify the order of the described elements (below).



.. dropdown:: The following is an explanation of the required and optional elements and any limitations

  *These are element names. The XML angle brackets (<>) as seen in the XML load specification file are omitted for simplicity. Indentation is used to indicate hierarchical relationships between elements.*

    .. dropdown::   load_spec

      - **mandatory**
      - top-level tag/element
      - container for other elements that define connection information, flags, data input, etc.

    *The following elements pertain to logging into the database.*

         .. dropdown:: connection

            - **mandatory**
            - tag for connection information

           .. dropdown:: management_system

              - **optional**
              - indicates which database is in use
              - recognized/expected values are one of the following:

                  - aurora
                  - mysql
                  - mariadb

              - delete this element if not using

           .. dropdown::  host

                - **mandatory**
                - name of host/machine where database is installed
                - format is *hostname*:*port number*
                - minimum number of characters is 3
                - maximum number of characters is 67
                - allowable characters (combinations of any of these):

                 - upper and lower alphabetical characters (English)
                 - digits 0-9
                 - ., -, _ (period, dash, underscore)

           .. dropdown:: database

              - **mandatory**
              - name of the database
              - maximum number of characters for database name is 124
              - allowable characters (combination of any of these):
                 - _,- (underscore, dash)
                 - upper and lower case alphabetical characters (English)
                 - digits 0-9

           .. dropdown:: user

              - **mandatory**
              - user name
              - minimum number of characters is 3
              - maximum number of characters is 32
              - allowable characters (combination of any of these):

                 - upper and lower case alphabetical characters (English)
                 - digits 0-9
                 - _,- (underscore, dash)

           .. dropdown::  password

             - **mandatory**
             - the password to access the database
             - minimum number of characters is 3
             - maximum number of characters is 30
             - all characters are allowed

           .. dropdown:: local_infile

             - **optional**
             - argument passed into 3rd party Python library pymysql

               - for establishing a connection to a MySQL server
               - indicate whether the input file is local
               - default is False
               - enables use of the LOAD DATA LOCAL command

               - Accepted value:
                 - Boolean value: True or False

                   - True if loading local data
                   - False otherwise

               - delete this element if loading of local data is not needed

                  - METdataio sets default to False if this element is absent

    *The following elements are used to define the format of multiple input data directories that are (optionally) organized by datetime.*

         .. dropdown:: date_list

            - **optional**
            - for describing data organized in datetime subdirectories
            - omit date_list entries if data resides in a singular directory
            - multiple date_list elements are allowed

              - maximum number of date_lists is 5
              - differentiate different date_list definitions by the *name* attribute (i.e. name=)

                **Example**:

               /var/autofs/mnt/hostmachine/projects/RRFS/prototype/met_out/{config}/{fcst_init}/{mem}/{valid_times}/metprd/{met_out}

                  - the *fcst_init* and *valid_times* subdirectories are based on datetime
                  - assign the fcst_init subdirectory to a descriptively named date_list attribute:

                        e.g. <date_list name="folder_dates">

                        - this attribute name will be used in the load_val element within the folder_tmpl element to describe the
                          {fcst_init} subdirectory template

                  - assign the valid_times subdirectory to a descriptively named date_list attribute:

                        e.g. <date_list name="valid_dates">

                        - this attribute name will be used in the load_val element within the folder_tmpl element to describe the {valid_times} subdirectory template

          .. dropdown:: start

              - **mandatory**  if date_list is being used
              - start datetime

          .. dropdown:: end

             - **mandatory** if date_list is being used
             - end datetime

          .. dropdown:: inc

             - **mandatory**  if date_list is being used
             - increment/step size between start and end time

               - Example, if 6-hour increment:
               - set inc to 0600
               - <inc>0600</inc>

          .. dropdown:: format

             - **mandatory** if date_list is being used
             - format of the datetime

               - For example, to specify 4 digit year, 2 digit month, 2 digit day, and 2 digit hour:
                  - <format>yyyyMMddHH</format>

    *The following elements define various flags*

     .. dropdown:: verbose

          - **mandatory**
          -  indicates the desired volume of output from the load module

              - TRUE resulting in more information
              - FALSE resulting in less information

     .. dropdown:: insert_size

          - **mandatory**
          - An integer indicating the number of MET output file rows inserted with each INSERT statement

            - This value is most often 1

     .. dropdown:: stat_header_db_check

         - **optional**
         - indicate whether a database query check for stat header information should be performed
         - True or False (case insensitive)

           - **WARNING** enabling this feature (i.e. set to True) could significantly increase load time

     .. dropdown:: mode_header_db_check

       - **optional**
       - indicate whether a database query check for the MODE header information should be performed
       - True or False (case insensitive)

         - **WARNING** enabling this feature (i.e. set to True) could significantly increase load time

     .. dropdown:: mtd_header_db_check

       - **optional**
       - indicate whether a database query check for the MODE TD header information should be performed
       - True or False (case insensitive)

         - **WARNING** enabling this feature (i.e. set to True) could significantly increase load time

     .. dropdown::  drop_indexes

       - **optional**
       - indicate whether to drop database indexes before loading new data
       - True or False (case insensitive)

     .. dropdown:: apply_indexes

       - **optional**
       - indicate whether to apply database indexes
       - True or False (case insensitive)

     .. dropdown:: load_stat

       - **optional**
       - indicate whether or not to load STAT data
       - True or False (case insensitive)

     .. dropdown:: load_mode

       - **optional**
       - indicate whether or not to load MODE data
       - True or False (case insensitive)

     .. dropdown:: load_mtd

       - **optional**
       - indicate whether or not to load MODE TD (MODE Time Domain) data
       - True or False (case insensitive)

     .. dropdown:: load_mpr

       - **optional**
       - indicate whether or not to load MPR (matched pair) data
       - True or False (case insensitive)

     .. dropdown:: load_orank

       - **optional**
       - indicate whether or not to load ORANK (observed rank) data
       - True or False (case insensitive)

     .. dropdown:: force_dup_file

       - **optional**
       - indicate whether or not to force load paths/files that already exist
       - True or False (case insensitive)

    *The following elements indicate which group the database should be assigned and a description.*

     .. dropdown::  group

       - **optional**
       - the name of the database group (databases are grouped in METviewer: e.g. Testing)
       - if undefined, the database will be placed under the NO GROUP group
       - minimum number of characters is 1
       - maximum number of characters is 300
       - acceptable characters (English), any combination:

         - upper and/or lower case alphabetic characters
         - any digits 0-9
         - _, . , - (underscore, period, dash)

     .. dropdown::  description

       - **optional**
       - description of the database
       - minimum number of characters is 1
       - maximum number of characters is 300
       - acceptable characters (English), any combination:

         - upper and/or lower case alphabetic characters
         - any digits 0-9
         - _, . , - (underscore, period, dash)

    **Determine whether to use folder_tmpl or load_files to define where the input data resides (descriptions below):**

    *The following defines the location of the input data to be loaded into the database based on data organized by datetime (and/or any other criteria).*

     .. dropdown:: folder_tmpl

        - for data in subdirectories that are datetimes *OR* data that resides under one directory
        - only one folder template element is permitted (i.e. only one <folder_tmpl> ... </folder_tmpl> )
        - **NOTE** the *date_list* element **MUST BE DEFINED** (see above in the *date_list* description) if **any subdirectories are based on datetime**

        *Specify the directory where the data is located in one of the following methods:*

          .. dropdown:: Using value templates for directories:

            Example:

            **/var/autofs/mnt/hostmachine/projects/RRFS/prototype/met_out/{config}/{fcst_init}/{mem}/{valid_times}/metprd/{met_out}**

              - data is organized into various directories based on datetime, and other criteria

              - use { } around "variable" names (in XML, these indicate attribute value templates)

                **config**, **fcst_init**, **mem**, **valid_times**, and **met_out** are attribute value template values that *must* be defined under the load_val element (for more details, refer to the *load_val* description below)

          .. dropdown:: Specify a single directory where all data reside:

            Example:

             **/var/autofs/mnt/hostmachine/projects/RRFS/prototype/met_out/mem00/metprd/all_runs**

               - **all** datafiles are located under this directory (indicate the full path)

      .. dropdown:: load_val

        - **optional** if *folder_tmpl* specifies a single directory where all data resides

        - **mandatory** if folder_tmpl has datetime subdirectories
           - *field* elements correspond to each attribute value template (i.e. variable names enclosed in {})


        .. dropdown:: field

          - **mandatory** if *folder_tmpl* has subdirectories that are datetimes
          - each *field* element defines the attribute value template in the directory structure (i.e. the variable inside the {})
            - *val* elements that can specify more subdirectories

              - **optional**
              - necessary when specifying subdirectories that are not datetimes (e.g. /path-to/model_A/air_quality/)
              - maximum number of val elements: 100

            - *date_list* elements for subdirectories that are datetimes

              - **optional**
              - mandatory when subdirectories are datetimes (e.g. /path-to/20240101/model/)
              - maximum number of date_list elements: 5

          *For this folder_tmpl example, which has datetime subdirectories:*

           .. dropdown:: /var/autofs/mnt/hostmachine/projects/RRFS/prototype/met_out/{config}/{fcst_init}/{mem}/{valid_times}/metprd/{met_out}

            *The following are the name attributes for the field* element for the above example:

            .. dropdown:: config

              - corresponds to the {config} template:

                 <field name="config">

               .. dropdown:: val

                 - for defining non-datetime subdirectories

                   - maximum number of *val* elements is 100

                 e.g.:
                     <field name="config">

                       <val>HREF_lag_offset</val>

                       <val>RTPS</val>

                     </field>

            .. dropdown:: fcst_init

               - corresponds to the {fcst_init} template:

                  <field name="fcst_init">

              .. dropdown:: date_list

                  - the *name* attribute corresponds to one of the *date_list* attribute names

                      <field name="fcst_init">

                            **<date_list name="folder_dates"/>**

                       </field>

                    .. dropdown:: The "folder_dates" name attribute was assigned in the date_list portion of the XML load specification file

                        **<date_list name="folder_dates">**

                           <start>2022050100</start>

                           <end>2022051200</end>

                           <inc>86400</inc>

                           <format>yyyyMMddHH</format>

                        </date_list>


            .. dropdown:: mem

              - corresponds to the {mem} template:

                 <field name="mem">

              .. dropdown:: val

                 - for defining non-datetime subdirectories

                   - maximum number of *val* elements is 100

                    e.g.:

                      <field name="mem">

                            <val>mem01</val>

                            <val>mem02</val>

                            <val>mem03</val>

                            <val>mem04</val>

                            <val>mem05</val>

                            <val>mem06</val>

                            <val>mem07</val>

                            <val>mem08</val>

                            <val>mem09</val>

                            <val>mem10</val>

                       </field>


            .. dropdown:: valid_times

                - corresponds to the {valid_times} template:

                   <field name="valid_times">

                .. dropdown:: date_list

                   - the *name* attribute corresponds to one of the *date_list* attribute names

                       <field name="valid_times">

                        **<date_list name="valid_dates">**

                       </field>

                      .. dropdown:: the "valid_dates" name attribute value was assigned in the date_list portion of the XML load specification file

                             **<date_list name="valid_dates">**

                                 <start>2022050100</start>

                                 <end>2022051200</end>

                                 <inc>0600</inc>

                                 <format>yyyyMMddHH</format>

                             </date_list>

            .. dropdown:: met_out

               - corresponds to the {met_out} template



               .. dropdown:: val

                   - for defining non-datetime subdirectories

                     - maximum number of *val* elements is 100

                    e.g.:

                       <field name="met_out">

                         <val>grid_stat_cmn</val>

                         <val>point_stat_cmn</val>

                       </field>

          *For this folder_tmpl example, which has all the data under one directory (no datetime subdirectories):*

            .. dropdown:: /var/autofs/mnt/hostmachine/projects/RRFS/prototype/met_out/metprd/point_stat





    *The following defines the location of specific input data files to be loaded into the database.*

     .. dropdown:: load_files

        - for specifying the location of individual data files that are in different directories

        .. dropdown:: file

           - **mandatory** for defining location of specific data files

             - maximum number of data files: 200

             Example:

                <load_files>

                    <file>/met_out/mode/mode_MASK_POLY_300000L_20120410_180000V_060000A_cts.txt</file>

                    <file>/met_out/rhist/ensemble_stat_RRFS_GEFS_GF.SPP.SPPT_RETOP_MRMS_20220507_120000V.stat</file>

                </load_files>

    *The following describe the linetypes to load.*

     .. dropdown:: line_type

        - **optional**
        - which MET output linetypes to load
        - if omitted, then **all linetypes** will be loaded
        - maximum number of line_type elements: 1

         *val* element defines which MET output linetypes to load

        .. dropdown:: val

          - the MET output linetype

            - linetype name (refer to the MET User's Guide for a complete list of linetypes)


              Example:

                   <line_type>

                        <val>ECNT</val>

                        <val>VL1L2</val>

                        <val>SAL1L2</val>

                   </line_type>

    *The following option allows one to indicate whether to save the XML commands into the database.*

     .. dropdown:: load_xml

         - **optional**
         - Option to save the XML  into the database
         - Only takes effect when the *load_note* element is present
         - Acceptable values: TRUE or FALSE (case-insensitive)
         - Default value: TRUE

         Example:

         <load_xml>true</load_xml>


    *The following allows one to create a note into the instance_info database table.*

     .. dropdown:: load_note

         - **optional**
         - Add a descriptive "note" into the database

         Example:

         <load_note>Load HREF and RTPS data from Spring 2022</load_note>


Load Data
=========

Now the MET data can be loaded in the database using the *met_db_load.py* script in the path-to-METdataio-source/METdbLoad/ush
directory.  The *path-to-METdataio-source* is the directory where the METdataio source code is saved.

.. note::
 Only data files with the *.stat* extension will be loaded


.. dropdown:: The usage statement for met_db_load.py:

    .. code-block:: ini

       INFO:root:--- *** --- Start METdbLoad --- *** ---

       usage: python met_db_load.py [-h] [-index] xmlfile [tmpdir [tmpdir ...]]

       positional arguments:
         xmlfile     Please provide required xml load_spec filename
         tmpdir      Optional - when different directory wanted for tmp file

       optional arguments:
         -h, --help  show this help message and exit

         -index      Only process index, do not load data


.. code-block:: ini

  cd /path-to-METdataio-source/METdataio/METdbLoad/ush

  python met_db_load.py /path-to/load_specification.xml

If logging is desired, redirect output to a log file (via &> command):

.. code-block:: ini

  python met_db_load.py /path-to/load_specification.xml  &> log/your_logname.log &

- Replace *path-to-METdataio-source* to the location where the METdataio source code is saved.
- Replace the *path-to* with the location where the load_specification.xml XML load specification file was saved.

Refer to the section **Create the XML Specification File** and expand the drop-down instructions
"The following is an explanation of the required and optional elements and any limitations" for details on what is expected in the XML load specification file.






Troubleshooting
---------------

.. _test:

.. list-table::

  * -  Error:
    -  **ERROR: Caught class
       com.mysql.jdbc.exceptions.MySQLIntegrityConstraintViolationException:
       Duplicate entry
       'CT07-NMM-LIN-R2-0-2005-07-15 12:00:00-2005-07-15 12:00:00-0-2005'
       for key 2**

  * - Solution:
    - This error is caused by trying to insert a stat_header record into
      the database when an identical one already exists. If identical
      stat_header information is present in more than one stat file, set
      the <stat_header_db_check> value to true. This setting will reduce
      performance, because the stat_header table is checked for duplicate
      stat_header each time a row is inserted. However, if a stat_header
      row already exists in the table with the insert information, then
      the existing record will be used instead of trying to insert a
      duplicate.

  * -  Error:
    - **ERROR:root: (1049, "Unknown database 'mv_test'") in run_sql Error when connecting to database**

  * - Solution:
    - This error is caused when attempting to load data into a database that does not exist.  You will need to create the database, set up the appropriate privileges as outlined above, and load the schema using the mv_mysql.sql file.

  * -  Error:
    -  /full-path-to/xyz.xml is not valid and may contain a recursive payload or an excessively large payload

  * - Solution:
    - This error is typically encountered when one of the following conditions exist as a result of failing the XML validation step:

         - the order of the elements in the XML load specification file is inconsistent with the order expected
         - the XML load specification file is missing one or more mandatory elements
         - one or more elements has exceeded size limits specified in the XML schema
         - there are additional XML elements that are not expected

       **Refer to the section **Create the XML Load Specification File** to verify that the XML load specification file is correct.**
