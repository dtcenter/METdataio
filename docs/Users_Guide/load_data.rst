**********
Background
**********

The METdbLoad module provides support for inserting MET output data into a relational database
(mysql, mariadb, or aurora).

Before using the METdbLoad module, the database **must** exist and have the proper permissions
(i.e. grant privileges to insert, delete, update, and index).  Next, use *mv_mysql.sql* and the mysql command
line client (https://dev.mysql.com/doc/refman/en/mysql.html) to create the tables corresponding to the MET line types.
The *mv_mysql.sql* file is located in the METdataio/METdbLoad/sql/ directory.

The METdbLoad script *met_db_load.py* performs loading of data based on settings in an XML specification file.
The XML specification file contains database connection information, the location of data to be loaded, and other
settings relevant to the type of data that is being loaded. The XML specification is validated against a schema to check
that the file is valid.  This validation is necessary to prevent extremely large payloads or recursive
payloads that can compromise the loading of data.
The elements in the XML specification file **must** adhere to the **order specified** by the
XML schema and conform to size and number of element limitations.

.. dropdown:: This is an **example XML specification file** that is **valid**:

    .. literalinclude:: ../../METdbLoad/test/Examples/example_load_specification.xml



.. dropdown:: This is the **XML schema** that is used to **validate the XML specification file**:

    .. literalinclude:: ../../METdbLoad/ush/load_specification_schema.xsd

Create the XML Specification File
===================================

The XML specification file contains the database connection information, data file location, and instructions for
loading the data into the database.


Create your own XML specification file by copying the example specification file
*METdataio/METdbLoad/test/Examples/example_load_specification.xml* file to a
location in your workspace. This  file will contain the username and password to the database.

**Do not put this XML specification file where it can be read by anyone who should not have access to this information.**

.. code-block:: ini

   cp $METDATAIO_HOME/METdbLoad/test/Examples/example_load_specification.xml  path-to-your-dir/load_specification.xml


$METDATAIO is the path to the location of the cloned or forked METdataio source code.
Replace the *path-to-your-dir* with the actual path to where this file is to be saved.

Change directory to the location where the *example_load_specification.xml* file was copied. Make the necessary edits
to the required elements and delete any optional, unused/irrelevant elements.

.. dropdown:: The following is an explanation of the required and optional elements and any limitations

  *These are element names. The XML angle brackets (<>) as seen in the XML specification file are omitted*

   **load_spec**
      - **mandatory**
      - top-level tag

    *The following elements pertain to logging into the database*
      **connection**
         - **mandatory**
         - tag for connection information

         **management_system**
         - **optional**
         - indicates which database is in use
         - recognized/expected values are one of the following:

             - aurora
             - mysql
             - mariadb

         - delete this element if not using

         **host**
           - **mandatory**
           - name of host/machine where database is installed
           - format is *hostname*:*port number*
           - minimum number of characters is 3
           - maximum number of characters is 67
           - allowable characters (combinations of any of these):

            - upper and lower alphabetical characters (English)
            - digits 0-9
            - ., -, _ (period, dash, underscore)

         **database**
          - **mandatory**
          - name of the database
          - maximum number of characters for database name is 124
          - allowable characters (combination of any of these):
            - _,- (underscore, dash)
            - upper and lower case alphabetical characters (English)
            - digits 0-9


         **user**
          - **mandatory**
          - user name
          - minimum number of characters is 3
          - maximum number of characters is 32
          - allowable characters (combination of any of these):

            - upper and lower case alphabetical characters (English)
            - digits 0-9
            - _,- (underscore, dash)

         **password**
          - **mandatory**
          - the password to access the database
          - minimum number of characters is 3
          - maximum number of characters is 30
          - all characters are allowed


         **local_infile**
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

    *The following elements are used to define the format of multiple input data directories that are organized by datetime*
     **date_list**
       - **optional**
       - only necessary when input data is organized based on datetime
       - omit date_list entries if data resides in a singular directory
       - multiple date_list elements are allowed

         - maximum number of date_lists is 5
         - differentiate different date_list definitions by the *name* attribute (i.e. name=)

        **start**
          - **mandatory**  if date_list is being used
          - start datetime


        **end**
          - **mandatory** if date_list is being used
          - end datetime

        **inc**
          - **mandatory**  if date_list is being used
          - increment/step size between start and end time

             - Example, if 6-hour increment:
             - set inc to 0600
             - <inc>0600</inc>

        **format**
          - **mandatory** if date_list is being used
          - format of the datetime

            - Example, if 4 digit year month day hour:
               - <format>yyyyMMddHH</format>

    *The following elements define various flags*

     **verbose**
       - **mandatory**
       -  indicates the desired volume of output from the load module

              - TRUE resulting in more information
              - FALSE resulting in less information

     **insert_size**
      - **mandatory**
      - An integer indicating the number of MET output file rows inserted with each INSERT statement

          - This value is most often 1

     **stat_header_db_check**
       - **optional**
       - indicate whether a database query check for stat header information should be performed
       - True or False (case insensitive)

         - **WARNING** enabling this feature (i.e. set to True) could significantly increase load time

     **mode_header_db_check**
       - **optional**
       - indicate whether a database query check for the MODE header information should be performed
       - True or False (case insensitive)

         - **WARNING** enabling this feature (i.e. set to True) could significantly increase load time

     **mtd_header_db_check**
       - **optional**
       - indicate whether a database query check for the MODE TD header information should be performed
       - True or False (case insensitive)

         - **WARNING** enabling this feature (i.e. set to True) could significantly increase load time


     **drop_indexes**
       - **optional**
       - indicate whether to drop database indexes before loading new data
       - True or False (case insensitive)

     **apply_indexes**
       - **optional**
       - indicate whether to apply database indexes
       - True or False (case insensitive)

     **load_stat**
       - **optional**
       - indicate whether or not to load STAT data
       - True or False (case insensitive)

     **load_mode**
       - **optional**
       - indicate whether or not to load MODE data
       - True or False (case insensitive)

     **load_mtd**
       - **optional**
       - indicate whether or not to load MODE TD (MODE Time Domain) data
       - True or False (case insensitive)

     **load_mpr**
       - **optional**
       - indicate whether or not to load MPR (matched pair) data
       - True or False (case insensitive)

     **load_orank**
       - **optional**
       - indicate whether or not to load ORANK (observed rank) data
       - True or False (case insensitive)

     **force_dup_file**
       - **optional**
       - indicate whether or not to force load paths/files that already exist
       - True or False (case insensitive)

    *The following elements indicate which group the database should be assigned and a description*
     **group**
       - **optional**
       - if undefined, the database will be placed under the NO GROUP group
       - minimum number of characters is 1
       - maximum number of characters is 300
       - acceptable characters (English), any combination:

         - upper and/or lower case alphabetic characters
         - any digits 0-9
         - _, . , - (underscore, period, dash)

     **description**
       - **optional**
       - description of the data in the database
       - minimum number of characters is 1
       - maximum number of characters is 300
       - acceptable characters (English), any combination:

         - upper and/or lower case alphabetic characters
         - any digits 0-9
         - _, . , - (underscore, period, dash)

    *The following defines the location of the input data to be loaded into the database*

     **folder_tmpl**
      - **mandatory**
      - only one folder template element is permitted

        **field**





Load Data
=========

Now the MET data can be loaded in the database using the *met_db_load.py* script in the path-to-METdataio-source/METdbLoad/ush
directory.  The *path-to-METdataio-source* is the directory where the METdataio source code is saved.

.. code-block:: ini

  cd /path-to-METdataio-source/METdataio/METdbLoad/ush

  * Replace path-to-METdataio-source to the location where the METdataio source code is saved.

  python met_db_load.py /path-to/load_met.xml

  * Replace the path-to with the location where the load_met.xml file was saved.  This is the same directory
    you created to save the copy of the data_loading_config.yaml file.

The usage statement:

.. code-block:: ini

  INFO:root:--- *** --- Start METdbLoad --- *** ---

  usage: python met_db_load.py [-h] [-index] xmlfile [tmpdir [tmpdir ...]]

  positional arguments:
    xmlfile     Please provide required xml load_spec filename
    tmpdir      Optional - when different directory wanted for tmp file

  optional arguments:
    -h, --help  show this help message and exit
    -index      Only process index, do not load data

The **xmlfile** is the XML specification file that passes information about the MET output files to load
into the database to METdbload. It is an XML file whose top-level
tag is <load_spec> and it contains the following elements, divided into
functional sections:


  * **<load_stat>:** **TRUE** or **FALSE**, this option indicates whether or
    not to load STAT data.

  * **<load_mode>:** **TRUE** or **FALSE**, this option indicates whether or
    not to load MODE data.

  * **<load_mtd>:** **TRUE** or **FALSE**, this option indicates whether or
    not to load MODE TD data.

  * **<load_mpr>:** **TRUE** or **FALSE**, this option indicates whether or not
    to load matched pair data.

  * **<load_orank>:** **TRUE** or **FALSE**, this option indicates whether or
    not to load observed rank data.

  * **<force_dup_file>:** **TRUE** or **FALSE**, this option indicates whether
    or not to force load paths/files that are already present.

  * **<verbose>:** **TRUE** or **FALSE**, this option indicates the desired
    volume of output from the load module, with TRUE resulting in more
    information and FALSE resulting in less information.

  * **<insert_size>:** An integer indicating the number of MET output file rows
    that are inserted with each INSERT statement. This value is most often 1.

  * **<stat_header_db_check>:** **TRUE** or **FALSE**, this option indicates
    whether a database query check for stat header information should be
    performed - **WARNING:** enabling this feature could significantly
    increase load time.

    **NOTE:** **<stat_header_table_check>** has been removed; remove it
    from the XML load specification document.

  * **<mode_header_db_check>:** **TRUE** or **FALSE**, this option indicates
    whether a database query check for MODE header information should be
    performed - **WARNING:** enabling this feature could significantly
    increase load time.

  * **<mtd_header_db_check>:** **TRUE** or **FALSE**, this option indicates
    whether a database query check for MODE TD header information should
    be performed - **WARNING:** enabling this feature could significantly
    increase load time.

  * **<drop_indexes>:** **TRUE** or **FALSE**, this option indicates whether
    database indexes should be dropped prior to loading new data.

  * **<load_indexes>:** **TRUE** or **FALSE**, this option indicates whether
    database indexes should be created after loading new data.

  * **<group>:** The name of the group for the user interface.

  * **<description>:** A short description of the database.

    * **<load_files>:** A list structure containing individual MET output
      files to load into the database.

    * **</load_files>:** Follows the list of files after the previous
      tag, to end the list.

    * **<file>:** Contains a single MET output file to load.

  * **<folder_tmpl>:** A template string describing the file structure of
    the input MET files, which is populated with values specified in
    the **<load_val>** tag structure.

    * **<load_val>:** A tree structure containing values used to populate
      the **<folder_tmpl>** template.

      * **<field>:** A template value, its name is specified by the attribute
	name, and its values are specified by its children **<val>** tags.

        * **<val>:** A single template value which will slot into the template
	  in the value specified by the parent field's name.

        * **<date_list>:** Specifies a previously declared **<date_list>**
	  element, using the name attribute, which represents a list of dates
	  in a particular format.

      * **<line_type>:** A list structure containing the MET output file line
	types to load. If omitted, all line types are loaded.

        * **<val>:** Contains a single MET output file line type to be loaded,
	  for example, CNT.

    * **<load_note>:** If present, creates a record in the instance_info
      database database table with a note containing the body of this tag

    * **<load_xml>:   TRUE** or **FALSE**, this option indicates whether or
      not to save the load xml; only effective if **<load_note>** is present
      - default: TRUE

  **Note**
  If <folder_tmpl> is used, at least one <load_val> entry should be present.
  For example, if the path is:

  .. code-block:: XML

    <folder_tmpl>/path/to/data</folder_tmpl>

  change it to

  .. code-block:: XML

    <folder_tmpl>/path/to/{type}</folder_tmpl>
    <load_val>
       <field name="type">
          <val>data</val>
       </field>
    </load_val>


Additional Loading Options
--------------------------

The load_met.xml specification file created above loads the entire dataset specified in the data_dir setting in the
YAML config file, data_loading_config.yaml.

A subset of the data can be selected by date and field names (i.e. by model, valid_time, vx_mask, etc.).
The load_met.xml specification file can be further modified to accomplish this by adding the date_list and
field_name elements to the XML specification file.

Here is a simple example:

.. code-block:: XML

  <load_spec>
    <connection>
      <host>kemosabe:3306</host>
      <database>mv_db_hwt</database>
      <user>pgoldenb</user>
      <password>pgoldenb</password>
    </connection>

    <date_list name="folder_dates">
      <start>2010051914</start>
      <end>2010051915</end>
      <inc>3600</inc>
      <format>yyyyMMddHH</format>
    </date_list>


    <verbose>false</verbose>
    <insert_size>1</insert_size>
    <mode_header_db_check>true</mode_header_db_check>
    <drop_indexes>false</drop_indexes>
    <apply_indexes>true</apply_indexes>
    <group>Group name</group>
    <load_stat>true</load_stat>
    <load_mode>true</load_mode>
    <load_mtd>true</load_mtd>
    <load_mpr>false</load_mpr>

    <folder_tmpl>/d1/data/{model}/{vx_mask}/{valid_time}</folder_tmpl>
    <load_val>
      <field name="model">
        <val>arw</val>
        <val>nmm</val>
      </field>

      <field name="valid_time">
        <date_list name="folder_dates"/>
      </field>

      <field name="vx_mask">
         <val>FULL</val>
         <val>SWC</val>
      </field>
    </load_val>
  </load_spec>


In this example, the load module would attempt to load any files with the
suffix .stat in the following folders.

.. code-block:: ini

  /d1/data/arw/FULL/2010051914
  /d1/data/arw/SWC/2010051914
  /d1/data/nmm/FULL/2010051914
  /d1/data/nmm/SWC/2010051914
  /d1/data/arw/FULL/2010051915
  /d1/data/arw/SWC/2010051915
  /d1/data/nmm/FULL/2010051915
  /d1/data/nmm/SWC/2010051915
  ...

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
