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

    For defining data organized by dates using the folder_tmpl element:

    .. literalinclude:: ../../METdbLoad/test/Examples/example_load_specification.xml

.. dropdown:: This is another **example XML specification file** that is **valid**:

    For specifying a list of input data files using the load_files element:

    .. literalinclude:: ../../METdbLoad/test/Examples/example_load_specific_files.xml



.. dropdown:: This is the **XML schema** that is used to **validate the XML specification file**:

    .. literalinclude:: ../../METdbLoad/ush/load_specification_schema.xsd

Create the XML Specification File
===================================

The XML specification file contains the database connection information, data file location, and instructions for
loading the data into the database.


Create your own XML specification file by copying the example specification file
*METdataio/METdbLoad/test/Examples/example_load_specification.xml* file to a
location in your workspace. This  file will contain the username and password to the database.

**Do not save this XML specification file where it can be read by anyone who should not have access to this information.**

.. code-block:: ini

   cp $METDATAIO_HOME/METdbLoad/test/Examples/example_load_specification.xml  path-to-your-dir/load_specification.xml


- $METDATAIO is the path to the location of the cloned or forked METdataio source code.

- Replace the *path-to-your-dir* with the actual path to where this file is to be saved.

Change directory to the location where the *example_load_specification.xml* file was copied. Make the necessary edits
to the required elements and delete any optional, unused/irrelevant elements.

.. dropdown:: The following is an explanation of the required and optional elements and any limitations

  *These are element names. The XML angle brackets (<>) as seen in the XML specification file are omitted for simplicity*

  **!!!NOTE!!!**

  The **order of the elements** in the XML specification file is crucial. **DO NOT** modify the order of the following elements.
  Indentation is used to indicate hierarchical relationships between elements.

    .. dropdown::   load_spec

      - **mandatory**
      - top-level tag/element
      - container for other elements that define connection information, flags, data input, etc.

    *The following elements pertain to logging into the database*

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

    *The following elements are used to define the format of multiple input data directories that are (optionally) organized by datetime*

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

    *The following elements indicate which group the database should be assigned and a description*

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

    *The following defines the location of the input data to be loaded into the database based on data organized by datetime (and any other criteria)*

     .. dropdown:: folder_tmpl

        - **mandatory** only if data is organized in directories that can be described by templates
        - only one folder template element is permitted (i.e. only one <folder_tmpl> ... </folder_tmpl> )
        - **NOTE** the *date_list* element **MUST BE DEFINED** (see above in the *date_list* description) if any subdirectories are based on datetime

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
           - *field* sub-elements correspond to each attribute value template (i.e. variable names enclosed in {})


        .. dropdown:: field

          - **mandatory** if *folder_tmpl* has subdirectories that are datetimes
          - each *field* element defines the attribute value template in the directory structure (i.e. the variable inside the {})
          - *field* elements can have one or more *val* sub-elements that can specify more subdirectories
          - *field* elements can have one or more *date_list* sub-elements for subdirectories that are datetimes

          *For this folder_tmpl example:*

           **/var/autofs/mnt/hostmachine/projects/RRFS/prototype/met_out/{config}/{fcst_init}/{mem}/{valid_times}/metprd/{met_out}**

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

                    - in this case, this corresponds to the *folder_dates* attribute name:

                        <date_list name="folder_dates">

                           <start>2022050100</start>
                           <end>2022051200</end>
                           <inc>86400</inc>
                           <format>yyyyMMddHH</format>

                        </date_list>


                    if *fcst_init* subdirectory is based on the *date_list* named *folder_dates*

                    then the following is expected

                       <field name="fcst_init">

                            <date_list name="folder_dates"/>

                       </field>

            .. dropdown:: mem

              - corresponds to the {mem} template:

                 <field name="mem">

                - *val* element to specify subdirectories that are NOT datetimes

                  - maximum number of vals: 100

            .. dropdown:: valid_times

                - since this is a datetime subdirectory, a *date_list* element is expected
                   - <date_list name="valid_dates"/>
                   - the "valid_dates" name attribute value matches what is defined in the *date_list* element at the top of the XML specification file

            .. dropdown:: met_out

               .. dropdown:: val

                  - one or more *val* elements
                  - define any other subdirectories that are NOT datetimes



    *The following defines the location of specific input data files to be loaded into the database*

      .. dropdown:: load_files

        - **mandatory** if specifying individual data file(s)



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




    * **<load_files>:** A list structure containing individual MET output
      files to load into the database.

    * **</load_files>:** Follows the list of files after the previous
      tag, to end the list.

    * **<file>:** Contains a single MET output file to load.



      * **<field>:** A template value, its name is specified by the attribute	name, and its values are specified by its children **<val>** tags.

        * **<val>:** A single template value which will slot into the template	  in the value specified by the parent field's name.

        * **<date_list>:** Specifies a previously declared **<date_list>**	  element, using the name attribute, which represents a list of dates	  in a particular format.

      * **<line_type>:** A list structure containing the MET output file line	types to load. If omitted, all line types are loaded.

        * **<val>:** Contains a single MET output file line type to be loaded,	  for example, CNT.

    * **<load_note>:** If present, creates a record in the instance_info
      database database table with a note containing the body of this tag

    * **<load_xml>:   TRUE** or **FALSE**, this option indicates whether or
      not to save the load xml; only effective if **<load_note>** is present
      - default: TRUE










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
