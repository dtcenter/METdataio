Reformat MET Point Stat Data
=============================


Description
___________


The *METreformat* module is used to reformat MET point_stat .stat files into a format that can be read by
METplotpy.  The reformatted data contains additional columns:

Two columns for statistics

- stat_name

- stat_value

Four columns for confidence levels (normal and bootstrap)

- stat_bcl

    - upper level bootstrap confidence limit

- stat_bcu

    - lower level bootstrap confidence limit

- stat_ncl

   - lower level normal confidence limit

- stat_ncu

  - upper level normal confidence limit

These columns are needed for the *METplotpy* plots that are
used by METviewer (these plots can also be generated from the command line).

The following MET line types* can be reformatted:

- FHO

- CNT

- CTC

- CTS

- SL1L2

*Other line types will be supported in the future.

Required Components
___________________

Some METdbLoad modules are used to find and collect data from the individual .stat files into
one data structure.

Two files are required:

- an XML specification file (a simplified version of what is used by METdbLoad to read in the .stat files)

- a YAML configuration file

The XML specification file is used to indicate the directory where the .stat file(s) are
located and the type of MET files to read.  The YAML configuration file is used to indicate the name and
location of the output file, and the location of the XML specification file.

Example
_______

- set up a base directory, where the METdataio source code is located


.. code-block:: ini

   bash:
   export BASE_DIR=/path/to/METdataio

   csh:
   setenv BASE_DIR /path/to/METdataio

- replace /path/to with an actual path

- set up a working directory, where the XML and YAML config files will be located

.. code-block:: ini

   bash:
   export WORKING_DIR=/path/to/working_dir

   csh:
   setenv WORKING_DIR /path/to/working_dir

- copy the point_stat.xml and point_stat.yaml files to the WORKING_DIR

.. code-block:: ini

   cp $BASE_DIR/METdataio/METreformat/point_stat.xml $WORKING_DIR/
   cp $BASE_DIR/METdataio/METreformat/point_stat.yaml $WORKING_DIR/


- modify the XML specification file

  replace the content between the <folder_tmpl> </folder_tmpl> tags with the full path to the .stat files to be reformatted:

  for example:

.. code-block:: ini

    <folder_tmpl>/path/to/METdataio/METreformat/test/data/point_stat</folder_tmpl>

- replacing the /path/to with the directory where you saved the source code (**NOTE**: Do NOT use environment variables, you must specify the full path to the METdataio source code).

- the following code does not need to be modified, it is used by METdbLoad to determine what types
  of files to look for and process.  Currently, only point_stat is supported, with support for the other
  MET tools to be added in the future.

.. code-block:: ini

	<load_val>
		<field name="met_tool">
			<val>ensemble_stat</val>
			<val>grid_stat</val>
			<val>mode</val>
			<val>point_stat</val>
			<val>stat_analysis</val>
			<val>wavelet_stat</val>
		</field>
	</load_val>

- modify the point_stat.yaml file to specify the output directory, output filename, and location of the
  XML specification file:
.. code-block:: ini

  output_dir: /path/to/output_dir

  output_filename: point_stat_reformatted.txt

  xml_spec_file: /path/to/xml_spec_file/<xml filename>.xml


- For *output_dir*, replace /path/to with the full path to the outputdir

- For *xml_spec_file*, replace /path/to with the full path to the XML spec file you edited in the
step above and replace the <xml filename> with *point_stat*.

- **NOTE**: Do NOT use environment variables for /path/to, specify the actual path.

- set the PYTHONPATH:

  bash

.. code-block:: ini

export PYTHONPATH=$BASE_DIR:/$BASE_DIR/METdbLoad:$BASE_DIR/METdbLoad/ush:$BASE_DIR/METreformat

   csh

.. code-block:: ini

setenv PYTHONPATH $BASE_DIR:/$BASE_DIR/METdbLoad:$BASE_DIR/METdbLoad/ush:$BASE_DIR/METreformat

- Generate the reformatted file:

   - from the command line (from any directory):

.. code-block:: ini

   python $BASE_DIR/METreformat/write_stat_ascii.py $WORKING_DIR/point_stat.yaml

- A text file will be created in the output directory with the file name as specified in the yaml file.


