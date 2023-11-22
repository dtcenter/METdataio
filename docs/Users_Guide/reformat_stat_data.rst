****************************
Reformat MET Point Stat Data
****************************


Description
===========


The *METreformat* module is used to reformat MET .stat files (from the point_stat, grid_stat, or ensemble_stat tools)
into a format that can be read by METplotpy.  The reformatted data contains additional columns, based on the type of
plot.  For line, bar, box, contour, performance diagram, revision box, revision series, and taylor diagram plots,
the .stat data contains additional columns:

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

Some linetypes (such as PCT) with a variable number of columns/headers, will create additional
labelled columns.  For example, in the PCT linetype, there are THRESH_i, THRESH_n, OY_i, and
ON_i columns which need to be explicitly labelled (i.e. OY_1, OY_2, ,,,. OY_m, where m is the THRESH_ith
value).

Other plot types require other special columns.

Currenty, following MET line types* can be reformatted:

- FHO

- CNT

- CTC

- CTS

- SL1L2

- ECNT

- MCTS

- PCT (for ROC)


These linetypes are produced by the MET point-stat, grid-stat, and ensemble_stat tools.
*Additional line types will be supported in the future.*.

Required Components
===================

Some METdbLoad modules are used to find and collect data from the individual .stat files into
one data structure.  The input .stat files must all reside under one directory, the path to the
data is specified in a YAML configuration file.


The YAML configuration file is used to indicate the name and
location of the output file, the location of the input data, logging information (filename, log level),
and the linetype to read in and reformat.

Example
=======

- set up a base directory, where the METdataio source code is located
  NOTE: This may require the reorganization of data that is distributed over numerous directories into
  a single directory.


.. code-block:: ini

   bash:
   export BASE_DIR=/path/to/METdataio

   csh:
   setenv BASE_DIR /path/to/METdataio

- replace /path/to with an actual path

- set up a working directory, where the YAML config file will be located

.. code-block:: ini

   bash:
   export WORKING_DIR=/path/to/working_dir

   csh:
   setenv WORKING_DIR /path/to/working_dir



- **NOTE**: Do NOT use environment variables for /path/to, specify the actual path.

- set the PYTHONPATH:


.. code-block:: ini

  bash
  export PYTHONPATH=$BASE_DIR:/$BASE_DIR/METdbLoad:$BASE_DIR/METdbLoad/ush:$BASE_DIR/METreformat

  csh
  setenv PYTHONPATH $BASE_DIR:/$BASE_DIR/METdbLoad:$BASE_DIR/METdbLoad/ush:$BASE_DIR/METreformat

- Generate the reformatted file:

   - from the command line (from any directory):

.. code-block:: ini

   python $BASE_DIR/METreformat/write_stat_ascii.py $WORKING_DIR/*line_type*_stat.yaml

   replace *line_type* with the line type to reformat.

- A text file will be created in the output directory with the file name as specified in the yaml file.
