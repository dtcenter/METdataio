****************************
Reformat MET Point Stat Data
****************************


Description
===========


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

Some linetypes (such as PCT) with a variable number of columns/headers, will create additional
labelled columns.  For example, in the PCT linetype, there are THRESH_i, THRESH_n, OY_i, and
ON_i columns which need to be explicitly labelled (i.e. OY_1, OY_2, ,,,. OY_m, where m is the THRESH_ith
value).

These columns are needed for the *METplotpy* plots that are
used by METviewer and for generating METplotpy plots from the command line.

The following MET line types* can be reformatted:

- FHO

- CNT

- CTC

- CTS

- SL1L2

- ECNT

- MCTS


These linetypes are produced by the MET point-stat, grid-stat, and ensemble_stat tools.
*Other line types will be supported in the future.*.

Required Components
===================

Some METdbLoad modules are used to find and collect data from the individual .stat files into
one data structure.

A YAML configuration file is required:

The YAML configuration file is used to indicate the name and
location of the output file, the location of the input data, and the MET tool used to generate the data.

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

   python $BASE_DIR/METreformat/write_stat_ascii.py $WORKING_DIR/point_stat.yaml

- A text file will be created in the output directory with the file name as specified in the yaml file.
