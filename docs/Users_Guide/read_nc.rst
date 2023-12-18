******************
Read netCDF Input
******************


Description
===========


The *METreadnc* module is used to read netCDF input files and create a list of
Xarray Datasets or a pandas dataframe.


There are two ways to read netCDF files and save as pandas dataframe
or a list of Xarray Datasets:
 - command line (requires a YAML configuration file)
 - invoking METreadnc methods within your code


Requirements
============

METreadnc requires the following Python packages that must be installed
prior to us:

- xarray
- netcdf4
- pandas
- pyyaml
- METcalcpy (from the same release version number as METdataio)

Install the same versions specified in the nco_requirements.txt
and requirements.txt in the METcalcpy repository (https://github.com/dtcenter/METcalcpy).




Setting up
==========

- set up a base directory, where the METdataio and METcalcpy source code reside


.. code-block:: ini
   bash:
   export BASE_DIR=/path/to/METdataio

   csh:
   setenv BASE_DIR /path/to/METdataio

- replace /path/to with an actual path

- set up a working directory, where the YAML config file will be located (if
  using from the command line)

.. code-block:: ini
   bash:
   export WORKING_DIR=/path/to/working_dir

   csh:
   setenv WORKING_DIR /path/to/working_dir


- set the PYTHONPATH:
.. code-block:: ini
  bash
  export PYTHONPATH=$BASE_DIR:/$BASE_DIR/METdataio:$BASE_DIR/METdataio/METreadnc:$BASE_DIR/METcalcpy:$BASE_DIR/METcalcpy/metcalcpy

  csh
  setenv PYTHONPATH $BASE_DIR:/$BASE_DIR/METdataio:$BASE_DIR/METdataio/METreadnc:$BASE_DIR/METcalcpy:$BASE_DIR/METcalcpy/metcalcpy

Using the command line:
=======================

.. code-block:: ini
 python read_netcdf.py read_netcdf.yaml

The read_netcdf.yaml file consists of a list of one or more files to be read either
by explicitly specifying the path to each file, or defining an environment
variable and using that to specify the path to each file:

.. code-block:: ini
  # Using environment variable CALCPY_DATA
  - !ENV '${CALCPY_DATA}/sa_test.nc'
  - !ENV '${CALCPY_DATA}/sa_test_20230101.nc'

**Make sure to include a whitespace between the '-' and the !ENV, and enclose the
environment variable name with curly braces ({}).**


.. code-block:: ini
  # Or explicitly indicating the full path to the file
  - /Users/someuser/data/sa_test2.nc'
  - /Users/someotheruser/project/data/sa_test_20230131.nc'

**Make sure to include a whitespace between the '-' and the file name.**


Invoking METreadnc from within code:
===================================

     - import the appropriate module

.. code-block:: ini

    import METreadnc.util.read_netcdf as read_netcdf

    - create a file reader object:

.. code-block:: ini
 file_reader = read_netcdf.ReadNetCDF()

- generate a list of Xarray Datasets
.. code-block:: ini
  ds = file_reader.read_into_xarray(infile)

The infile can be a single file (string) or a list of file names (with
full path specified).

- create a pandas dataframe

.. code-block:: ini
  df = file_reader_read_into_pandas(infile)

The infile can be a single file (string) or a list of file names (with
full path specified).



