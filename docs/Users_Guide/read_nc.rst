******************
Read netCDF Input
******************


Description
===========


The *METreadnc* module is used to read netCDF input files and create a list of
Xarray Datasets or a pandas dataframe.

Requirements
============

METreadnc requires the following Python packages that must be installed
prior to us:

- xarray
- netcdf4
- pandas
- pyyaml


Install the same versions of Python packages as those specified in the nco_requirements.txt
or requirements.txt in the METcalcpy repository (https://github.com/dtcenter/METcalcpy).


Setting up
==========

- set up a base directory, where the METdataio source code reside

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


Invoking METreadnc from within your code
=========================================

- **Import the appropriate module**


.. code-block:: ini

    import METreadnc.util.read_netcdf as read_netcdf


- **Create a file reader object**:

.. code-block:: ini

   file_reader = read_netcdf.ReadNetCDF()


- **To read the netCDF file(s) into a list of Xarray Datasets:**

  Invoke the *read_into_xarray* method with the name of a single file or a list
  of files

.. code-block:: ini

  ds = file_reader.read_into_xarray(infile)


The variable *infile* represents a single file (string) or a list of file names. Specify the full path to the file(s).

- **To read the netCDF file(s) into a Pandas DataFrame:**

  Invoke the *read_into_pandas* method with the name of a single file or a list
  of files.  Specify the full path to the file(s).

.. code-block:: ini

  df = file_reader_read_into_pandas(infile)

The variable *infile* represents a single file (string) or a list of file names.  Specify the full path to the file(s).



