.. _installation:

*************************************
Software Installation/Getting Started
*************************************

Introduction
============

This chapter describes how to install and get started using METdataio.
METdataio has been developed and tested on Mac and Linux operating
systems.  Support for additional platforms may be added in future releases.

METdbload is a Python3 program that requires some extra packages to be
available on the user's computer prior to installation.  METdbload is part
of the METdataio database package. Additional METdataio programs that work
with the data are planned.

Requirements
============

METdataio relies on the following tools. These must be installed and tested
prior to installing METdataio:

**MySQL or MariaDB** - download and install the latest version. This can be
on a separate computer. Use "SET GLOBAL max_allowed_packet=110000000;" by
typing the command in the CLI and/or make the corresponding edit to
/etc/my.cnf, so that the change persists after the next reboot.

**AuroraDB** could alternately be used as a database in the cloud.

Python Requirements
-------------------

**Python 3.10.4+** - Python 3.10.4 or higher must be installed. 

The requirements below come directly from the **requirements.txt** file 
at the top level of the repository.

.. literalinclude:: ../../requirements.txt

Installation
============

Download
--------

Download the latest release:

.. code-block:: ini

  git clone https://github.com/dtcenter/METdataio

Instructions for installing the METdataio package locally via conda
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * Activate the conda environment, changing <your-conda-env-name> to the correct location.

    .. code-block:: ini

      conda activate your-conda-env-name

 * From within the active conda environment, cd to the *METdataio/* directory,
   where the file **pyproject.toml** is located.

 * From this directory, run the following on the command line:
   
   .. code-block:: ini

      pip install -e .

 * The -e option stands for editable, which is useful to
   update the user's METdataio source without reinstalling it.

 * The . indicates that the user should search the current directory for
   the **pyproject.toml** file.

 * Use metdataio package via import statement:

   .. code-block:: ini
      from metdataio import metdbload.ush

        * To use the modules in the *METdataio/METdbLoad/ush* directory.


Instructions for setting the PYTHONPATH
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If pip install is not permitted for the host machine, setting the **PYTHONPATH** is an alternative.
Add the following to the existing **PYTHONPATH**:

.. code-block:: bash

  bash:

  export PYTHONPATH=${PYTHONPATH}:$SRC/METdataio:$SRC/METdataio/METdbLoad:$SRC/METdataio/METdbLoad/ush:$SRC/METdataio/METreformat:$SRC/METdataio/METreadnc

Or

.. code-block:: bash

  cshrc:

  setenv PYTHONPATH ${PYTHONPATH}:$SRC/METdataio:

This will ensure that all available METdataio code will be loaded.

**$SRC** - Represents the path to the METdataio code.

**${PYTHONPATH}** - Corresponds to existing paths that are already defined.


.. note::

  Programs can be run from the METdbLoad/ush subdirectory.

