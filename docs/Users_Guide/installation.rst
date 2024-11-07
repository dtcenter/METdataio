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

.. note::

  Programs can be run from the METdbLoad/ush subdirectory.

