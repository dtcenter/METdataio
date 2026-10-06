
*************************************
Running Unit Tests Locally
*************************************

Background
===========

Unit tests are included in METdataio and all of its sub-modules. The tests are found under the `test/` directory in each module, e.g., `METdbLoad/test/`.
These tests are run automatically when a pull request is raised on GitHub and must pass before any merge will be considered. 
When developing new features it is advisable to ensure the tests pass by running them locally against your changes. To do this you must first have either
a `mysql` or `mariadb` service running and set up with an appropriate user. Although either database can be used, this guide will focus on `mariadb`.

Database Setup
==============

The setup of `mariadb` will be different depending on the operating system you are running, and the available privileges. If you encounter issues it is recommended you consult your system administrator
for support appropriate to your system and environment. For up to date instructions on installing mariadb consult the `MariaDB docs <mariadb.org>`_ or the `MariaDB GitHub page <https://github.com/MariaDB/>`_.

Below is an example setup on CentOS.

1. Install mariadb-server: 

.. code-block:: console

    $ sudo yum install mariadb-server

2. Start mariadb and change the root password. Note that by default there is no password for the root user:

.. code-block:: console

    $ sudo mariadb -u root -p
    [MariaDB]> ALTER USER 'root'@'localhost' IDENTIFIED BY 'root_password';
    [MariaDB]> exit;

3. Start the mariadb service:

.. code-block:: console

    $ sudo systemctl start mariadb

Running Tests
=============

Tests can be run using `pytest`. If required, you can install using either `conda install pytest` or `pip install pytest`.

.. code-block:: console

    $ pytest METdbLoad/test/

To check test coverage `conda install pytest-cov`, and use the `--cov` commands. For example:

.. code-block:: console

    $ pytest METdbLoad/test/ --cov METdbLoad/ush/ --cov-report term-missing

Writing Tests
=============

All Pull Requests that change source code in `METdataio` should include appropriate unit tests. These tests should 
demonstrate that when the new source code is invoked it produces the desired results. For help with writing tests, and for examples 
of how to use pytest, refer to the `pytest documentation <https://docs.pytest.org/>`_.

When writing new tests for `METdataio` you should be familiar with the content of `conftest.py`, noting that each sub module may have its own `conftest.py`.
For example, when writing a test for `METdbLoad` you may want to instantiate an empty test database. To do this use the `emptyDB` test fixture, found in `METdbLoad/conftest.py`.

For further examples, refer to the existing tests for each module.
