*****************************
METdataio Release Information
*****************************

When applicable, release notes are followed by the GitHub issue number which
describes the bugfix, enhancement, or new feature:
`METdataio GitHub issues. <https://github.com/dtcenter/METdataio/issues>`_

METdataio Release Notes
=======================

METdataio Version 3.1.0 release notes (20250630)
------------------------------------------------------

.. dropdown:: Bugfixes

  * Include support for CTS and CTC linetype in the METdbLoad module for TC data (`#376 <https://github.com/dtcenter/METdataio/pull/376>`_)
  * Modify load_specification schema (`#374 <https://github.com/dtcenter/METdataio/issues/374>`_)
  * TC and MODE data not loading correctly with latest METdataio  (`#389 <https://github.com/dtcenter/METdataio/issues/389>`_)

.. dropdown:: Documentation

   * Enhance the installation instructions (`#361 <https://github.com/dtcenter/METdataio/issues/361>`_)
   * Enhance the Table of Contents to include all METplus components (`#369 <https://github.com/dtcenter/METdataio/pull/369>`_)


.. dropdown:: Enhancements

   * Provide reformatting for the DMAP line type and any associated plots (`#348 <https://github.com/dtcenter/METdataio/issues/348>`_)
   * Update installation modulefiles for Python 3.12 (`#373 <https://github.com/dtcenter/METdataio/issues/373>`_)

.. dropdown::	Repository, build, and test

   * Update infrastructure to reflect move to developing with Python 3.12 (`#368 <https://github.com/dtcenter/METdataio/pull/368>`_)
   * Update modulefiles used on various machines (`#365 <https://github.com/dtcenter/METdataio/issues/365>`_)



METdataio Upgrade Instructions
==============================

.. note::

   In the METdataio-3.1.0-beta2 release, METdataio switched from development
   with Python 3.10.4 to development with Python 3.12. View the
   requirements.txt/nco_requirements.txt file at the top level of the
   repository for version numbers for the corresponding third-party packages.

