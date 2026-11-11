*****************************
METdataio Release Information
*****************************

When applicable, release notes are followed by the GitHub issue number which
describes the bugfix, enhancement, or new feature:
`METdataio GitHub issues. <https://github.com/dtcenter/METdataio/issues>`_

METdataio Release Notes
=======================
METdataio Version 13.0.0-rc1 Release Notes (20261110)
-----------------------------------------------------

.. dropdown:: Bugfixes

   * **Bugfix: METdbLoad loads PSTD thresholds shifted by 5, fails on old RHIST and RPS lines with pandas 3, and other loading bugs** (`#441 <https://github.com/dtcenter/METdataio/issues/441>`_)
   * Bugfix for loading variable-length line types into the database (`#425 <https://github.com/dtcenter/METdataio/issues/425>`_)

.. dropdown:: Documentation

   * Enhance the documentation by correcting broken links and typos (`#428 <https://github.com/dtcenter/METdataio/issues/428>`_)

.. dropdown:: Enhancements

   * **Support reformatting for the SAL1L2 linetype** (`#413 <https://github.com/dtcenter/METdataio/issues/413>`_)
   * **Add VL1L2 to METreformat part of issue #434** (`#437 <https://github.com/dtcenter/METdataio/pull/437>`_)
   * **Add VAL1L2 to METreformat** (`#454 <https://github.com/dtcenter/METdataio/issues/454>`_)
   * **METreformat: implement process_ctc_for_agg to complete CTC linetype support** (`#458 <https://github.com/dtcenter/METdataio/issues/458>`_)
   * **METreformat: implement process_cts_for_agg to complete CTS linetype support in METplotpy scorecard_plot.py** (`#461 <https://github.com/dtcenter/METdataio/issues/461>`_)

.. dropdown:: Repository, build, and test

   * Generate tests to exercise error handling added for METplus-Internal #50 and to increase code coverage (`#339 <https://github.com/dtcenter/METdataio/issues/339>`_)
   * Remove test data from repository, use non-root MySQL user, fix pandas warnings, increase total code coverage from 87% to 94% related to issue #434  (`#442 <https://github.com/dtcenter/METdataio/pull/442>`_)
   * Improve METdataio testing infrastructure (`#455 <https://github.com/dtcenter/METdataio/issues/455>`_)
   * Documentation: Automate link checking with Sphinx (`METplus#3247 <https://github.com/dtcenter/METplus/issues/3247>`_)
   * Update the modulefiles (`#420 <https://github.com/dtcenter/METdataio/issues/420>`_)
   * Store the version number in a single location (`#459 <https://github.com/dtcenter/METdataio/issues/459>`_)
METdataio Version 13.0.0-rc1 Release Notes (20261110)
-------------------------------------------------------

.. dropdown:: Bugfixes

   * **Use case fix when using pandas 3** (`#415 <https://github.com/dtcenter/METdataio/pull/415>`_)

.. dropdown:: Documentation

   * None

.. dropdown:: Enhancements

   * None

.. dropdown:: Repository, build, and test

METdataio Version 13.0.0-beta2 Release Notes (20260507)
-------------------------------------------------------

.. dropdown:: Bugfixes

   * **Use case fix when using pandas 3** (`#415 <https://github.com/dtcenter/METdataio/pull/415>`_)

.. dropdown:: Documentation

   * None

.. dropdown:: Enhancements

   * None
     
.. dropdown:: Repository, build, and test

   * **Add testing for Python 3.14** (`#410 <https://github.com/dtcenter/METdataio/pull/410>`_)
   * Remove python-dateutil from nco_requirements.txt and requirements.txt (`#409 <https://github.com/dtcenter/METdataio/issues/409>`_)


METdataio Version 13.0.0-beta1 Release Notes (20260204)
-------------------------------------------------------

.. dropdown:: Bugfixes

   * **Bugfix to error out when loading MODE object data without CTS data** (`#401 <https://github.com/dtcenter/METdataio/issues/401>`_)

.. dropdown:: Documentation

   * None

.. dropdown:: Enhancements

   * None
     
.. dropdown:: Repository, build, and test

   * None



METdataio Upgrade Instructions
==============================

This section summarizes and highlights important changes to METdataio since version 3.1.0, including:

.. note::


   The version numbering for METdataio has been updated to 13.0.0 to provide consistency and clarity with all METplus components. 

   In the METdataio-13.0.0-beta2 release, METdataio switched from development
   with Python 3.12 to development with Python 3.14. View the
   requirements.txt/nco_requirements.txt file at the top level of the
   repository for version numbers for the corresponding third-party packages.

