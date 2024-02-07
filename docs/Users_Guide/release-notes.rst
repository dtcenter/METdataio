*****************************
METdataio Release Information
*****************************

When applicable, release notes are followed by the GitHub issue number which
describes the bugfix, enhancement, or new feature:
`METdataio GitHub issues. <https://github.com/dtcenter/METdataio/issues>`_

METdataio Release Notes
=======================

METdataio Version 3.0.0 Beta 3 Release Notes (20230207)
-------------------------------------------------------


  .. dropdown:: New Functionality



  .. dropdown:: Enhancements

     * **RRFS METreformat: Implement Tier 1 Linetypes for RRFS UFS R2O command line support** (`#234 <https://github.com/dtcenter/METdataio/issues/234>`_)
     * **Provide instructions for updating the database schema corresponding to any MET updates** (`#245 <https://github.com/dtcenter/METdataio/issues/245>`_)


  .. dropdown:: Internal

     * Update GitHub actions workflows to switch from node 16 to node 20 (`#264 <https://github.com/dtcenter/METdataio/issues/264>`_)


  .. dropdown:: Bugfixes

     * Remove existing output and update documentation reflecting change (`#232 <https://github.com/dtcenter/METdataio/issues/232>`_)
     * **METreadnc does not support reading netCDF data that only has level and latitude** (rather than longitude and latitude and level)  (`#247 <https://github.com/dtcenter/METdataio/issues/247>`_)
     * Create an update schema script to add columns to line_data_tcmpr table (`#251 <https://github.com/dtcenter/METdataio/issues/251>`_)
     * **TC Pairs files not loading into database** (`#256 <https://github.com/dtcenter/METdataio/issues/256>`_)
     * **Refactor the ECNT reformatter to accomodate the METcalcpy input data format requirements** (`#267 <https://github.com/dtcenter/METdataio/issues/267>`_)



METdataio Version 3.0.0 Beta 2 Release Notes (20231114)
-------------------------------------------------------


  .. dropdown:: New Functionality


  .. dropdown:: Enhancements

     * Make Headers consistent across all repos (`#238 <https://github.com/dtcenter/METdataio/issues/238>`_)


  .. dropdown:: Internal


  .. dropdown:: Bugfixes


METdataio Version 3.0.0 Beta 1 Release Notes (20230915)
-------------------------------------------------------


  .. dropdown:: New Functionality


  .. dropdown:: Enhancements


  .. dropdown:: Internal


  .. dropdown:: Bugfixes

     * **Password field in loading XML files can be empty** (`#221 <https://github.com/dtcenter/METdataio/issues/221>`_)

     * **METdataio isn't correctly placing the database in the correct METviewer group** (`#228 <https://github.com/dtcenter/METdataio/issues/228>`_)

     * **METreformat address PerformanceWarning** (`#219 <https://github.com/dtcenter/METdataio/issues/219>`_)




METdataio Upgrade Instructions
==============================

Upgrade instructions will be listed here if they are
applicable for this release.
