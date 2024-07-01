*****************************
METdataio Release Information
*****************************

When applicable, release notes are followed by the GitHub issue number which
describes the bugfix, enhancement, or new feature:
`METdataio GitHub issues. <https://github.com/dtcenter/METdataio/issues>`_

METdataio Release Notes
=======================

METdataio Version 3.0.0 Beta 5 Release Notes (20240630)
-------------------------------------------------------


  .. dropdown:: New Functionality



  .. dropdown:: Enhancements

     * **Enhance METdataio schema to load the TOTAL_DIR column added to the VL1L2, VAL1L2, and VCNT line types during the MET 12.0.0 beta5 develpment cycle** (`#307 <https://github.com/dtcenter/METdataio/issues/307>`_)

     * **Add support for reformatting the MPR linetype** (`#255 <https://github.com/dtcenter/METdataio/issues/255>`_)

  .. dropdown:: Internal

     * Update GitHub issue and pull request templates to reflect the current development workflow details (`#231 <https://github.com/dtcenter/METdataio/issues/231>`_)

     * Consider using only .yml or only .yaml extensions (`#272 <https://github.com/dtcenter/METdataio/issues/272>`_)

     * METdataio: Code coverage statistics  (`#53 <https://github.com/dtcenter/METplus-Interna/issues/53>`_)


  .. dropdown:: Bugfixes




METdataio Version 3.0.0 Beta 4 Release Notes (20240417)
-------------------------------------------------------



  .. dropdown:: New Functionality



  .. dropdown:: Enhancements

     * **Support reformatting the TCDIAG line type written by the TC-Pairs tool** (`#240 <https://github.com/dtcenter/METdataio/issues/240>`_)
     * **Modify Requirements section of the User's Guide** (`#273 <https://github.com/dtcenter/METdataio/issues/273>`_)
     * **Enhance METdataio schema to handle changes to the ECNT, VL1L2, VAL1L2, and VCNT line types during the MET 12.0.0 beta4 development cycles** (`#282 <https://github.com/dtcenter/METdataio/issues/282>`_)


  .. dropdown:: Internal

     * **Add GitHub action to run SonarQube for METdataio pull requests and feature branches** (`#289 <https://github.com/dtcenter/METdataio/issues/289>`_)


  .. dropdown:: Bugfixes






METdataio Version 3.0.0 Beta 3 Release Notes (20240207)
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



  .. dropdown:: Internal




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

Database Schema upgrade instructions
------------------------------------

Changes were made to MET version 12.0.0 for following linetypes (addition of the TOTAL_DIR column):
  * VL1L2
  * VAL1L2
  * VCNT

The database schema requires updating, please follow these instructions in the Contributor's Guide:

`Update the Database Schema <https://metdataio.readthedocs.io/en/develop/Contributors_Guide/update_database_schema.html>`_
