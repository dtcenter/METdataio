*****************************
METdataio Release Information
*****************************

When applicable, release notes are followed by the GitHub issue number which
describes the bugfix, enhancement, or new feature:
`METdataio GitHub issues. <https://github.com/dtcenter/METdataio/issues>`_

METdataio Release Notes
=======================

METdataio Version 3.0.0 Beta 6 Release Notes (20241017)
-------------------------------------------------------

  .. dropdown:: New Functionality

     None

  .. dropdown:: Enhancements

     * Improve error messages for when multiple files are not loaded (`#35 <https://github.com/dtcenter/METdataio/issues/35>`_)
     * Improve logging for 5 STIGS (`METplus-Internal#45 <https://github.com/dtcenter/METplus-Internal/issues/45>`_))
     * Improve error handling and testing (`METplus-Internal#50 <https://github.com/dtcenter/METplus-Internal/issues/50>`_)
     * Validation against recursive payloads, oversized payloads and overlong element names (`METplus-Internal#56 <https://github.com/dtcenter/METplus-Internal/issues/56>`_)
     * Add the user ID to the log output at beginning and end of each logging session (`#202 <https://github.com/dtcenter/METdataio/issues/202>`_)
     * **Update database to load 7 new MODE CTS statistics** (`#226 <https://github.com/dtcenter/METdataio/issues/226>`_)
     * **Enhance METdataio schema to handle new/modified MPR and ORANK columns introduced for MET-12.0.0-beta6** (`#320 <https://github.com/dtcenter/METdataio/issues/320>`_)
     * **Update the MPR reformatting in METreformat due to added columns in MET-12.0.0-beta6** (`#321 <https://github.com/dtcenter/METdataio/issues/321>`_)
     * **Modify the column names for the existing SEEPS line type in the database schema** (`#335 <https://github.com/dtcenter/METdataio/issues/335>`_)
		
  .. dropdown:: Internal

     * Consider using only .yml or only .yaml extensions (`#272 <https://github.com/dtcenter/METdataio/issues/272>`_)
     * Create additional tests to METdataio to increase code coverage (`#318 <https://github.com/dtcenter/METdataio/issues/318>`_)
     * Skip SonarQube scan for PR from fork and allow compare_db workflow from fork (`PR #327 <https://github.com/dtcenter/METdataio/pull/327>`_)

  .. dropdown:: Bugfixes

     * Bugfix: Schema file missing on install (`PR #341 <https://github.com/dtcenter/METdataio/pull/341>`_)
		
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

beta6
-----

The following changes were made in MET version 12.0.0 for the following linetypes:

  * for the MPR linetype:
  
    * the renaming of the climo_mean column to obs_climo_mean
    * the renaming of the climo_stdev column to obs_climo_stdev
    * the renaming of the climo_cdf column to obs_climo_cdf 
    * the addition of the fcst_climo_mean column
    * the addition of the fcst_climo_stdev column

  * for the ORANK linetype:
  
    * the renaming of the climo_mean column to obs_climo_mean
    * the renaming of the climo_stdev column to obs_climo_stdev
    * the addition of the fcst_climo_mean column
    * the addition of the fcst_climo_stdev column

  * for the SEEPS linetype:

    * the renaming of the s12 column fo odfl
    * the renaming of the s13 column to odfh
    * the renaming of the s21 column to olfd
    * the renaming of the s23 column to olfh
    * the renaming of the s31 column to ohfd
    * the renaming of the s32 column to ohfl

The ALTER TABLE mysql/mariadb commands are available in the **METdataio/METdbLoad/sql/updates/update_for_6_0_beta6.sql**
file. Refer to the appropriate documentation to use the proper database syntax to update existing tables. 

      
beta5 - Database Schema
-----------------------

Changes were made to MET version 12.0.0 for following linetypes (addition of the TOTAL_DIR column):

  * VL1L2
  * VAL1L2
  * VCNT

