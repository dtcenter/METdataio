METdatadb release notes
_______________________

When applicable, release notes are followed by the GitHub issue number which
describes the bugfix, enhancement, or new feature:
`METdatadb GitHub issues. <https://github.com/dtcenter/METdatadb/issues>`_

Version |version| release notes (|release_date|)
------------------------------------------------

Version 1.0.0 release notes (20210512)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

New Functionality:

  * Read MET (.stat) files (`#1 <https://github.com/dtcenter/METdatadb/issues/1>`_)

  * Transform MET data for any database (`#3 <https://github.com/dtcenter/METdatadb/issues/3>`_)
  
  * Write MET (.stat) files to a SQL database (`#6 <https://github.com/dtcenter/METdatadb/issues/6>`_)

  * Read VSDB (.vsdb) files (`#2 <https://github.com/dtcenter/METdatadb/issues/2>`_)

  * Transform VSDB data for any database (`#4 <https://github.com/dtcenter/METdatadb/issues/4>`_)

  * Apply and Drop SQL Indexes (`#8 <https://github.com/dtcenter/METdatadb/issues/8>`_)

  * Implement date_list tag (`#15 <https://github.com/dtcenter/METdatadb/issues/15>`_)

  * Load MODE files (`#22 <https://github.com/dtcenter/METdatadb/issues/22>`_)

  * Load MTD files (`#23 <https://github.com/dtcenter/METdatadb/issues/23>`_)

  * Enhance METdatadb to load the TCST output line types from TC-Pairs. (`#28 <https://github.com/dtcenter/METdatadb/issues/28>`_)

  * Add new optional command line argument to change default location of temporary files (`#30 <https://github.com/dtcenter/METdatadb/issues/30>`_)

Enhancements:

  * Load new DMAP line type created by Grid-Stat in MET version 9.0. (`#19 <https://github.com/dtcenter/METdatadb/issues/19>`_)
  
  * Load new MTD columns for MET version 9.0. (`#20 <https://github.com/dtcenter/METdatadb/issues/20>`_)

  * Update METdb to handle changes in met-9.1. (`#25 <https://github.com/dtcenter/METdatadb/issues/25>`_)

  * Enhance METdbLoad to read new RPS line type from Ensemble-Stat and Point-Stat. (`#26 <https://github.com/dtcenter/METdatadb/issues/26>`_)

  * Add support for ANOM_CORR_RAW which is new in the CNT line type for met-9.1. (`#29 <https://github.com/dtcenter/METdatadb/issues/29>`_)

  * Add version number to usage (`#41 <https://github.com/dtcenter/METdatadb/issues/41>`_)

  * Create initial METdbload documentation (`#46 <https://github.com/dtcenter/METdatadb/issues/46>`_)

  * Enhance METdatadb to load the additional CRPS columns in the ECNT line type updated for met-10.0.0. (`#48 <https://github.com/dtcenter/METdatadb/issues/48>`_)

  * Enhance METdatadb to load the additional climatology column in the ORANK line type updated for met-10.0.0. (`#49 <https://github.com/dtcenter/METdatadb/issues/49>`_)

Bugfixes:

  * Fix bug - handle non-existent directory with templates (`#12 <https://github.com/dtcenter/METdatadb/issues/12>`_)

  * Error loading ECLV(ECON) line type from VSDB files (`#32 <https://github.com/dtcenter/METdatadb/issues/32>`_)

Internal:

  * Put flags from XML load_spec into a dictionary (`#5 <https://github.com/dtcenter/METdatadb/issues/5>`_)
  
  * Handle transformations related to PSTD, RHIST, ECNT, and ORANK (`#7 <https://github.com/dtcenter/METdatadb/issues/7>`_)

  * Handle variable length line types in stat records (`#9 <https://github.com/dtcenter/METdatadb/issues/9>`_)

  * Speed up reading files (`#13 <https://github.com/dtcenter/METdatadb/issues/13>`_)

  * Check to make sure XML file exists (`#18 <https://github.com/dtcenter/METdatadb/issues/18>`_)

  

