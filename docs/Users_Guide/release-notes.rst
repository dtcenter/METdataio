METdatadb release notes
_______________________

When applicable, release notes are followed by the GitHub issue number which
describes the bugfix, enhancement, or new feature:
`METdatadb GitHub issues. <https://github.com/dtcenter/METdatadb/issues>`_

Version |version| release notes (|release_date|)
------------------------------------------------

Version 1.1.0 release notes (20220311)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* New Functionality:

* Enhancements:

   * Handle variable lenght lines with more than 125 columns (`#125 <https://github.com/dtcenter/METdatadb/issues/125>`_)

   * Setup GitHub actions to implement regression testing of loading METdatadb (`#76 <https://github.com/dtcenter/METdatadb/issues/76>`_)

   * Read in stratospheric data for means calculation (`#66 <https://github.com/dtcenter/METdatadb/issues/66>`_)

   * Load new SSIDX STAT line type from Stat-Analysis (`#63 <https://github.com/dtcenter/METdatadb/issues/63>`_)

   * Load new DMAP columns for G and GBETA (`#62 <https://github.com/dtcenter/METdatadb/issues/62>`_)

   * Update database loader to handle new CNT columns (`#61 <https://github.com/dtcenter/METdatadb/issues/61>`_)

   * Create a class to read netcdf files (`#56 <https://github.com/dtcenter/METdatadb/issues/56>`_)

   * Add loading of updated MCTC and MCTS line types (`#54 <https://github.com/dtcenter/METdatadb/issues/54>`_)

* Bugfixes:

   * **Change field name "rank" in table line_data_orank to something else (`#70 <https://github.com/dtcenter/METdatadb/issues/70>`_)**

   * Distinguish Mode files  (`#68 <https://github.com/dtcenter/METdatadb/issues/68>`_)**

* Internal:

   * Setup GitHub actions to implement regression testing (`#67 <https://github.com/dtcenter/METdatadb/issues/67>`_)**

   * Update documentation to reference GitHub Discussions (`#55 <https://github.com/dtcenter/METdatadb/issues/55>`_)**
