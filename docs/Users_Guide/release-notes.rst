*****************************
METdataio Release Information
*****************************

When applicable, release notes are followed by the GitHub issue number which
describes the bugfix, enhancement, or new feature:
`METdataio GitHub issues. <https://github.com/dtcenter/METdataio/issues>`_

METdataio Release Notes
=======================

METdataio Version 2.0.2 release notes (20230223)
------------------------------------------------------
* Bugfixes:
    * **New way of reading files did not handle files with headers but no data**
      (`#181 <https://github.com/dtcenter/METdataio/issues/181>`_)



METdataio Version 2.0.1 release notes (20230125)
------------------------------------------------------
* Bugfixes:

   * **METdbLoad INFO message changed to show correct version**
     (`#154 <https://github.com/dtcenter/METdataio/issues/154>`_)

   * **Getting KeyError multiple times when loading test data**
     (`#157 <https://github.com/dtcenter/METdataio/issues/157>`_)

   * **Upgrade pandas version from 1.2.3**
     (`#161 <https://github.com/dtcenter/METdataio/issues/161>`_)

   * **Add nco_requirements.txt file**
     (`#167 <https://github.com/dtcenter/METdataio/issues/167>`_)

   * **Add modulefiles to the repository**
     (`#170 <https://github.com/dtcenter/METdataio/issues/170>`_)

METdataio Version 2.0.0 release notes (20221207)
------------------------------------------------------

* New Functionality:

    * **Create a MET data reformatter**
      (`#121 <https://github.com/dtcenter/METdataio/issues/121>`_)

    * **Update database schema to handle new columns and line types added to MET Beta 4**
      (`#131 <https://github.com/dtcenter/METdataio/issues/131>`_)

    * **Update database schema to handle new columns and line types added to MET Beta 5**
      (`#141 <https://github.com/dtcenter/METdataio/issues/141>`_)

    * **Update database schema to handle new columns and line types added to MET**
      (`#119 <https://github.com/dtcenter/METdataio/issues/119>`_)

    * **Add processing to set value of field ec_value for CTC,
      CTS, MCTC, and MCTS records**
      (`#105 <https://github.com/dtcenter/METdataio/issues/105>`_)






* Enhancements:

    * **Add 8 new columns to ecnt line data**
      (`#136 <https://github.com/dtcenter/METdataio/issues/136>`_)

    * **Update the database schema to handle new columns added to the
      MET-11.0-beta output**
      (`#92 <https://github.com/dtcenter/METdataio/issues/92>`_)

    * **Set up SonarQube to run nightly**
      (`#39 <https://github.com/dtcenter/METplus-Internal/issues/39>`_)


* Internal:

    * **Update ubuntu version in github action workflow**
      (`#114 <https://github.com/dtcenter/METdataio/issues/114>`_)

    * **Deprecate old ubuntu version in github actions**
      (`#128 <https://github.com/dtcenter/METdataio/issues/128>`_)

    * **Move release notes into is own chapter**
      (`#123 <https://github.com/dtcenter/METdataio/issues/123>`_)

    * **Fix warnings in github Actions**
      (`#122 <https://github.com/dtcenter/METdataio/issues/122>`_)

    * Create a checksum for released code
      (`#112 <https://github.com/dtcenter/METdataio/issues/112>`_)

    * Replace the METdatadb logo with one for METdataio
      (`#112 <https://github.com/dtcenter/METdataio/issues/112>`_)

    * Add modulefiles used for installations on various machines
      (`#108 <https://github.com/dtcenter/METdataio/issues/108>`_)

    * Update the documentation as a result of repository name
      from METdatadb to METdataio
      (`#95 <https://github.com/dtcenter/METdataio/issues/95>`_)


* Bugfixes:

    * **Fix MTD loader to include the last fcst_lead column into the series data**
      (`#120 <https://github.com/dtcenter/METdataio/issues/120>`_)

    * **Fix reading of data files for Python 3.8 with Pandas 1.4**
      (`#85 <https://github.com/dtcenter/METdataio/issues/85>`_)

    * **User has a database without a password**
      (`#90 <https://github.com/dtcenter/METdataio/issues/90>`_)









METdataio Upgrade Instructions
==============================

Upgrade instructions will be listed here if they are
applicable for this release.
