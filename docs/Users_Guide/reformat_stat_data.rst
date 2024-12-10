*******************************
Reformat MET Stat and TCST Data
*******************************


Description
===========

The METreformat module provides support for reformatting/rearranging MET .stat/.tcst output into METplotpy-readable
input formats.  The formats vary, based on the plot type (e.g. a histogram plot requires ranks and values column data,
whereas a line plot will need statistic name and statistic value columns). The MET .stat/.tcst ASCII output generated
by MET tools such as the Point-Stat, Grid-Stat, Ensemble-Stat, and TC-Pairs tools may contain ASCII columnar data
from multiple line types (determined by settings in the MET configuration file).

.. note ::
  Currently, there are MET linetypes that **do not** have reformatter support.  When an unsupported linetype
  is requested, an appropriate error message will be generated.

MET .stat and .tcst files can be comprised of numerous rows of data with different number of columns. This is a
result of requesting multiple linetypes in a MET configuration file (e.g. requesting .
All MET .stat line types have common columns (refer to table 11.1 in the MET User's Guide) that are labelled
(i.e. the columns have headers/names).  The remaining columns
are unlabelled.  When the stat output from the MET point-stat, grid-stat, ensemble-stat, or tc-pairs tools are
reformatted, these unlabelled columns are rearranged into a format appropriate
for the METplotpy plot of interest (based on the *line_type* setting in the YAML configuration file).

The format of input data used by the METplotpy plots is influenced by METviewer.
When generating plots in METviewer, the MET .stat input data is first loaded into the
a database. A database query is then performed to filter the data (based on criteria specified through
METviewer's graphical user interface).  The query is based on:

- the selected values for the dependent variables (i.e. forecast variable) and the associated statistics of interest
- series variables (i.e. model and specific model values)
- fixed values (corresponding to columns in the MET output such as forecast lead, vx mask, model, etc.)

Any requested aggregation statistics (i.e. mean, median) are calculated by invoking the METcalcpy agg_stat.py code.
The format of the data from the query result may vary, based on the line type and whether aggregation statistics were
calculated.

.. note::

  METviewer invokes the appropriate METplotpy plot script using the database query results as input, and **it
  is this format that is expected by the METplotpy scripts**.

Users have the option to generate plots from the command line, by-passing METviewer and its database. **However,
the MET stat-analysis** tool will need to be used for performing any necessary filtering of data (e.g. by any combination of
times, models, regions, etc.) prior to reformatting. The METcalcpy agg_stat.py module is used after
reformatting to calculate aggregation statistics (i.e. total, mean, median, confidence
intervals for a specific statistic, etc.). Not all data requires the calculation of
aggregation statistics (e.g. histogram plots).

Plots that are generated from the command line require a YAML configuration file. The .stat output from the point-stat,
grid-stat, ensemble-stat, or tc-pairs tool must be reformatted before invoking the METplotpy scripts from the
command line.




..  dropdown:: Linetypes, MET tools and corresponding plot types (click to expand)

     .. dropdown:: FHO

          **Forecast, Hit, Observation Rates**

          From MET tools:
            - point-stat
            - grid-stat

         Used by the following METplotpy plot types:
            - line
            - bar
            - box
            - contour
            - performance diagram
            - revision box
            - revision series
            - Taylor Diagram


     .. dropdown:: CNT

         **Continuous Statistics**

         From MET tools:
            - point-stat
            - grid-stat

        Used by the following METplotpy plot types:
            - line
            - bar
            - box
            - contour
            - performance diagram
            - revision box
            - revision series
            - Taylor Diagram

     .. dropdown:: CTC

          **Contingency Table Counts**

          From MET tools:
            - point-stat
            - grid-stat

         Used by the following METplotpy plot types:
            - line
            - bar
            - box
            - contour
            - performance diagram
            - revision box
            - revision series
            - Taylor Diagram

     .. dropdown:: CTS

         **Contingency Table Statistics**

         From MET tools:
            - point-stat
            - grid-stat

        Used by the following METplotpy plot types:
            - line
            - bar
            - box
            - contour
            - performance diagram
            - revision box
            - revision series
            - Taylor Diagram

     .. dropdown:: SL1L2

         **Scalar L1L2 Partial Sums**

        From MET tools:
            - point-stat
            - grid-stat

        Used by the following METplotpy plot types:
            - line
            - bar
            - box
            - contour
            - performance diagram
            - revision box
            - revision series
            - Taylor Diagram

     .. dropdown:: VL1L2

        **Vector L1L2 Partial Sums**

        From MET tools:
            - point-stat
            - grid-stat

        Used by the following METplotpy plot types:
            - line
            - bar
            - box
            - contour
            - performance diagram
            - revision box
            - revision series
            - Taylor Diagram

     .. dropdown:: ECNT

        **Ensemble Continuous Statistics**

        From MET tools:
            - point-stat
            - ensemble-stat

        Used by the following METplotpy plot types:
            - line (for ensemble spread-skill plots)

     .. dropdown:: MCTS

        **Multi-category Contingency Table Statistics**

        From MET tools:
            - point-stat

        Used by the following METplotpy plot types:
            - line
            - bar
            - box
            - contour
            - performance diagram
            - revision box
            - revision series
            - Taylor Diagram

     .. dropdown:: VCNT

        **Vector Continuous Statistics**

        From MET tools:
            - point-stat
            - grid-stat

        Used by the following METplotpy plot types:
            - line
            - bar
            - box
            - contour
            - performance diagram
            - revision box
            - revision series
            - Taylor Diagram

     .. dropdown:: PCT

        **Contingency Table counts for Probabilistic forecasts**

        From MET tools:
            - point-stat
            - grid-stat
            - ensemble_stat

        Used by the following METplotpy plot types:
            - ROC diagram (Receiver Operator Curve)


     .. dropdown::  RHIST

        **Ranked Histogram Counts**

         From MET tools:
            - ensemble-stat

        Used by the following METplotpy plot types:
            - rank histogram (rhist)

     .. dropdown::  TCDIAG (with TCMPR)

         **Tropcial Cyclone Diagnostics**
         **Tropical Cyclone Matched Pairs**

         From MET tool:
            - tc-pairs

        Used by the following METplotpy plot types:
            - TCMPR (Tropical Cyclone Matched Pair) plots:

            - mean line plot
            - median line plot
            - boxplot
            - relative performance plot
            - rank plot
            - mean skill line plot
            - median skill line plot

     .. dropdown:: MPR

         **Matched Pair data**

         From MET tools:
            - point-stat
            - grid-stat

        Used by the following METplotpy plot types:
            - line
            - scatter

     .. dropdown:: DMAP

         **Distance Map Statistics**

         From MET tools:
            - grid-stat

        Used by the following METplotpy plot types:
            - line
            - contour
            - scatter


Description of Formats:
-----------------------

The reformatted data contains new columns, based on the type of
plot and line type.  Formats fall into the following categories:

    .. dropdown::  by statistic name, statistic value and confidence values

       - Needed by the following plot types in METplotpy:

         - line
         - bar
         - box
         - contour
         - performance diagram
         - revision box
         - revision series
         - Taylor diagram

       - Unlabelled columns in the original .stat file are placed under statistic name, statistic value,
         and confidence level columns (in addition to the common stat columns):

         - stat_name
         - stat_value
         - stat_ncl

            - lower level normal confidence limit

         - stat_ncu

            - upper level normal confidence limit

         - stat_bcl

            - lower level bootstrap confidence limit

         - stat_bcu

            - upper level bootstrap confidence limit

       - The original format is converted from wide format (where each row corresponds to a unique
         set of common stat column values) to longer format (where there are multiple rows
         corresponding to the same common stat column values).

           - The common stat columns are common to all line types for point-stat, grid-stat, and ensemble-stat (model, vx mask, description, fcst lead, etc.)
             as described in the MET User's Guide:

              - table 11.1 (point-stat)
              - table 12.1 (grid-stat)
              - table 13.1 (ensemble-stat)



         .. dropdown:: Unformatted Example:

            .. literalinclude:: ./figure/grid_stat_FV3_core_lsm1_020000L_20190521_020000V.stat

         - In the unformatted example:

           - each row represents unique data (tidy data)
           - the number of columns is different for each row, due to each row representing a different linetype
             (e.g. DMAP, NBRTC)
           - numerous columns are unlabelled


         .. dropdown:: Reformatted Example (truncated):

            ..  literalinclude:: ./figure/reformatted_dmap_for_lineplot_output.txt


         -  In the reformatted example:

            - reformatted data file contains only one linetype (in this example DMAP), as requested in the configuration file

            - multiple rows have duplicate common stat column values, resulting in more rows

              - note that for the rows containing the first **fy** and **oy** stat_names, the common stat columns of
                version,..., linetype, and total are the same.  The unique data has been replicated and separated
                based on stat_name and stat_value.

    .. dropdown::  by all common stat columns AND linetype-specific columns from MET

       - Used by the following plot types and linetypes in METplotpy:

         - **scatter** plot

           - **MPR** linetype
           - **DMAP** linetype


         - unlabelled columns in the original .stat file are placed under the linetype-specific column names

           - refer to the MET User's Guide for point-stat, grid-stat, ensemble-stat, and tc-pairs  for the column names


       - The original format is converted from wide format (where each row corresponds to a unique
         set of common stat column values) to longer format (where there are multiple rows
         corresponding to the same common stat column values).

           - The common stat columns are common to all line types for point-stat, grid-stat, and ensemble-stat (model, vx mask, description, fcst lead, etc.)
             as described in the MET User's Guide:

              - table 11.1 (point-stat)
              - table 12.1 (grid-stat)
              - table 13.1 (ensemble-stat)


         .. dropdown:: Unformatted Examples:

            .. literalinclude:: ./figure/grid_stat_FV3_core_lsm1_020000L_20190521_020000V.stat

         - In the unformatted example:

           - each row represents unique data (tidy data)
           - the number of columns is different for each row, due to each row representing a different linetype
             (e.g. DMAP, NBRTC)
           - numerous columns are unlabelled


         .. dropdown:: Reformatted Example (truncated):

            ..  literalinclude:: ./figure/dmap_for_scatter.data

         -  In the reformatted example:

            - reformatted data file contains only one linetype (in this example DMAP), as requested in the configuration file
            - the common stat columns are present, in addition to all the linetype-specific columns
            - first column is an index value created during the reformatting process
            - each row represents unique data (tidy data)


    .. dropdown::  by threshold values

       - The PCT linetype contains threshold values

       - Needed by the **ROC diagram** plot type in METplotpy

       - The reformatted .stat file contains the following columns (in addition
         to the common stat columns):

         - thresh_i
           - the ith probability threshold value (repeated)

         - oy_i
           - number of observations *yes* when forecast is between the ith and i+1th probability threshold

         - on_i
           - number of observations when *no* forecast is between the ith and i+1th probability threshold

         - i_value
           - indicates the value number, corresponding to the ith value of thresh_i, oy_i, and on_i

       - The PCT line type consists of a **variable** number of unlabelled columns/headers
         corresponding to THRESH_i, OY_i, and ON_i, as described in the MET User's Guide:
         https://met.readthedocs.io/en/latest/Users_Guide/.

      -  The columns corresponding to OY_1, OY_2, ,,,. OY_m (where *m*
         is the THRESH_ith value) are **unlabelled** when generated by the MET tool.

         -  These unlabelled columns are appropriately labelled to OY_1,..., OY_m values, ON_1, .., ON_m,
            and THRESH_1,..., THRESH_m.

         - These labelled columns are then ordered into the thresh_i, oy_i, on_i, and i_value
           columns.

             - The i_value column is derived from the ith value of OY, ON, and THRESH.
             - The thresh_i column consists of the threshold values for the threshold number defined in the i_value column.
             - The oy_i and on_i columns contain the OY_i and ON_i values from the .stat data.

         .. dropdown:: Unreformatted example (truncated):

            .. literalinclude:: ./figure/point_stat_RRFS_GEFS_GF.SPP.SPPT_prob_ADPSFC_NDAS_000000L_20220506_000000V.stat

         - In the unformatted example:

             - each row represents unique data (tidy data)
             - the number of columns is different for each row, due to each row representing a different linetype
               (e.g. PCT, PSTD, PJC, PRC)
             - numerous columns are unlabelled

         .. dropdown:: Reformatted Example (truncated):

                       .. literalinclude:: ./figure/roc_pct.data

         -  In the reformatted example:

                          - all columns are now labelled and only PCT linetype data is present
                            (as requested in the configuration file)
                          - numerous rows were removed from the example for simplicity
                          - numbers on the far left correspond to index values used in reformatting the original data
                          - TMP forecast variable rows used in the example, other forecast variables were manually
                            removed from the output file
                          - numerous rows have the same common stat columns (i.e. version, ..., interp_mthd),
                            the same line_type, and total value

                               - the data is organized by thresh_i, oy_i, on_i, and i_value using the criteria describe
                                 above


    .. dropdown::  by ranked data values

       - Needed by the **rank histogram** plot type in METplotpy

       - The reformatted .stat file contains these columns (in addition to the
         common stat columns and RHIST-specific columns):

         - rank_i

           - count of observations with the ith rank (repeated)

         - i_value

           - the rank number

        .. dropdown:: Unformatted Example (truncated):

            .. literalinclude:: ./figure/ensemble_stat_RRFS_GEFS_GF.SPP.SPPT_ADPSFC_NDAS_20220506_000000V.stat

        - In the unformatted example:

          - each row represents unique data (tidy data)
          - the number of columns is different for each row, due to each row representing a different linetype
            (e.g. ECNT, RHIST, PHIST, RELP, SSVAR)
          - numerous columns are unlabelled


        .. dropdown:: Reformatted Example (truncated):

            .. literalinclude:: ./figure/rhist.data

        - In the reformatted example:

          - each row represents only RHIST linetype data (as requested in the configuration file)
          - all columns are labelled
          - an additional column (first column) corresponds to index values created in the reformatting process
          - there are numerous rows with the same common stat values (model,...,total)
            - with data separated by rank_i and i_values


    .. dropdown:: by specific linetype: TCDIAG

       - **TCDIAG** linetype

             - generated by the MET tc-pairs tool
             - refer to MET User's Guide table 24.1 for the common stat columns for all tc-pairs output
             - refer to MET User's Guide table 24.3 for the TCDIAG columns
             - TCMPR line is always generated by MET tc-pairs
             - TCDIAG line is included when diagnostics are requested in the MET config file


       - Used to generate **TCMPR** plots

           - the available TCMPR plot types in METplotpy:

             - mean line plot
             - median line plot
             - boxplot
             - relative performance plot
             - rank plot
             - mean skill line plot
             - median skill line plot

       - The reformatted data consolidates the TCDIAG line with its corresponding TCMPR line, containing the following
         columns:

         - Common TCDIAG stats columns (Table 24.1 in MET User's Guide)
         - TCMPR stats columns (Table 24.2 in MET User's Guide)
         - TCDIAG stats columns (Table 24.3 in MET User's Guide)


       .. dropdown:: Unformatted Example:

          .. literalinclude:: ./figure/al092022_20220926_DIAGNOSTICS.tcst

       - In the unformatted example:

         - The TCDIAG line is directly below its corresponding TCMPR line
         - TCDIAG columns are **not** in separate columns, they are located under existing TCMPR columns:

            - The INDEX column contains the INDEX value of the corresponding TCMPR line
              (this will be the line above the TCDIAG line)

            - The following existing TCMPR columns are "re-used":

              - The TCMPR **LEVEL** column corresponds to the TCDIAG **DIAG_SOURCE** column
              - DIAG_SOURCE identifies the diagnostics data source

              - The TCMPR **WATCH_WARN** column corresponds to the TCDIAG **TRACK_SOURCE** column
              - TRACK_SOURCE is the ATCF ID of the track data used to define the diagnostics

              - The TCMPR **INITIALS** column corresponds to the TCDIAG **FIELD_SOURCE** column
              - The FIELD_SOURCE is a description of gridded field data source used to define the diagnostics

              - The TCMPR **ALAT** column corresponds to the TCDIAG **N_DIAG** column
              - N_DIAG provides the number of the storm diagnostic name and value columns

              - The remaining TCMPR columns are "re-used" by TCDIAG to define each DIAG_i value followed by its corresponding
                VALUE_i


      .. dropdown:: Reformatted Example:

         .. literalinclude:: ./figure/tcmpr_reformatted.txt

      - In the reformatted example:

       - The data corresponds to the TCDIAG data consolidated with the corresponding TCMPR line
       - The unlabelled first column contains index values created during the reformatting process
       - The LINE_TYPE column is now located AFTER the last TCMPR column (i.e. MAX_WIND_STDEV)
       - The INDEX_PAIRS column corresponds to the INDEX column of TCDIAG (Table 24.3 of the MET User's Guide)
         - renamed INDEX_PAIRS to **differentiate it from the INDEX column of TCMPR**
       - The DIAG_SOURCE for this data is from CIRA_DIAG_RT
       - The N_DIAG column indicates the number of diagnostics for this line/row of data:

         - Inspecting the first row of data, the N_DIAG column has a value of **4**:

           - **Four** storm diagnostics columns with values are found:

             - SHEAR_MAGNITUDE
             - STORM_SPEED
             - TPW
             - DIST_TO_LAND

         - Inspecting the 24th line (the first row with DIAG_SOURCE=SHIPS_DIAG_RT), the N_DIAG column has a value of **3**:

           - **Three** storm diagnostics columns with values are found:

             - SHEAR_MAGNITUDE
             - DIST_TO_LAND
             - PW01


    .. dropdown::  by specific linetype: ECNT

       - The ECNT linetype (from the MET ensemble-stat tool) can be reformatted to contain all the ECNT statistic values specified in `Table 13.2 of the MET User's Guide <https://met.readthedocs.io/en/develop/Users_Guide/ensemble-stat.html#id2>`_.

            - in addition, the following values are separated into additional columns:

              - stat_name
              - stat_value
              - stat_ncl

                - lower level normal confidence limit

              - stat_ncu

                - upper level normal confidence limit

              - stat_bcl

                - lower level bootstrap confidence limit

              - stat_bcu

                - upper level bootstrap confidence limit

       .. dropdown:: Reformatted Example (no aggregation statistics step needed):

          .. literalinclude:: ./figure/ecnt_reformatted.data

          In the example above:

          - the statistics are separated into stat_name and stat_value columns
          - the statistic names under the stat_name column correspond to the ECNT column names specified in the MET
            User's Guide
          - the corresponding statistics value is located under the stat_value column
          - if confidence limits are available, they are located under their corresponding column:

            - stat_ncl
            - stat_ncu
            - stat_bcl
            - stat_bcu

       - Some plots require aggregation statistics (i.e. mean, sum, confidence levels, etc.)

         - an example of when aggregation statistics are needed is when using the METplotpy line plot to generate an
           ensemble spread skill plot that consists of ratio lines (e.g. ECNT spread_plus_oerr/rmse)

         - the METcalcpy agg_stat.py module can be used to calculate these aggregation statistics, but requires
           all the ECNT statistic values specified in `Table 13.2 of the MET User's Guide <https://met.readthedocs.io/en/develop/Users_Guide/ensemble-stat.html#id2>`_
           in addition to a stat_name column and stat_value column

           - the stat_name column contains all the ECNT statistic names pre-pended with **ECNT_**:

             - RMSE is replaced by ECNT_RMSE
             - SPREAD_PLUS_OERR is replaced by ECNT_SPREAD_PLUS_OERR
             - ... etc.

           - the stat_value column is empty and will be populated by the METcalcpy agg_stat.py module with the
             computed aggregate statistic value

         .. dropdown:: Reformatted Example (for input to METcalcpy agg_stat.py)

            .. literalinclude:: ./figure/reformatted_ecnt_for_agg_stat.data

            In the example above:

            - all ECNT columns are present, as specified in the MET User's Guide
            - the statistics under the stat_name column correspond to the header names specified in the MET User's Guide,
              pre-pended with **ECNT_**
            - the stat_value column is empty


Required Components
===================

Use the **MET stat-analysis** tool to filter data (by criteria such as model, valid times, etc.).
The output from the stat-analysis tool can then be used
as input to the METdataio METreformat reformatter.  If filtering of data is not needed, the .stat files from the MET
point-stat, grid-stat, and ensemble-stat tools can be used as input to the reformatter. If aggregation statistics
are needed, then the METcalcpy agg_stat.py module can be used following the reformatting step.
Reformatting to accommodate METcalcpy agg_stat is currently only available for the **ECNT linetype**.
The *input_stats_aggregated* setting is used to indicate whether the reformatter needs to
reformat the output for the METcalcpy agg_stat module.

METdbLoad modules are used to find and collect data from the individual .stat files into
one data structure.  The input .stat files must all reside under one directory. The path to this
input data is specified in a YAML configuration file.

The YAML configuration file also indicates the name and
location of the output file, logging information (filename, log level),
and the line type to reformat.

Copy the reformat_stat.yaml config file from the directory where the source code
was saved to the working directory.

.. dropdown:: Modify the reformat_stat.yaml configuration file (click to view config file)

    .. literalinclude:: ../../../METdataio/METreformat/reformat_stat.yaml

    Refer to the following details for each of the mandatory settings in the configuration file.

    .. dropdown:: Definition of Mandatory Config Settings

       .. dropdown:: input_stats_aggregated

            - By default, this is set to **True** to:
              -indicate that the input data has been processed by the MET stat-analysis
              tool to calculate aggregation statistics

               **or**

             - if the data of interest does *not* require calculation of aggregation statistics. This
               reformatted data can be used as input to the appropriate METplotpy plotting script.

            - Otherwise, set this to *False* if aggregation statistics need to be calculated (METcalcpy agg_stat module).

       .. dropdown:: input_data_dir

         - The full path (no environment variables) to the directory that contains all the input .stat files from the MET point-stat, grid-stat, or ensemble-stat tool
         - If data is distributed among numerous directories, they will need to be consolidated into one directory
    
       .. dropdown:: output_dir

         - The full path (no environment variables) to the directory where the reformatted file will be saved

       .. dropdown:: output_filename

         - The name of the output file
         - **NOTE**: save with .data extension if this is to be used for plotting using METplotpy
         - If reformatting is run successively without removing an existing output file of the same name, the existing file
           will be overwritten.

       .. dropdown:: log_filename

         - The name of the log file
         - Set to STDOUT or stdout (case insensitive) if no log file is to be saved

       .. dropdown:: log_dir

         - The full path to the directory (no environment variables are supported) where the log file is to be saved
  
       .. dropdown:: log_level

         - The verbosity of the logging: INFO, DEBUG, WARNING, ERROR
         - INFO is the most verbose, ERROR is least verbose

       .. dropdown:: line_type

         - The line type to be reformatted
         - Currently supported line types are:

           * FHO
           * CNT
           * CTC
           * CTS
           * SL1L2
           * VL1L2
           * ECNT
           * MCTS
           * VCNT
           * RHIST
           * PCT
           * TCDIAG
           * MPR
           * DMAP

       .. dropdown:: keep_all_cols

         - relevant for reformatting MPR or DMAP linetypes only
         - True if reformatting for scatter plot, False otherwise
         - For scatter plots, all column names are needed but the stat_name, stat_value, and confidence limits are not
           needed

Example
=======

- set up a base directory, where the METdataio source code is located

.. code-block:: ini

   bash:
   export BASE_DIR=/path/to/METdataio

   csh:
   setenv BASE_DIR /path/to/METdataio

- replace /path/to with an actual path

- set up a working directory, where the YAML config file will be located

.. code-block:: ini

   bash:
   export WORKING_DIR=/path/to/working_dir

   csh:
   setenv WORKING_DIR /path/to/working_dir


.. note::  Do NOT use environment variables for /path/to, specify the actual path.

- set the PYTHONPATH:


.. code-block:: ini

  bash
  export PYTHONPATH=$BASE_DIR:/$BASE_DIR/METdbLoad:$BASE_DIR/METdbLoad/ush:$BASE_DIR/METreformat

  csh
  setenv PYTHONPATH $BASE_DIR:/$BASE_DIR/METdbLoad:$BASE_DIR/METdbLoad/ush:$BASE_DIR/METreformat



Generate the reformatted file:
------------------------------

   - place the .stat/.tcst data of interest (output from MET tool) into a single directory

     .. note::

          This requires data to be located in a single directory.  If data is distributed over numerous directories, they will need to be reorganized under a single directory.


   - modify the **reformat_stat.yaml** file (refer to instructions above in section 6.2 **Required Components**)

        - specify the *input_stats_aggregated* to indicate

          - if the input MET data is generated from stat-analysis
          - **or no further aggregation statistics** are needed via METcalcpy agg_stat module

        - specify the *input directory*
        - specify the *output directory*
        - specify the *output file name*
        - specify the *line stat to reformat*
        - specify the *logging settings*:

          - log level
          - log filename
          - log directory
        - specify *keep_all_cols*

           - relevant only for the MPR and DMAP linetypes

     Refer to the
     **Definition of Mandatory Config Settings** pull-down under the **Modify the reformat_stat.yaml configuration file**
     in section **6.2 Required Components** (above)

**Run the following command from the command line:**

.. code-block:: ini


   python $BASE_DIR/METreformat/write_stat_ascii.py $WORKING_DIR/reformat_stat.yaml


- A text file will be created in the output directory with the file name that was specified in the yaml file.
