
*************************************
Running Unit Tests Locally
*************************************

Background
===========

Unit tests are included in METdataio and all of its sub-modules. The tests are found under the `test/` directory in each module, e.g. `METdbLoad/test/`.
These tests are run automatically when a pull request is raised on GitHub and must pass before any merge will be considered. 
When developing new features it is advisable to ensure the test pass by running them locally against your changes. To do this you must first have either
a `mysql` or `mariadb` service running and setup with an appropriate user. Although either database can be used, this guide will focus on `mariadb`.

Database Setup
==============

The setup of `mariadb` will be different depending on the operating system you are running, and the available privileges. If you encounter issues it is recommended you consult your system administrator
for support appropriate to your system and environment. For up to date instructions on installing mariadb consult the `MariaDB docs <mariadb.org>`_ or the `MariaDB GitHub page <https://github.com/MariaDB/>`_.

Below is an example setup on CentOS.

1. Install mariadb-server: 

.. code-block:: console

    $ sudo yum install mariadb-server

2. start mariadb and change the root password. Note that by default there is no password for the root user:

.. code-block:: console

    $ sudo mariadb -u root -p
    [MariaDB]> ALTER USER 'root'@'localhost' IDENTIFIED BY 'root_password';
    [MariaDB]> exit;

3. start the mariadb service:

.. code-block:: console

    $ sudo systemctl start mariadb

Running Tests
=============

Tests can be run using `pytest`. If required, you can install using either `conda install pytest` or `pip install pytest`.

.. code-block:: console

    $ pytest METdbLoad/test/

To check test coverage `conda install pytest-cov`, and using the `--cov` commands. For example:

.. code-block:: console

    $ pytest METdbLoad/test/ --cov METdbLoad/ush/ --cov-report term-missing

Writing Tests
=============

All Pull Requests that change source code in `METdataio` should include appropriate unit tests. These tests should 
demonstrate that when the new source code is invoked it produces the desired results. For help with writing tests, and for examples 
of how to use pytest, refer ot the `pytest documentation <https://docs.pytest.org/>`_.

When writing new tests for `METdataio` you should be familiar with the content of `conftest.py`, noting that each sub module may have it's own `conftest.py`.
For example, when writing a test for `METdbLoad` you may want to instatiate an empty test database. To do this use the `emptyDB` test fixture, found in `METdbLoad/conftest.py`.

For further examples, refer to the existing tests for each module.

Adding Tests for a New METreformat Line Type
--------------------------------------------

METreformat converts MET `.stat` (and `.tcst`) output into the format that METplotpy and the METcalcpy
`agg_stat` module expect. Support is added one line type at a time, so the tests in
`METreformat/test/test_reformatting.py` cover every line type, including ones that are not yet implemented.
When you add support for a line type, update the tests as described below.

Overview of the Reformatting Code
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All reformatting logic is in the `WriteStatAscii` class in `METreformat/write_stat_ascii.py`:

* `write_stat_ascii(stat_data, parms)` subsets the input data to the requested `line_type`, calls
  `process_by_stat_linetype`, writes the result to `<output_dir>/<output_filename>` as a tab-separated file,
  and returns the reformatted `pandas.DataFrame`.
* `process_by_stat_linetype` looks up the handler method for the line type in the registry built by
  `_build_handler_registry()` and calls it.
* Each line type has up to two handler methods:

  * `process_<linetype>` handles **aggregated** input (`input_stats_aggregated: True`), i.e. output from
    MET stat-analysis. It reshapes the data from wide to long form with the `stat_name`, `stat_value`,
    `stat_ncl`, `stat_ncu`, `stat_bcl`, and `stat_bcu` columns. Some line types (e.g. PCT, RHIST, TCDIAG)
    use a plot-specific format instead.
  * `process_<linetype>_for_agg` handles **non-aggregated** input (`input_stats_aggregated: False`), i.e.
    output from point-stat, grid-stat, ensemble-stat, etc. It formats the data for METcalcpy `agg_stat`:
    one column per line type statistic (lowercase header names), plus a `stat_name` column containing
    `<LINETYPE>_<STAT>` (e.g. `ECNT_CRPS`) and a `stat_value` column set to `NaN` for `agg_stat` to fill in.

A `NotImplementedError` is raised in these cases:

* the line type is not in the registry,
* the registry entry for the requested mode is `None`, or
* the handler method is a stub that raises `NotImplementedError` (e.g. `process_val1l2`).

Step 1: Implement the Line Type Handler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Make sure the line type's constants exist in `METdbLoad/ush/constants.py`. Existing handlers use:

   * the line type name (e.g. `cn.ECNT`),
   * the number of columns to use (e.g. `cn.NUM_STAT_ECNT_COLS`),
   * the full header list (e.g. `cn.ECNT_HEADERS`, which is `cn.LC_COMMON_STAT_HEADER + ['total'] + ...`),
   * the list of statistic names that are melted into `stat_name`/`stat_value`
     (e.g. `cn.ECNT_STATISTICS_HEADERS`), and
   * for the `_for_agg` handler, the lowercase statistic names (e.g. `cn.LC_ECNT_SPECIFIC`).

   Check these columns against the MET User's Guide for the MET version that produced your test data,
   because line type columns sometimes change between MET versions.

2. Check the line type's entry in `_build_handler_registry()`. Many line types (e.g. VAL1L2, MCTC, NBRCTC,
   NBRCTS, NBRCNT, SSVAR, GRAD, RPS, ECLV, PSTD, PJC, PRC) are already registered and point to stub methods.
   Other line types that may never be implemented (ISC, PHIST, ORANK, RELP, ENSCNT, PERC, SSIDX, SEEPS,
   SEEPS_MPR) are registered with `None` for both modes and have no stub methods. To add support for one
   of these, replace `None` with the handler method name and add the method. For a line type that is not
   registered at all, add an entry:

   .. code-block:: python

       cn.NEW_TYPE: {
           'aggregated': 'process_new_type',
           'non_aggregated': 'process_new_type_for_agg',
       },

   Set a mode to `None` if that mode will never be supported for the line type. Then
   `_validate_handler_support` raises a `NotImplementedError` with a message listing the supported modes.

3. Replace the `raise NotImplementedError(...)` stub in `process_<linetype>` and/or
   `process_<linetype>_for_agg` with the implementation. Use similar existing handlers as templates:

   * `process_ecnt`, `process_sl1l2`, and `process_cnt` show the aggregated wide-to-long reshape with
     `DataFrame.melt`.
   * `process_ecnt_for_agg` and `process_sal1l2_for_agg` show the non-aggregated format for `agg_stat`.

   Each handler receives the full `stat_data` dataframe, which has the common header columns followed
   by the line-type-specific columns named `'0'`, `'1'`, `'2'`, .... The handler should:

   * select only the rows for its line type,
   * select the first `NUM_STAT_<LINETYPE>_COLS` columns,
   * assign the header names from `constants.py`, and
   * return a `DataFrame`.

   Do not write files from the handler; `write_stat_ascii` writes the output file.

4. Update the docstrings of `write_stat_ascii` and `process_by_stat_linetype` if they list supported
   line types. Also update the User's Guide (`docs/Users_Guide/reformat_stat_data.rst`) to document the new
   line type and its output format.

Step 2: Add Input Data (if needed)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Test input data is in `METreformat/test/data/` by default (see :ref:`Test Input Location <metreformat_test_input_location>`
to read it from another directory). First check whether an existing directory already has
`.stat` files with the new line type, e.g. by running:

.. code-block:: console

    $ grep -rl " VAL1L2 " METreformat/test/data

If none exists, add a small set of MET output files that contain the line type:

* Put the files in a **new directory** for the line type, e.g. `METreformat/test/data/point_stat/val1l2`.
  The test reader loads every file directly in the directory (it does not recurse into subdirectories).
  If you add files to a shared directory, other tests that read that directory also load them, which can
  change their results.
* Keep the files small. A few files with a modest number of lines is enough and keeps the test suite fast.
* If possible, use output from the current MET version, so the columns match the definitions in
  `constants.py`. If the column layout changed between MET versions, consider adding data from each
  version in separate directories (e.g. `vl1l2_MET13`).
* For aggregated tests, the data should come from stat-analysis. For non-aggregated tests, the data
  should come directly from the MET tool. One directory may contain both if the line type appears in both.

Step 3: Add an Entry to `input_data_dir_lookup`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`build_dispatch_test_config()` in `test_reformatting.py` builds a minimal config dictionary for a test,
so you don't need a separate YAML config file for each test. The `input_data_dir_lookup` dictionary
sets the input data directory:

.. code-block:: python

    input_data_dir_lookup = {
        cn.ECNT: 'ensemble_stat',
        ...
        cn.VAL1L2: 'point_stat/val1l2',      # new line type
        'VAL1L2_for_MET13': 'point_stat/val1l2_MET13',  # optional test_name-specific data
    }

How the lookup works:

* Paths are relative to the test input data directory (`METreformat/test/data/` by default).
* The key can be either a line type constant (e.g. `cn.VAL1L2`) or a custom `test_name` string.
  The lookup tries `test_name` first and then the line type. If neither matches, it falls back to
  `point_stat`. Because of this fallback, a missing entry may not cause an error, but the test may
  read the wrong data. Always add an entry for a new line type.
* Use a `test_name` key when one line type needs different input data for different tests, e.g.
  `'VCNT_for_MET13'` or `'mpr_climo_data'`.
* `is_tcst` is `True` only for `cn.TCMPR` and `cn.TCDIAG`. If the new line type is read from `.tcst`
  files (TC-Pairs/TC-Stat output), add it to the `is_tcst` set in `build_dispatch_test_config()`, so the
  `.tcst` data is returned instead of the `.stat` data.

Step 4: Update `test_process_by_stat_linetype_dispatch`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`test_process_by_stat_linetype_dispatch` runs once for each `(linetype, is_aggregated, is_implemented)`
tuple in its `pytest.mark.parametrize` list:

* If `is_implemented` is `False`, the test checks that `write_stat_ascii` raises a `NotImplementedError`
  and that no output file is created.
* If `is_implemented` is `True`, the test checks that `write_stat_ascii` returns a non-empty
  `DataFrame` and that the output file exists.

After implementing a handler, change `is_implemented` to `True` for that line type and mode. For example,
after implementing `process_val1l2`:

.. code-block:: python

    (cn.VAL1L2, True, True),    # aggregated: now implemented
    (cn.VAL1L2, False, False),  # non-aggregated: still raises NotImplementedError

Notes:

* Each line type should have a tuple for both `is_aggregated=True` and `is_aggregated=False`, so the
  test covers both modes. If you add a new line type, add both tuples.
* For an implemented mode, the test fails if the input data has no rows for the line type, because the
  returned `DataFrame` is empty. This usually means the `input_data_dir_lookup` entry is missing or
  points to the wrong directory.
* This test only checks that a handler runs and produces output. It does not check the values. Add a
  separate test that checks the values (see the next step).

Step 5: Write Tests that Check the Output Values
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use `setup_test()` to read the input data and get a config dictionary for a new test:

.. code-block:: python

    stat_data, parms = setup_test(linetype, test_name=None, is_aggregated=True, for_scatter=False)

* `linetype`: the line type to test, e.g. `'VAL1L2'` or `cn.VAL1L2`.
* `test_name`: optional. Selects a `test_name`-specific entry in `input_data_dir_lookup` and sets the output
  filename. If not set, the line type is used.
* `is_aggregated`: sets `input_stats_aggregated`, which chooses between `process_<linetype>` and
  `process_<linetype>_for_agg`. It also adds `_for_agg` to the output filename when `False`.
* `for_scatter`: sets `keep_all_cols`, which is used when reformatting MPR/DMAP data for scatter plots.

It returns:

* `stat_data`: a `DataFrame` with all data read from the input directory.
* `parms`: the config dictionary to pass to `WriteStatAscii` and `write_stat_ascii`.

The output file is written to `<output_dir>/<test_name lowercase>[_for_agg]_reformatted.data`.
`write_stat_ascii` **appends** to the output file (`mode='a'`). If two tests use the same line type and
`test_name`, they write to the same file and the second test's output is appended to the first.
Give each new test that calls `write_stat_ascii` a unique `test_name`.

To catch these duplicates, call `fail_if_output_exists(parms)` before **every** call to `write_stat_ascii`
in a test. Do this even if the call is expected to raise an exception. If the output file already exists,
the test fails with a message telling you to resolve the duplicate. Because the default output directory
is emptied at the start of each test run, an existing file means that another test in the same run uses
the same output filename. To fix it, pass a unique `test_name` to `setup_test`. If no
`input_data_dir_lookup` entry matches the new `test_name`, the lookup uses the line type's entry, so the
test still reads the same input data.

Example test for the aggregated mode that compares one value in the input data with the reformatted output:

.. code-block:: python

    def test_val1l2_consistency():
        """Verify that a value in the original VAL1L2 data matches the value
           in the reformatted output.
        """
        stat_data, parms = setup_test(cn.VAL1L2, test_name='val1l2_consistency')

        # Subset the original data to VAL1L2 rows and add the header names
        val1l2_columns = np.arange(0, cn.NUM_STAT_VAL1L2_COLS).tolist()
        val1l2_df = stat_data[stat_data['line_type'] == cn.VAL1L2].iloc[:, val1l2_columns]
        val1l2_df.columns = cn.VAL1L2_HEADERS
        expected_row = val1l2_df.iloc[0]

        # Reformat the data and write the output file
        wsa = WriteStatAscii(parms, logger)
        fail_if_output_exists(parms)
        reformatted_df = wsa.write_stat_ascii(stat_data, parms)
        output_file = os.path.join(parms['output_dir'], parms['output_filename'])
        assert os.path.exists(output_file)

        # Find the matching row in the reformatted data and compare
        actual = reformatted_df.loc[
            (reformatted_df['total'] == expected_row['total']) &
            (reformatted_df['fcst_var'] == expected_row['fcst_var']) &
            (reformatted_df['fcst_lev'] == expected_row['fcst_lev']) &
            (reformatted_df['stat_name'] == 'UFABAR')]
        assert actual.iloc[0]['stat_value'] == expected_row['UFABAR']

The constant names in this example (e.g. `cn.NUM_STAT_VAL1L2_COLS` and `cn.VAL1L2_HEADERS`) are for
illustration. Use the names defined in `constants.py` for your line type.

Tips for writing these tests:

* To test only the reshaping logic without writing a file, call the handler directly
  (e.g. `wsa.process_val1l2(stat_data)`). To test the whole process, including the line type subsetting,
  `NA` replacement, and file output, call `wsa.write_stat_ascii(stat_data, parms)`.
* `write_stat_ascii` replaces `NaN` values with `'NA'` before calling the handler. A test that calls the
  handler directly does not do this replacement.
* Values in `stat_data` are read as strings. Compare them as strings, or convert both sides to numbers
  before comparing.
* Good things to check:

  * the expected columns are present,
  * the set of `stat_name` values matches the list of statistics for the line type,
  * the number of output rows equals (input rows) × (number of statistics),
  * a few specific values match the input data, and
  * missing values are represented as expected.

  For `_for_agg` output, also check that every `stat_name` has the `<LINETYPE>_` prefix and that
  `stat_value` is `NaN`. See `test_ecnt_reformat_for_agg` for an example.
* You can change `parms` before creating `WriteStatAscii` if a test needs a non-default setting
  (e.g. `parms['input_stats_aggregated'] = False`).
* `_setup_test_from_yaml()` is still available for a test that must read an existing YAML config file,
  but `setup_test()` is preferred.
* Remove or update any older test that expects a `NotImplementedError` for the newly implemented
  handler (e.g. `test_process_ctc_agg` checks that `process_ctc_for_agg` raises `NotImplementedError`).

.. _metreformat_test_input_location:

Test Input Location
~~~~~~~~~~~~~~~~~~~

By default, the tests read input data from `METreformat/test/data/`. To read it from another directory, set
the `METREFORMAT_TEST_INPUT_DIR` environment variable to a directory with the same layout (e.g. it contains
`ensemble_stat/`, `point_stat/`, etc.):

.. code-block:: console

    $ export METREFORMAT_TEST_INPUT_DIR=/path/to/metreformat_test_data
    $ pytest METreformat/test/test_reformatting.py

If the input data directory does not exist, each test that reads input data fails with a message that
names the missing directory. Tests that do not read input data still run.

Test Output Location
~~~~~~~~~~~~~~~~~~~~

By default, test output (reformatted data files and `test_reformatting_log.txt`) is written to
`METreformat/test/output/`. This directory is emptied automatically at the start of each test run, so the
tests do not need to delete their output files. To write the output to another directory, set the
`METREFORMAT_TEST_OUTPUT_DIR` environment variable. A custom directory is **not** emptied automatically;
a warning is printed if it is not empty, and you must clear it before running the tests.

To run only the METreformat tests, or a subset of them:

.. code-block:: console

    $ pytest METreformat/test/test_reformatting.py
    $ pytest METreformat/test/test_reformatting.py -k "VAL1L2 or val1l2"

Checklist
~~~~~~~~~

When adding support for a new line type:

#. Add or confirm the line type constants in `METdbLoad/ush/constants.py`.
#. Add or update the registry entry in `WriteStatAscii._build_handler_registry()`.
#. Implement `process_<linetype>` and/or `process_<linetype>_for_agg`.
#. Add input data in its own directory under `METreformat/test/data/`, if needed.
#. Add the data directory to `input_data_dir_lookup` in `build_dispatch_test_config()`, and add the line type
   to `is_tcst` if it is read from `.tcst` files.
#. Change `is_implemented` to `True` for the implemented mode(s) in the
   `test_process_by_stat_linetype_dispatch` parameters.
#. Add tests that use `setup_test()` to check the output values, using a unique `test_name` and calling
   `fail_if_output_exists()` before each call to `write_stat_ascii`.
#. Remove or update older tests that expect a `NotImplementedError`.
#. Update the User's Guide documentation for METreformat.
