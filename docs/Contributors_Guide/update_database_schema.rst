

*************************************
Update the Database Schema
*************************************

Background
===========

The METdataio METdbLoad module is used to load input data into the METplus Analysis database. It is important that
the database schema is updated when the following conditions apply:

* The MET tool has introduced a *new linetype*
* The MET tool has *modified one or more existing linetype(s)*

If no changes exist, then an update to the database schema is unnecessary.

Steps
=====

1. Create a Github issue in the METdataio repository.

*  Click on the **Issues** tab at the top of the `METdataio Repository page <https://github.com/dtcenter/METdataio>`_
*  Click on the green **New issue** button on the top right of the issues page.
*  Click on the green **Get started** button corresponding to the **Task** template
*  Assign the appropriate settings under
   the *Assignees*, *Labels*, *Projects* and *Milestones* links to the right side of the Github issues
   page.
*  Fill in relevant information under the **Add a description** window


2. Create a feature branch corresponding to this Github issue in the METdataio repository:

.. code-block:: ini

    feature_xyz_update_db_schema

where **xyz** corresponds to the Github issue number and is branched from the *develop* branch

.. code-block:: ini

   git checkout develop
   git checkout -b feature_xyz_update_db_schema

3. Create an **update_for_x_y.sql** file in the $BASE_DIR/METdbLoad/sql/updates directory

   * $BASE_DIR corresponds to the directory where the METdataio source code resides

4. In the **update_for_x_y.sql** file created above, make any changes to reflect updates to the schema
   The `MET release guide  <https://met.readthedocs.io/en/latest/Users_Guide/release-notes.html>`_
   is a useful resource for determining which linetypes were added or modified.
   Test that this loads the schema updates correctly.

5. In the $BASE_DIR/METdataio/METdbLoad/sql/mv_mysql.sql file, make necessary edits corresponding to the latest
   changes in the database schema.

   For example, if adding columns, use syntax like the following:
.. code-block:: ini

   DELIMITER |
      ALTER TABLE line_data_val1l1
          ADD COLUMN dira_ma DOUBLE |

   DELIMITER ;

In the example above, the *dira_ma* column is to be added to the existing **VAL1L2** linetype
columns, with type 'DOUBLE'.

Remember to include the 'DELIMITER |' at the beginning/top of the file and 'DELIMETER ;' at the end of the file.

6. Update the Release Notes under the $BASE_DIR/METdataio/docs/Users_Guide/release-notes.rst under the
   **METdataio Upgrade Instructions** section at the bottom of the documentation

   * $BASE_DIR corresponds to the directory where the METdataio source code resides

7. Add and commit the changes.

In the $BASE_DIR/METdataio/METdbLoad/sql/updates directory:

.. code-block:: ini

   git add update_for_x_y.sql
   git commit update_for_x_y.sql

* Replace **x_y** with the appropriate version

In the $BASE_DIR/METdataio/METdbLoad/sql directory:

.. code-block:: ini

   git commit mv_mysql.sql



* The git commit will generate a pop-up box for adding comments.  Include the Github issue number in
  the comment and provide a concise description of what was done.

8. Submit a Github PR (at least one reviewer is required).

9. Perform a Squash and Merge once the PR has been approved.

10. Close the PR and close the Github issue









