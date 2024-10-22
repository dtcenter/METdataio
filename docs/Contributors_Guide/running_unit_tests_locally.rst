
*************************************
Running Unit Tests Locally
*************************************

Background
===========

The METdataio METdbLoad module includes unit tests, found in `METdbload/test/`.
These tests are run automatically when a pull request is raised on GitHub and must pass before any merge will be considered. 
When developing new features it is advisable to ensure the tests will pass by running them locally. To do this you must first have either
a `mysql` or `mariadb` service running and setup with an appropriate user.

Steps
=====