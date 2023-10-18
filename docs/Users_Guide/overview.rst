********
Overview
********

Purpose and Organization of the User's Guide
============================================

The goal of this User's Guide is to provide basic information for users of the
METdataio database and the METviewer and METexpress display systems to enable
users to create plots from their MET output statistics.

The METdataio User's Guide is organized as follows.  Currently, the METdataio
User's Guide only contains information about METdbload, a utility of
METdataio.  Eventually, there will be utilities to add and delete databases,
delete duplicate records, etc.

The Developmental Testbed Center (DTC)
======================================

METdataio has been developed, and will be maintained and enhanced, by the
Developmental Testbed Center (DTC; http://www.dtcenter.org/ ).
The main goal of the DTC is to serve as a bridge between operations and
research, to facilitate the activities of these two important components of
the numerical weather prediction (NWP) community. The DTC provides an
environment that is functionally equivalent to the operational environment
in which the research community can test model enhancements; the operational
community benefits from DTC testing and evaluation of models before new models
are implemented operationally. METdataio serves both the research and
operational communities in this way - offering capabilities for researchers
to test their own enhancements to models and providing a capability for
the DTC to evaluate the strengths and weaknesses of advances in NWP
prior to operational implementation.

METdataio will also be available to DTC visitors and to the modeling community
for testing and evaluation of new model capabilities, applications in new
environments, and so on.

METdataio Goals and Design Philosophy
=====================================

METdataio is a Python rewrite of the capabilities in METviewer. METdbload,
a METdataio utility, reads MET verification statistics ASCII files,
and loads them into a database for plotting with METviewer and METexpress.
The specification for which files to load is written in XML.

The METdataio code and documentation is maintained by the DTC in Boulder,
Colorado. The MET package is freely available to the modeling, verification,
and operational communities, including universities, governments,
the private sector, and operational modeling and prediction centers.

	     
Future Development Plans
========================

METdataio is an evolving verification database package. New capabilities are
planned in controlled, successive version releases. Bug fixes and
user-identified problems will be addressed as they are found. Plans are also
in place to incorporate many new capabilities and options in future releases
of METdataio. Additionally, updates to accommodate new features of the MET
software are often required.

Code Support
============

Support for METdataio is provided through the
`METplus GitHub Discussions Forum <https://github.com/dtcenter/METplus/discussions>`_.
We will endeavor to respond to requests for help in a timely fashion.

We welcome comments and suggestions for improvements to METdataio, especially
information regarding errors. In addition, comments on this document would be
greatly appreciated. While we cannot promise to incorporate all suggested
changes, we will certainly take all suggestions into consideration.

METdataio is a "living" application. Our goal is to continually enhance it and
add to its capabilities.
