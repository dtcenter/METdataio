Overview of METdbload
=====================


Purpose and Organization of the User's Guide
--------------------------------------------

The goal of this User's Guide is to provide basic information for users of the METdatadb database and the METviewer and METexpress display systems to enable users to create plots from their MET output statistics.

The METdbload User's Guide is organized as follows.


The Developmental Testbed Center (DTC)
--------------------------------------

METdbload has been developed, and will be maintained and enhanced, by the Developmental Testbed Center (DTC; http://www.dtcenter.org/ ). The main goal of the DTC is to serve as a bridge between operations and research, to facilitate the activities of these two important components of the numerical weather prediction (NWP) community. The DTC provides an environment that is functionally equivalent to the operational environment in which the research community can test model enhancements; the operational community benefits from DTC testing and evaluation of models before new models are implemented operationally. METdbload serves both the research and operational communities in this way - offering capabilities for researchers to test their own enhancements to models and providing a capability for the DTC to evaluate the strengths and weaknesses of advances in NWP prior to operational implementation.

METdbload will also be available to DTC visitors and to the modeling community for testing and evaluation of new model capabilities, applications in new environments, and so on.


METdbload goals and design philosophy
-------------------------------------

METdbload is the part of METdatadb that reads MET verification statistics ASCII files and loads them into a database for plotting with METviewer and METexpress. The specification for which files to load is written in XML.

The METdbload code and documentation is maintained by the DTC in Boulder, Colorado. The MET package is freely available to the modeling, verification, and operational communities, including universities, governments, the private sector, and operational modeling and prediction centers.


METdbload Requirements
----------------------

METdbload requires installation of Python 3.6+ and MySQL.


Future development plans
------------------------

METdatadb is an evolving verification database package. New capabilities are planned in controlled, successive version releases. Bug fixes and user-identified problems will be addressed as they are found. Plans are also in place to incorporate many new capabilities and options in future releases of METdatadb. Additionally, updates to accommodate new features of the MET software are often required.


Code support
------------

METdbload support is provided through a MET-help e-mail address: met_help@ucar.edu. We will endeavor to respond to requests for help in a timely fashion.

We welcome comments and suggestions for improvements to METdbload, especially information regarding errors. Comments may be submitted using the MET Feedback form available on the MET website. In addition, comments on this document would be greatly appreciated. While we cannot promise to incorporate all suggested changes, we will certainly take all suggestions into consideration.

METdbload is a "living" application. Our goal is to continually enhance it and add to its capabilities.