# Vizier Web API - Changelog

### 0.1.0

* Initial Version

### 0.2.0

* Second prototype

### 0.3.0 - 2018-01-22

* Workflow branches
* Dataset annotations
* Drop and rename datasets
* Mimir lenses and data store
* Deletion of workflow modules
* Access full history of workflows
* Access repository of available workflow modules via API
* Allow different workflow engines for different projects

### 0.3.1 - 2018-02-08

* Replace workflow engine dictionary with dictionary of execution environments
* Change workflow context to be a dictionary


### 0.3.2 - 2018-03-05

* Add file upload size limit to service description
* Enable upload of files that do not parse correctly as CSV/TSV files


### 0.4.0 - 2018-05-23

* Add support for dataset chart views
* Fix issues with dataset upload and column data type awareness
* Pagination for datasets
* Switch to column ids in module arguments
* Server-side command text for workflow modules
* Default label for data series
* Enable multiple annotation with same key for single object (e.g., multiple user comments)


### 0.4.1 - 2018-06-07

* Add GEOCODE lens


### 0.4.2 - 2018-06-12

* Push pagination queries to datastore
* Avoid materializing Mimir annotations in dataset metadata
* Encode special characters as unicode in dataset creation


### 0.4.3 - 2018-06-13

* Add VizUAL Sort and Filter Columns commands


### 0.4.4 - 2018-06-18

* Upload files from URL


### 0.5.0 - 2019-01-??

This is the first prototype that supports asynchronous execution of workflow modules. The prototype was created from the code base of the web API version 0.4.4. It incorporates changes that were made to the UB branch and that were merged into the master branch while the prototype was under development.

* Change definition and handling of packages
* Allow concurrent execution of workflow modules
* Datastore that uses the web API to access and manipulate dataset instead of the file system
* Support containerization of individual projects
* Command Line Interface to interact with the web API
