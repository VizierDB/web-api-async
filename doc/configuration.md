Vizier API Configuration
========================

The Vizier API is configured using a configuration object that contains (i) API parameters and default settings for the Web service, (ii) definition of the file store, (iii) definition of the data store, (iv) definition of the viztrails repository, and (v) the list of available packages and their respective configuration parameters.


Configuration Parameters
------------------------

The API is configured using a dictionary of configuration parameters. The parameters are divided into the following parts:

```
datastore:
    moduleName: Name of the Python module containing data store class
    className: Class name of data store
    properties: Dictionary of data store specific properties
debug: Flag indicating whether server is started in debug mode
filestore:
    moduleName: Name of the Python module containing file store class
    className: Class name of file store
    properties: Dictionary of file store specific properties
logs:
    server: Log file for Web Server
packages: # List of package declarations
    - declarations: File containing declaration of package commands
      parameters: Package specific configuration parameters
viztrails:
    moduleName: Name of the Python module containing repository class
    className: Class name of viztrails repository
    properties: Dictionary of repository specific properties
webservice:
    server_url: Url of the server (e.g., http://localhost)
    server_port: Public server port (e.g., 5000)
    server_local_port: Locally bound server port (e.g., 5000)
    app_path: Application path for Web API (e.g., /vizier-db/api/v1)
    app_base_url: Concatenation of server_url, server_port and app_path
    doc_url: Url to API documentation
    name: Web Service name
    defaults:
        row_limit: Default row limit for requests that read datasets
        max_row_limit: Maximum row limit for requests that read datasets (-1 = all)
        max_file_size: Maximum size for file uploads (in byte)
```

Note that a package declaration file can contain multiple packages. The *parameters* that are associated with a *declarations* element in the *packages* list will be passed to the constructor of the package engine for each of the packages in the file.


Packages of Workflow Commands
-----------------------------

The commands that are available to be executed by workflow modules are defined in separate packages. Each package is represented by a dictionary that contains the definition of the commands (i.e., modules) that the package supports. Refer to [Workflow Modules](https://github.com/VizierDB/web-api/blob/master/doc/workflow-modules.md) for details on the structure of module definitions.

By default, the API only supports the VizUAL and the system packages. All other packages have to be loaded explicitly when the application configuration object is created (see below). Packages are loaded from files that contain serializations of dictionaries with command definitions (either in Yaml or Json format). For an example have a look at the [Mimir Package Definition](https://github.com/VizierDB/web-api/blob/master/config/mimir.pckg.json).


Components of Vizier Instance
-----------------------------

A running Vizier instance has four main components: (1)) data store, (2) file store, (3) viztrails repository, and (4) workflow execution engine. For each of these components different implementations are possible. When initializing the instance the components that are to be used are loaded based on the respective configuration parameters.


## Configure the Default Viztrails Repository

The configuration parameter for the default viztrails repository are:

- *directory*: Path to base directory on file system where viztrails resources are being stored
- *keepDeletedFiles*: Boolean flag that indicates whether files are being physicaly deleted (False) or kept (True) when a viztrail or branch is deleted. Default is False.
- *useLongIdentifier*: Use long unique identifier if Ture or short eight character identifier if False. Default is False


Initialization
--------------

The configuration is read from a file that is expected to be in Yaml format (unless the file name ends with .json in which case Json format is assumed). The file is expected to contain two elements: packages and settings.

packages: List of file names that point to package definition files. All files are expected to be in Yaml format unless the file name ends with .json.

settings: Application settings object. The structure of this object is expected to be the same as the structure shown above.

The configuration file can either be specified using the environment variable **VIZIERSERVER_CONFIG** or be located (as file **config.yaml**) in the current working directory.
