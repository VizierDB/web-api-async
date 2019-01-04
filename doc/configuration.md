Vizier API Configuration
========================

The Vizier API is configured using a configuration object that contains (i) API parameters and default settings for the Web service, (ii) definition of the execution engine for data curation workflows, and (iii) additional server specific information (e.g., the log file folder).


Configuration Parameters
------------------------

The API is configured using a dictionary of configuration parameters. The parameters are divided into the following parts:

```
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
engine:
    identifier: Unique engine configuration identifier
    properties: Configuration specific parameters
debug: Flag indicating whether server is started in debug mode
logs:
    server: Log file for Web Server
```

Vizier Engine
-------------

The **Vizier Engine** defines the interface that is used by the API for creating, deleting, and manipulating projects. Different implementations of the engine will use different implementations for datasores, filestores, vitrails repositories and backends.

The class that contains the engine that is being used by a Vizier instance is defined in the *engine* section of the configuration file. The section may contain another element *properties* that contains an engine-specific dictionary of configuration parameters. This dictionary is passed to the specified engine object when it is instantiated.

## Configuring the Local Vizier Engine

The community edition environment runs on the local machine. Expects the definition of the datastore, filestore and viztrail repository that is used for all projects. Intended for single user that works on a single project. Uses multi-process backend.

A running Vizier instance has four main components: (1)) data store, (2) file store, (3) viztrails repository, and (4) workflow execution engine. For each of these components different implementations are possible. When initializing the instance the components that are to be used are loaded based on the respective configuration parameters.


## Configure the Default Viztrails Repository

The configuration parameter for the default viztrails repository are:

- *directory*: Path to base directory on file system where viztrails resources are being stored
- *keepDeletedFiles*: Boolean flag that indicates whether files are being physicaly deleted (False) or kept (True) when a viztrail or branch is deleted. Default is False.
- *useLongIdentifier*: Use long unique identifier if True or short eight character identifier if False. Default is False



Packages of Workflow Commands
-----------------------------

The commands that are available to be executed by workflow modules are defined in separate packages. Each package is represented by a dictionary that contains the definition of the commands (i.e., modules) that the package supports. Refer to [Workflow Modules](https://github.com/VizierDB/web-api/blob/master/doc/workflow-modules.md) for details on the structure of module definitions.

By default, the API only supports the VizUAL and the system packages. All other packages have to be loaded explicitly when the application configuration object is created (see below). Packages are loaded from files that contain serializations of dictionaries with command definitions (either in Yaml or Json format). For an example have a look at the [Mimir Package Definition](https://github.com/VizierDB/web-api/blob/master/config/mimir.pckg.json).

Note that a package declaration file can contain multiple packages. The *parameters* that are associated with a *declarations* element in the *packages* list will be passed to the constructor of the package engine for each of the packages in the file.



Initialization
--------------

The configuration is read from a file that is expected to be in Yaml format (unless the file name ends with .json in which case Json format is assumed). The file is expected to contain two elements: packages and settings.

packages: List of file names that point to package definition files. All files are expected to be in Yaml format unless the file name ends with .json.

settings: Application settings object. The structure of this object is expected to be the same as the structure shown above.

The configuration file can either be specified using the environment variable **VIZIERSERVER_CONFIG** or be located (as file **config.yaml**) in the current working directory.
