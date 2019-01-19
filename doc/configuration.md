Configuration
=============

The Vizier API and all worker components are configured using environment variables in an attempt to follow [The Twelve-Factor App methodology](https://12factor.net/) for web application development. All relevant environment variables have default values that allow to run the web service using a default configuration without the need to set any variables. The current default configuration will run the service in development mode. 


Web Service REST-API
--------------------

The following environment variables control different parameters of the web API server. Additional variables to configure the different execution backends are described below.


### General

- ***VIZIERSERVER_NAME***: Web service name
- ***VIZIERSERVER_LOG_DIR***: Log file directory used by the web server (DEFAULT: *./.vizierdb/logs*)
- ***VIZIERSERVER_DEBUG***: Flag indicating whether server is started in debug mode (DEFAULT: *True*)

### Web Service

- ***VIZIERSERVER_BASE_URL***: Base url of the server running the web service (DEFAULT: *http://localhost*)
- ***VIZIERSERVER_SERVER_PORT***: Public server port (DEFAULT: *5000*)
- ***VIZIERSERVER_SERVER_LOCAL_PORT***: Locally bound server port (DEFAULT: *5000*)
- ***VIZIERSERVER_APP_PATH***: Application path for Web API (DEFAULT: */vizier-db/api/v1*)
- ***VIZIERSERVER_ROW_LIMIT***: Default row limit for requests that read datasets (DEFAULT: *25*)
- ***VIZIERSERVER_MAX_ROW_LIMIT***: Maximum row limit for requests that read datasets (DEFAULT: *-1* (returns all rows))
- ***VIZIERSERVER_MAX_UPLOAD_SIZE***: Maximum size for file uploads in bytes (DEFAULT: *16777216*)

The destinction between *VIZIERSERVER_SERVER_PORT* and *VIZIERSERVER_SERVER_LOCAL_PORT* is relevant when running the web service inside a Docker container. Otherwise the value for both variables should be identical.


### Workflow Execution Engine

- ***VIZIERSERVER_ENGINE***: Name of the workflow execution engine (DEFAULT: *DEV*)
- ***VIZIERSERVER_PACKAGE_PATH***: Path to the package declaration directory (DEFAULT: *./resources/packages/dev*)
- ***VIZIERSERVER_PROCESSOR_PATH***: Path to the task processor definitions for supported packages (DEFAULT: *./resources/processors/dev*)


The files in the package directory are used to initialize the registry of supported packages. Each file in the packages directory is expected to contain the declaration of a vizier packages. All files are read during service initialization to create the repository of available workflow packages.



Workflow Engine
-------------

The *vizier workflow engine* is used by the web API for creating, deleting, and manipulating projects and workflows. Different configurations for the engine can use different implementations of datasores, filestores, vitrails repositories, and execution backends. The engine configuration is controlled using the variable *VIZIERSERVER_ENGINE*. Vizier currently supports three different engine configurations:

- *DEV*: Development engine uses a simple datastore that maintains all datasets as separate files on the file system (*vizier.datastore.fs*). 
- *MIMIR*: The Mimir engine uses the Mimir gateway to store and manipulate datasets.
- *CLUSTER*: The cluster engine runs each project in an individual Docker container. The engine iteself does not have instances of the datstore and filestore. Instead, each of the containers will have ther own datastore, filestore, and execution backend.

At this point there exists only one implementation for the viztrails repository interface (*vizier.viztrails.objectstore*) as wel as for the filestore interface (*vizier.filestore.fs*). Both implementations are therefore used by all three configurations.


The community edition environment runs on the local machine. Expects the definition of the datastore, filestore and viztrail repository that is used for all projects. Intended for single user that works on a single project. Uses multi-process backend.

- ***VIZIERENGINE_BACKEND***: Name of the used backend (CELERY, MULTIPROCESS, or CONTAINER) (DEFAULT: MULTIPROCESS)
- ***VIZIERENGINE_SYNCHRONOUS***: Colon separated list of package.command strings that identify the commands that are executed synchronously
- ***VIZIERENGINE_USE_SHORT_IDENTIFIER***: Flag indicating whether short identifier are used by the viztrail repository

A running Vizier instance has four main components: (1)) data store, (2) file store, (3) viztrails repository, and (4) workflow execution engine. For each of these components different implementations are possible. When initializing the instance the components that are to be used are loaded based on the respective configuration parameters.

Not all of them are supported by each of the engines. Look at the engine specific documentation below.

- **VIZIERENGINE_BACKEND**: Name of the used backend (CELERY, MULTIPROCESS, or CONTAINER) (DEFAULT: MULTIPROCESS)
- **VIZIERENGINE_USE_SHORT_IDENTIFIER**: Flag indicating whether short identifier are used by the viztrail repository
- **VIZIERENGINE_SYNCHRONOUS**: Colon separated list of package.command strings that identify the commands that are executed synchronously

### Development

- **VIZIERENGINE_DATA_DIR**: Base data directory for the default engine


### Celery

- **VIZIERENGINE_CELERY_ROUTES**: Colon separated list of package.command=queue strings that define routing information for individual commands
- **CELERY_BROKER_URL**: Url for the celery broker

Engines: DEV_LOCAL, DEV_CELERY


### Container

- **VIZIERENGINE_DATA_DIR**: Base data directory for viztrails and configuration files
- **VIZIERENGINE_CONTAINER_PORTS** : First port number for new project containers. All following containers will have higher port numbers
- **VIZIERENGINE_CONTAINER_IMAGE**: Unique identifier of the project container docker image


## Configure the Default Viztrails Repository

The configuration parameter for the default viztrails repository are:

- *directory*: Path to base directory on file system where viztrails resources are being stored
- *keepDeletedFiles*: Boolean flag that indicates whether files are being physicaly deleted (False) or kept (True) when a viztrail or branch is deleted. Default is False.
- *useShortIdentifier*: Use short eight character identifier if True or long 32 character identifiers if False. Default is False


Worker Config
--------------

- **VIZIERWORKER_ENV**: Identifier for environment in which the worker operates (supported values are DEV or REMOTE) (DEFAULT: *DEV*)
- **VIZIERWORKER_PROCESSOR_PATH**: Path to the task processor definitions for supported packages (DEFAULT: *./resources/processors*)
- **VIZIERWORKER_CONTROLLER_URL**: Url of the controlling web service (DEFAULT: *http://localhost:5000/vizier-db/api/v1*)
- **VIZIERWORKER_LOG_DIR**: Log file directory used by the worker (DEFAULT: *./.vizierdb/logs/worker*)


If the value of the environment variable **VIZIERWORKER_ENV** is *DEV* the environment variable **VIZIERENGINE_DATA_DIR** is used to instantiate the datastore and filestore factory. If the value of **VIZIERWORKER_ENV** is *REMOTE* the variable **VIZIERWORKER_CONTROLLER_URL** is used to instantiate the datastore factory. In a remote environment a dummy filestore factory is used.


Project Container
-----------------

- **VIZIERCONTAINER_PROJECT_ID**: Unique identifier for the project that the container maintains
- **VIZIERCONTAINER_CONTROLLER_URL**: Url for the controlling web service

Ignores **VIZIERENGINE_SYNCHRONOUS**

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
