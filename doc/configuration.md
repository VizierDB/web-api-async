# Configuration

The Vizier web service API and all worker components are configured using environment variables in an attempt to follow [The Twelve-Factor App methodology](https://12factor.net/) for web application development. All relevant environment variables have default values that allow you to run the web service using a default configuration without the need to set any of the variables. The current default configuration will run the service in development mode. 



## Web Service REST-API

The following environment variables control different parameters of the web API server. Additional variables to configure the different execution backends are described below.


### General

- ***VIZIERSERVER_NAME***: Web service name
- ***VIZIERSERVER_LOG_DIR***: Log file directory used by the web server (DEFAULT: *./.vizierdb/logs*)
- ***VIZIERSERVER_DEBUG***: Flag indicating whether server is started in debug mode (DEFAULT: *True*)


### Web Service

- ***VIZIERSERVER_BASE_URL***: Base URL of the server running the web service (DEFAULT: http://localhost)
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


The files in the package directory are used to initialize the registry of supported packages. Each file in the packages directory is expected to contain the declaration of one or more vizier packages. All files are read during service initialization when repository of available workflow packages is created.



## Workflow Engine

The *vizier workflow engine* is used by the web API to create, delete, and manipulate projects and workflows. The engine is a wrapper for the four main system components: (1) the datastore for storing and manipulating datasets, (2) the filestore that manages uploaded files, (3) the viztrails repository for maintaining projects and workflows, and (4) the execution backend for running workflow modules. Different configurations for the engine can use different implementations of datasores, filestores, vitrails repositories, and execution backends. The engine configuration is set using the variable *VIZIERSERVER_ENGINE*. Vizier currently supports three different engine configurations:

- *DEV*: Development engine uses a simple datastore that maintains all datasets as separate files on the file system. 
- *MIMIR*: The Mimir engine uses the Mimir gateway to store and manipulate datasets.
- *CLUSTER*: The cluster engine runs each project in an individual Docker container. In this configuration the engine itself does not contain instances of the datstore and filestore. Instead, each of the containers will have their own datastore, filestore, and execution backend.

At this point there exists only one implementation for the viztrails repository interface (*vizier.viztrails.objectstore*) as wel as for the filestore interface (*vizier.filestore.fs*). Both implementations are therefore used by all three configurations.

The vizier engine is further configured using the following four environment variables:

- ***VIZIERENGINE_BACKEND***: Name of the execution backend. The currently implemented backends are CELERY, MULTIPROCESS, or CONTAINER (DEFAULT: MULTIPROCESS).
- ***VIZIERENGINE_SYNCHRONOUS***: Colon separated list of package.command strings that identify the commands that are executed synchronously (DEFAULT: None)
- ***VIZIERENGINE_USE_SHORT_IDENTIFIER***: Flag indicating whether short identifiers (eight characters instead of 32) are used by the viztrail repository (DEFAULT: True)
- **VIZIERENGINE_DATA_DIR**: Base data directory for storing data. The datastore, filestore, and viztrail repository will create sub-folders in the directory for maintaining information and resources they maintain.

Each execution backend may use additional environment variables for its configuration. **Note** that not all combinations of engine configuration and backend name are valid. The backends *MULTIPROCESS* and *CELERY* can only be used in combination with engine configurations *DEV* and *MIMIR*. Backend *CONTAINER* is the backend when using engine configuration *CLUSTER*.


### MULTIPROCESS Backend

The MULTIPROCESS backend executes workflow modules using separate processes within the web service API. For each module a new process is created. This backend is primarily intended for installations on a local machine with a single user. There are no additional environment variables to configure the backend.


### CELERY Backend

The CELERY backend uses the distributed  task queue [Celery](http://www.celeryproject.org) to execute workflow modules using one or more Celery workers. This is a scalable configuration that is prefered over the MULTIPROCESS backend for installation with multiple users. The CELERY backend has the additional benefit that different modules can be executed by dedicated workers. This allows, for example, to execute Python cells in a notebook using a dedicated (containerized) worker to reduce the risk of malicious users interfering with the system. **Note**, however, that at this point workers are still shared by different users. To ensure complete isolation of user projects the *CLUSTER* configuration should be used.

The CELERY backend has two additional environment variables for its configuration:

- ***VIZIERENGINE_CELERY_ROUTES***: Colon separated list of package.command.queue strings that define routing information for individual commands (DEFAULT: None)
- ***CELERY_BROKER_URL***: URL for the celery broker (DEFAULT: amqp://guest@localhost//)

The default configuration uses [RabbitMQ](https://www.rabbitmq.com) as the message broker. Celery workers are configured using a set of environment variables as described below.


### CONTAINER Backend

When using the CONTAINER backend each vizier projects runs in a separate isolated container. This configuration is intended to prevent users from manipulating (or destroying) the projects of other users. The CONTAINER backend can only be used in combination with *CLUSTER* as the engine configuration. The CONTAINER backend is configured using two additional environment variables:

- ***VIZIERENGINE_CONTAINER_PORTS*** : List of port numbers for new project containers. Expects a comma-separated list of port number of number intervals (e.g. 8080-8088,9000,9090-9099)  (DEFAULT: 20171-20271)
- ***VIZIERENGINE_CONTAINER_IMAGE***: Unique identifier of the docker image for project containers (DEFAULT: *heikomueller/vizierdbprojectcontainer*)

Each project container exposes a limited version of the web service API via a given port on the host machine. Port numbers are drawn from the list of number in *VIZIERENGINE_CONTAINER_PORTS*. It is assumed that all port numbers in the given list are available. Once all numbers are in use no ne projects can be added. Port numbers are released when a project is deleted.



## Worker Configuration

Celery worker configuration is controlled by four environment variables:

- ***VIZIERWORKER_ENV***: Identifier for environment in which the worker operates (supported values are *DEV*, *MIMIR*, and *REMOTE*) (DEFAULT: *DEV*)
- ***VIZIERWORKER_PROCESSOR_PATH***: Path to the task processor definitions for supported packages (DEFAULT: *./resources/processors/dev*)
- ***VIZIERWORKER_LOG_DIR***: Log file directory used by the worker (DEFAULT: *./.vizierdb/logs/worker*)
- ***VIZIERWORKER_CONTROLLER_URL***: URL of the controlling web service (DEFAULT: http://localhost:5000/vizier-db/api/v1)

 In addition, the variables *CELERY_BROKER_URL* and *VIZIERENGINE_DATA_DIR* are also used by the workers.
 
The value of the environment variable *VIZIERWORKER_ENV* should either match the value of *VIZIERSERVER_ENGINE* or be *REMOTE*. The remote case is intended for running dedicated workers that execute Python cells. In a remote environment the worker will use the remote datastore client to read and write datasets. Thus, the worker does not need access to the local file system and can be run in an isolated container. The remote datastore client is initialized using the same URL that is used by the worker controller (set in *VIZIERWORKER_CONTROLLER_URL*).



## Packages and Task Processors

The list of available commands that can be executed as workflow modules (i.e., notebook cells) is defined using the files in the in directories in *VIZIERSERVER_PACKAGE_PATH*. The path is a colon-separated list of local directories. Every file in each of the directories is expected to contain a package declaration. See [Packages in Vizier](https://github.com/VizierDB/web-api/blob/master/doc/workflow-modules.md) for more details on the file format.

For each package a task processor needs to be specified to execute the commands that are defined in the package. Task processors should implement the interface [TaskProcessor](https://github.com/VizierDB/web-api/blob/master/vizier/engine/task.processor.py). Task processors are instantiated from files that are found in the directories in the *VIZIERSERVER_PROCESSOR_PATH* (or *VIZIERWORKER_PROCESSOR_PATH* for Celery workers). The expected file format is is:

```
packages:
    - '...' # List of package identifier
engine:
    className: '...' # Name of the Python class that implements the task processor
    moduleName: '...' # Name of the module that contains the task processor class
    properties: # Optional dictionary of processor-specific parameters
        ...
```

Files can either be serialized as JSON or Yaml.

### Task Processor for Plot Package

The Yaml serialization that instantiates the task processor for plot commands is:

```
packages:
    - plot
engine:
    className: 'PlotProcessor'
    moduleName: 'vizier.engine.packages.plot.processor'
```

The plot task processor does not take any additional arguments.

### Task Processor for VizUAL Package

The task processor for VizUAL commands takes an additional argument *api* that specifies the Python class that implements the VizUAL API that is used by the task processor (depending on whether *DEV* or *MIMIR* is used as the engine configuration).

**VizUAL Task Processor for DEV Engine**

```
packages:
    - vizual
engine:
    className: 'VizualTaskProcessor'
    moduleName: 'vizier.engine.packages.vizual.processor'
    properties:
        api:
            className: 'DefaultVizualApi'
            moduleName: 'vizier.engine.packages.vizual.api.fs'
```

**VizUAL Task Processor for MIMIR Engine**

```
packages:
    - vizual
engine:
    className: 'VizualTaskProcessor'
    moduleName: 'vizier.engine.packages.vizual.processor'
    properties:
        api:
            className: 'MimirVizualApi'
            moduleName: 'vizier.engine.packages.vizual.api.mimir'
```