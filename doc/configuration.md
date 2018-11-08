Web API Configuration
=====================

The Web API is configured using a configuration object that contains (i) the application settings and (ii) the list of available packages.


Packages
--------
Each package is represented by a dictionary that contains the definition of the commands (i.e., modules) that the package supports. Refer to the [Workflow Modules](https://github.com/VizierDB/web-api/blob/master/doc/workflow-modules.md) document for details on the structure of module definitions.

By default, the API only supports the VizUAL and the system packages. All other packages have to be loaded explicitly when the application configuration object is created (see below). Packages are loaded from files that contain serializations of dictionaries with command definitions (either in Yaml or Json format). For an example have alook at the [Mimir Package Definition](https://github.com/VizierDB/web-api/blob/master/config/mimir.pckg.json).


Settings
--------

The application settings are divided into the following parts:

```
api:
    server_url: Url of the server (e.g., http://localhost)
    server_port: Public server port (e.g., 5000)
    server_local_port: Locally bound server port (e.g., 5000)
    app_path: Application path for Web API (e.g., /vizier-db/api/v1)
    doc_url: Url to API documentation
fileserver:
    directory: Path to base directory for file server
    max_file_size: Maximum size for file uploads (in byte)
envs:
    - identifier: Execution environment identifier (i.e., BASIC or MIMIR)
      name: Printable name of execution environment (used by UI)
      description: Descriptive text for execution environment (used by UI)
      default: Flag indicating if this is the default environment
      datastore:
          properties: Data store specific configuration properties
          type: Data store type ['DEFAULT', 'MIMIR']
      packages: [list of identifier for supported packages]
viztrails:
    directory: Base directory for storing viztrail information and meta-data
defaults:
    row_limit: Default row limit for requests that read datasets
    max_row_limit: Maximum row limit for requests that read datasets (-1 for all)
name: Web Service name
debug: Flag indicating whether server is started in debug mode
logs:
    server: Log file for Web Server
    engine: Flag to toggle loggin for workflow engine telemetry
```

The list of elements in the packages list for each execution environment has to correspond to identifier of default packages or packages that have been specified in the packages part of the configuration.


Initialization
--------------

The configuration is read from a file that is expected to be in Yaml format (unless the file name ends with .json in which case Json format is assumed). The file is expected to contain two elements: packages and settings.

packages: List of file names that point to package definition files. All files are expected to be in Yaml format unless the file name ends with .json.

settings: Application settings object. The structure of this object is expected to be the same as the structure shown above.

The configuration file can either be specified using the environment variable **VIZIERSERVER_CONFIG** or be located (as file **config.yaml**) in the current working directory.
