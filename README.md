# Vizier Asynchronous Web API

This repository contains the code for the Vizier Web Server API that supports asynchronous execution of workflow modules. The Web API is the backend that manages Viztrails and provides the API that is used by the [Vizier Web UI](https://github.com/VizierDB/web-ui).


## Install and Run

The following steps describe how to obtain a local installation of the Web API on your local machine. Installation requires [Anaconda](https://conda.io/docs/user-guide/install/index.html). If you want to use Mimir modules within your curation workflows a local installation of Mimir is required. Refer to this [guide for Mimir installation details](https://github.com/VizierDB/Vistrails/tree/MimirPackage/vistrails/packages/mimir).

The also exist the option to run the Vizier Web API using Docker. Refer to the [Use Docker and the Vizier Web API](https://github.com/VizierDB/web-api-async/tree/master/doc/docker.md) document for details on how to run Vizier using Docker.


### Python Environment

To setup the Python environment clone the repository and run the following commands:

```
git clone https://github.com/VizierDB/web-api-async.git
cd web-api-async
conda create --name vizierasync pip
source activate vizierasync
pip install -r requirements.txt
pip install -e .
```


### Configuration

The web server is configured using environment variables following [The Twelve-Factor App methodology](https://12factor.net/) for web application development. Please refer to the [Vizier Configuration](https://github.com/VizierDB/web-api-async/tree/master/doc/configuration.md) document for a detailed description of the configuration options. 



### Run Server

After adjusting the server configuration the server is started using the following command:

```
python vizier/api/webservice/server.py
```

If you do not set any of the environment variables described in the [Vizier Configuration](https://github.com/VizierDB/web-api-async/tree/master/doc/configuration.md) document the above command will start the web server in development mode using the MULTIPROCESS backend. This configuration does not support any of the Mimir Lenses or the SQL package. In order to have these command available it is required to set at least the *VIZIERSERVER_ENGINE* environment variable

```
export VIZIERSERVER_ENGINE=MIMIR
```

If using Mimir the gateway server should be started before running the web server.



## Command Line Interface

The command line interface is currently the only option to interact with the Web API. Refer to the [Vizier API Command Line Interface](https://github.com/VizierDB/web-api-async/tree/master/doc/cli.md) document for details on how to run command line interface.