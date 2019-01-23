# Use Docker and the Vizier Web API

There exist several options to run the Vizier Web API using Docker containers. These options are described in Section **Vizier Web Service**. Docker can also be used to containerize individual projects in order to isolate projects from different users from each other. See Section **Project Container** for more details. All examples assume that Docker is installed on the local machine (see the [Docker Get Started Guide](https://docs.docker.com/get-started/) for setup instructions).



## Vizier Web Service

Three Docker images have been created to run the Vizier Web Service application in different configurations.



### Web Server

A default Docker container for the Vizier Web API has been build using the following commands:

```
docker build -f resources/docker/dockerfile/webapi --tag=vizierapiasync .
docker tag vizierapiasync heikomueller/vizierapi:server
docker push heikomueller/vizierapi:server
```

To run the API in default development mode use the following command:

```
docker run -p 5000:5000 heikomueller/vizierapi:server
```

This will run the server in development mode with a MULTIPROCESS backend. In order to run a CELERY backend at least the *Universal Worker* is required. If you want to run the application with a dedicated (and isolated) worker for Python cells the *Python Worker* is required together with the *Universal Worker*



### Universal Worker

The universal worker will start a Celery worker that is capable to execute any of the packages that are currently supported for Vizier workflows. The Docker image has been created using the following commands:

```
docker build -f resources/docker/dockerfile/worker --tag=vizierapiasyncworker .
docker tag vizierapiasyncworker heikomueller/vizierapi:worker
docker push heikomueller/vizierapi:worker
```

To run the Vizier application use the Docker compose file [celery-app-general-worker.yml](https://github.com/VizierDB/web-api-async/tree/master/resources/docker/compose/celery-app-general-worker.yml) shown below:

```
version: '3'
services:
    rabbit:
        hostname: rabbit
        image: rabbitmq:latest
        environment:
            - RABBITMQ_DEFAULT_USER=admin
            - RABBITMQ_DEFAULT_PASS=mypass
        ports:
            - "5673:5672"
    webapi:
        image: heikomueller/vizierapi:server
        environment:
            - CELERY_BROKER_URL=amqp://admin:mypass@rabbit:5672
            - VIZIERENGINE_BACKEND=CELERY
            - VIZIERENGINE_DATA_DIR=/data
        ports:
            - "5000:5000"
        volumes:
          - "/home/user/vizier/vizier-api/.vizierdb:/data"
        depends_on:
            - rabbit
    worker:
        image: heikomueller/vizierapi:worker
        environment:
            - CELERY_BROKER_URL=amqp://admin:mypass@rabbit:5672
            - VIZIERWORKER_CONTROLLER_URL=http://webapi:5000/vizier-db/api/v1
            - VIZIERENGINE_DATA_DIR=/data
        volumes:
          - "/home/user/vizier/vizier-api/.vizierdb:/data"
        depends_on:
            - rabbit
```

Ensure to adjust the path to the data volumes. To run the application use the following command:

```
docker stack deploy -c resources/docker/compose/celery-app-general-worker.yml vizierapp
```


### Python Worker

To run a configuration in which Python commands are executed by dedicated workers that do not require access to the file system of the Docker host a separate Python worker is required. The Python worker has been build using the following commands:

```
docker build -f resources/docker/dockerfile/pyworker --tag=vizierapiasyncpyworker .
docker tag vizierapiasyncpyworker heikomueller/vizierapi:pyworker
docker push heikomueller/vizierapi:pyworker
```

To run the Vizier application use the Docker compose file [celery-app-separate-workers.yml](https://github.com/VizierDB/web-api-async/tree/master/resources/docker/compose/celery-app-separate-workers.yml) shown below:

```
version: '3'
services:
    rabbit:
        hostname: rabbit
        image: rabbitmq:latest
        environment:
            - RABBITMQ_DEFAULT_USER=admin
            - RABBITMQ_DEFAULT_PASS=mypass
        ports:
            - "5673:5672"
    webapi:
        image: heikomueller/vizierapi:server
        environment:
            - CELERY_BROKER_URL=amqp://admin:mypass@rabbit:5672
            - VIZIERENGINE_BACKEND=CELERY
            - VIZIERENGINE_DATA_DIR=/data
            - VIZIERENGINE_CELERY_ROUTES=python.code.pycell
        ports:
            - "5000:5000"
        volumes:
          - "/home/user/vizier/vizier-api/.vizierdb:/data"
        depends_on:
            - rabbit
    worker:
        image: heikomueller/vizierapi:worker
        environment:
            - CELERY_BROKER_URL=amqp://admin:mypass@rabbit:5672
            - VIZIERWORKER_CONTROLLER_URL=http://webapi:5000/vizier-db/api/v1
            - VIZIERENGINE_DATA_DIR=/data
        volumes:
          - "/home/user/vizier/vizier-api/.vizierdb:/data"
        depends_on:
            - rabbit
    pyworker:
        image: heikomueller/vizierapi:pyworker
        environment:
            - CELERY_BROKER_URL=amqp://admin:mypass@rabbit:5672
            - VIZIERWORKER_CONTROLLER_URL=http://webapi:5000/vizier-db/api/v1
            - VIZIERWORKER_ENV=REMOTE
        depends_on:
            - rabbit
```

Ensure to adjust the path to the data volumes. To run the application use the following command:


```
docker stack deploy -c resources/docker/compose/celery-app-separate-workers.yml vizierapp
```

To stop the application use the following command:

```
docker stack rm vizierapp
```


## Project Container

The Vizier Web API supports a configuration in which a separate Docker container is used for each user project to execute data curation workflows. The Docker image that contains the engine for each of the projects has been build using the following commands.

```
docker build -f resources/docker/dockerfile/project_container --tag=vizierapiasynccontainer .
docker tag vizierapiasynccontainer heikomueller/vizierapi:container
docker push heikomueller/vizierapi:container
```

To run the Vizier Web API in the CONTAINER configuration set at least the following two environment variables before starting the server . This will run the the project containers in development configuration.

```
export VIZIERSERVER_ENGINE=CLUSTER
export VIZIERENGINE_BACKEND=CONTAINER
```


To start the Vizier Web API server use the following command:

```
python vizier/api/webservice/server.py
```
