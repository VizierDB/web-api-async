import docker

import vizier.config.app as app
import vizier.config.container as cont


client = docker.from_env()

container_image = 'heikomueller/vizierapi:container'
port = 5005
project_id = '0123456789'
controller_url = 'http://localhost:5000/vizier-db/api/v1'


container = client.containers.run(
    image=container_image,
    environment={
        app.VIZIERSERVER_NAME: 'Project Container API - ' + project_id,
        app.VIZIERSERVER_LOG_DIR: '/app/data/logs/container',
        app.VIZIERENGINE_DATA_DIR: '/app/data',
        app.VIZIERSERVER_PACKAGE_PATH: '/app/resources/packages',
        app.VIZIERSERVER_PROCESSOR_PATH: '/app/resources/processors',
        app.VIZIERSERVER_SERVER_PORT: port,
        app.VIZIERSERVER_SERVER_LOCAL_PORT: port,
        cont.VIZIERCONTAINER_PROJECT_ID: project_id,
        cont.VIZIERCONTAINER_CONTROLLER_URL: controller_url
    },
    network='host',
    detach=True
)

print container
print container.id


"""
  -e VIZIERSERVER_SERVER_PORT=5005 \
  -e VIZIERSERVER_SERVER_LOCAL_PORT=5005 \
  -e VIZIERCONTAINER_PROJECT_ID=60fc09ed \
  -e VIZIERCONTAINER_CONTROLLER_URL=http://localhost:5000/vizier-db/api/v1 \
"""
