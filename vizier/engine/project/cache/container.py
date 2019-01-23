# Copyright (C) 2017-2019 New York University,
#                         University at Buffalo,
#                         Illinois Institute of Technology.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Project cache for backends that run individual projects in their own
container.
"""

import docker
import requests

from vizier.api.routes.container import ContainerApiUrlFactory
from vizier.core.io.base import DefaultObjectStore
from vizier.engine.project.base import ProjectHandle
from vizier.engine.project.cache.base import ProjectCache

import vizier.api.serialize.labels as labels
import vizier.config.app as app
import vizier.config.container as contnr


class ContainerProjectHandle(ProjectHandle):
    """Extend the default project handle with a reference to the base url of
    the project container API.
    """
    def __init__(self, viztrail, container_api, port, container_id):
        """Initialize the project viztrail and the container API url.

        Parameters
        ----------
        viztrail: vizier.viztrail.base.ViztrailHandle
            The viztrail handle for the project
        container_api: string
            Base url for the project container API
        port: int
            Local port of the container API
        container_id: string
            Unique container identifier
        """
        super(ContainerProjectHandle, self).__init__(viztrail=viztrail)
        self.container_api = container_api
        self.port = port
        self.container_id = container_id
        self.urls = ContainerApiUrlFactory(base_url=self.container_api)

    def cancel_task(self, task_id):
        """Cancel exection of tasks for the notebook.

        """
        url = self.urls.cancel_exec(task_id)
        r = requests.post(url)
        r.raise_for_status()

    def execute_task(self, task_id, command, context, resources=None):
        """Send request to project container API to execute a given task.

        Parameters
        ----------
        task_id: string
            Unique task identifier
        command : vizier.viztrail.command.ModuleCommand
            Specification of the command that is to be executed
        context: dict
            Dictionary of available resource in the database state. The key is
            the resource name. Values are resource identifiers.
        resources: dict, optional
            Optional information about resources that were generated during a
            previous execution of the command
        """
        url = self.urls.execute_task()
        data = {
            labels.ID: task_id,
            labels.COMMAND: {
                labels.COMMAND_PACKAGE: command.package_id,
                labels.COMMAND_ID: command.command_id,
                labels.COMMAND_ARGS: command.arguments.to_list()
            },
            labels.CONTEXT: [{
                labels.ID: context[name],
                labels.NAME: name
            } for name in context]
        }
        if not resources is None:
            data[label.RESOURCES] = resources
        # Send request. Raise exception if status code indicates that the
        # request was not successful
        r = requests.post(url, json=data)
        r.raise_for_status()


class ContainerProjectCache(ProjectCache):
    """The project cache for containerized projects. It is assumed that each
    project runs in a separate container on the local machine that exposes the
    container API via a local port. Maintains a mapping of project identifier
    to information about local container in a separate file.
    """
    def __init__(self, viztrails, container_file, config):
        """Initialize the cache components and load all projects in the given
        viztrails repository. Maintains all projects in an dictionary keyed by
        their identifier.

        Parameters
        ----------
        viztrails: vizier.vizual.repository.ViztrailRepository
            Repository for viztrails
        container_file: string
            Path to the container information file
        config: vizier.config.app.AppConfig
            Application object
        """
        self.viztrails = viztrails
        self.container_file = container_file
        self.config = config
        self.container_image = config.engine.backend.container.image
        # Keep track of the port numbers for the project containers.
        self.ports = config.engine.backend.container.ports
        # Instantiate the Docker daemon client using the default socket or
        # configuration in the environment. This may need to be adjusted for
        # production deployments.
        self.client = docker.from_env()
        # Read mapping of project identifier to container information
        self.store = DefaultObjectStore()
        containers = dict()
        if self.store.exists(self.container_file):
            for obj in self.store.read_object(self.container_file):
                containers[obj['projectId']] = obj
        # Create index of project handles from existing viztrails. The project
        # handles do not have a reference to the datastore or filestore.
        self.projects = dict()
        for viztrail in self.viztrails.list_viztrails():
            container = containers[viztrail.identifier]
            project = ContainerProjectHandle(
                viztrail=viztrail,
                container_api=container['url'],
                port=container['port'],
                container_id=container['containerId']
            )
            self.projects[viztrail.identifier] = project

    def create_project(self, properties=None):
        """Create a new project. Will (i) create a viztrail in the underlying
        viztrail repository, and (ii) start a docker container for the project.

        Parameters
        ----------
        properties: dict, optional
            Set of properties for the new viztrail

        Returns
        -------
        vizier.engine.project.base.ProjectHandle
        """
        # Create the viztrail for the project
        viztrail = self.viztrails.create_viztrail(properties=properties)
        # Start a new docker container for the project on the next unused
        # port. Raises ValueError if all given port numbers are currently used.
        used_ports = [p.port for p in self.projects.values()]
        port = None
        for port_nr in self.ports:
            if not port_nr in used_ports:
                port = port_nr
                break
        if port is None:
            raise ValueError('no port number available')
        project_id = viztrail.identifier
        container = self.client.containers.run(
            image=self.container_image,
            environment={
                app.VIZIERSERVER_NAME: 'Project Container API - ' + project_id,
                app.VIZIERSERVER_BASE_URL: self.config.webservice.server_url,
                app.VIZIERSERVER_SERVER_PORT: port,
                app.VIZIERSERVER_SERVER_LOCAL_PORT: port,
                app.VIZIERSERVER_APP_PATH: self.config.webservice.app_path,
                app.VIZIERSERVER_LOG_DIR: '/app/data/logs/container',
                app.VIZIERENGINE_DATA_DIR: '/app/data',
                app.VIZIERSERVER_PACKAGE_PATH: '/app/resources/packages/common',
                app.VIZIERSERVER_PROCESSOR_PATH: '/app/resources/processors/common:/app/resources/processors/dev',
                contnr.VIZIERCONTAINER_PROJECT_ID: project_id,
                contnr.VIZIERCONTAINER_CONTROLLER_URL: self.config.app_base_url
            },
            network='host',
            detach=True
        )
        project = ContainerProjectHandle(
            viztrail=viztrail,
            container_api=self.config.get_url(port),
            port=port,
            container_id=container.id
        )
        self.projects[project.identifier] = project
        self.write_container_info()
        return project

    def delete_project(self, project_id):
        """Delete all resources that are associated with the given project.
        Returns True if the project existed and False otherwise.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        bool
        """
        if project_id in self.projects:
            project = self.projects[project_id]
            # Delete the viztrail for the project
            viztrail = project.viztrail
            # Stop and remove the associated container
            self.viztrails.delete_viztrail(viztrail.identifier)
            container = self.client.containers.get(project.container_id)
            container.stop()
            container.remove()
            # Remove project from internal cache and update the materialized
            # mapping of projects to containers
            del self.projects[project_id]
            self.write_container_info()
            return True
        return False

    def get_branch(self, project_id, branch_id):
        """Get the branch with the given identifier for the specified project.
        The result is None if the project of branch does not exist.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier

        Returns
        -------
        vizier.viztrail.branch.BranchHandle
        """
        # If the project is not in the internal cache it does not exist
        if not project_id in self.projects:
            return None
        # Return the handle for the specified branch
        return self.projects[project_id].viztrail.get_branch(branch_id)

    def get_project(self, project_id):
        """Get the handle for project. Returns None if the project does not
        exist.

        Returns
        -------
        vizier.engine.project.base.ProjectHandle
        """
        if project_id in self.projects:
            return self.projects[project_id]
        return None

    def list_projects(self):
        """Get a list of handles for all projects.

        Returns
        -------
        list(vizier.engine.project.base.ProjectHandle)
        """
        return self.projects.values()

    def write_container_info(self):
        """Write the current mapping of project identifier to project containers
        to the object store container file.
        """
        self.store.write_object(
            content=[{
                'projectId': p.identifier,
                'containerId': p.container_id,
                'port': p.port,
                'url': p.container_api
            } for p in self.projects.values()],
            object_path=self.container_file
        )
