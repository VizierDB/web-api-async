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

"""The environment for a remote celery worker is a wrapper around the components
that are used by the worker to execute tasks.
"""

import os

from vizier.api.client.datastore.factory import DatastoreClientFactory
from vizier.api.routes.task import TaskUrlFactory
from vizier.datastore.fs.factory import FileSystemDatastoreFactory
from vizier.engine.backend.remote.controller import RemoteWorkflowController
from vizier.engine.task.processor import load_processors
from vizier.filestore.factory import DevNullFilestoreFactory
from vizier.filestore.fs.factory import FileSystemFilestoreFactory

import vizier.config.app as app
import vizier.config.base as base


class WorkerEnvironment(object):
    """The worker environment is a wrapper around the task processors for the
    commands that are supported by the worker, the datastore factory, the
    filestore factory, and the base url for the controlling web service.

    The filestore factory is optional. At this point only workers that support
    the vizual package need to have access to filestores.
    """
    def __init__(self, controller_url, processors, datastores, filestores=None):
        """Initialize the components of the worker environment.

        Parameters
        ----------
        controller_url: string
            Base url of controlling web service
        processors: dict(vizier.engine.packages.task.processor.TaskProcessor)
            Dictionary of task processors for supported packages
        datastores: vizier.datastore.factory.DatastoreFactory
            Factory for project-specific filestores
        filestores: vizier.filestore.factory.FilestoreFactory, optional
            Factory for project-specific filestores
        """
        self.controller_url = controller_url
        self.processors = processors
        self.datastores = datastores
        self.filestores = filestores

    def get_controller(self, project_id):
        """Get remote workflow controller for a given project.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        vizier.engine.backend.remote.controller.RemoteWorkflowController
        """
        return RemoteWorkflowController(
            urls=TaskUrlFactory(base_url=self.controller_url)
        )


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def get_env(config):
    """Get teh celery worker environment based on the value of the
    environment identifier.

    If the value of the environment variable VIZIERWORKER_ENV is 'DEV' the
    environment variable VIZIERENGINE_DATA_DIR is used to instantiate the
    datastore and filestore factory. If the value of VIZIERWORKER_ENV is
    'REMOTE' the variable VIZIERWORKER_CONTROLLER_URL is used to instantiate
    the datastore factory. In a remote environment a dummy filestore factory
    is used.

    Parameters
    ----------
    config: vizier.config.worker.WorkerConfig
        Worker configuration object

    Returns
    -------
    vizier.engine.backend.remote.celery.env.WorkerEnvironment
    """
    if config.env.identifier == base.DEV_ENGINE:
        base_dir = base.get_config_value(
            env_variable=app.VIZIERENGINE_DATA_DIR,
            default_values={app.VIZIERENGINE_DATA_DIR: base.ENV_DIRECTORY}
        )
        datastores_dir = os.path.join(base_dir, app.DEFAULT_DATASTORES_DIR)
        filestores_dir = os.path.join(base_dir, app.DEFAULT_FILESTORES_DIR)
        datastore_factory=FileSystemDatastoreFactory(datastores_dir)
        filestore_factory=FileSystemFilestoreFactory(filestores_dir)
    elif config.env.identifier == 'REMOTE':
        datastore_factory = DatastoreClientFactory(base_url=config.controller.url)
        filestore_factory = DevNullFilestoreFactory()
    else:
        raise ValueError('unknown worker environment identifier \'' + config.env.identifier + "\'")
    return WorkerEnvironment(
        controller_url=config.controller.url,
        processors=load_processors(config.env.processor_path),
        datastores=datastore_factory,
        filestores=filestore_factory
    )
