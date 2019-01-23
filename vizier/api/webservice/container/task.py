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

"""Vizier Container API (Tasks) - Implements all methods of the container API
to run and cancel tasks.
"""

from vizier.api.routes.task import TaskUrlFactory
from vizier.engine.backend.remote.controller import RemoteWorkflowController
from vizier.engine.task.base import TaskHandle

import vizier.api.serialize.labels as labels


class VizierContainerTaskApi(object):
    """The Vizier task API implements the methods that start and cancel tasks
    for the container project.
    """
    def __init__(self, engine, controller_url):
        """Initialize the API components.

        Parameters
        ----------
        engine: vizier.engine.base.VizierEngine
            Instance of the API engine
        controller_url: string
            Url of the controlling web service
        """
        self.engine = engine
        self.controller_url = controller_url

    def cancel_task(self, task_id):
        """Cancel execution of the task with the given identifier.

        Parameters
        ----------
        task_id: string
            Unique task identifier

        Returns
        -------
        dict
        """
        self.engine.backend.cancel_task(task_id)
        return {labels.RESULT: True}

    def execute_task(self, project_id, task_id, command, context, resources=None):
        """Execute a given command asynchronously using the engies backend.

        Make use to use the remote web service as controller that orchestrates
        workflow execution.

        Parameters
        ----------
        project_id: string
            Unique project identifier
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

        Returns
        ------
        dict
        """
        self.engine.backend.execute_async(
            task=TaskHandle(
                project_id=project_id,
                task_id=task_id,
                controller=RemoteWorkflowController(
                    urls=TaskUrlFactory(base_url=self.controller_url)
                )
            ),
            command=command,
            context=context,
            resources=resources
        )
        return {labels.RESULT: True}
