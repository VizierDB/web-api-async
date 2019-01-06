# Copyright (C) 2018 New York University,
#                    University at Buffalo,
#                    Illinois Institute of Technology.
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

"""Remote Celery worker to execute workflow commands."""

from celery import Task

from vizier.config.engine.celery import celery_app
from vizier.config.worker import WorkerConfig
from vizier.core.timestamp import get_current_time
from vizier.engine.backend.base import worker
from vizier.engine.task.base import TaskContext
from vizier.engine.task.processor import ExecResult
from vizier.viztrail.module.output import ModuleOutputs, TextOutput


"""Initialize global objects."""
# Read worker configuration information
config = None

@celery_app.task
def execute(task_id, project_id, command, context, resources):
    """
    Parameters:
    -----------
    task_id: string
        Unique task identifier
    project_id: string
        Unique project identifier
    command : vizier.viztrail.command.ModuleCommand
        Specification of the command that is to be executed
    context: dict
        Dictionary of available resources in the database state. The key is
        the resource name. Values are resource identifiers.
    resources: dict
        Optional information about resources that were generated during a
        previous execution of the command
    """
    # Create a remote workflow controller for the given task
    controller = config.get_controller(project_id)
    # Notify the workflow controller that the task started to run
    controller.set_running(task_id=task_id, started_at=get_current_time())
    # Get the processor and execute the command. In case of an unknown package
    # the result is set to error.
    if command.package_id in config.processors:
        processor = config.processors[command.package_id]
        exec_result = worker(
            task_id=task_id,
            command=command,
            context=TaskContext(
                datastore=self.datastores.get_datastore(project_id),
                filestore=self.filestores.get_filestore(project_id),
                datasets=context,
                resources=resources
            ),
            processor=processor
        )
    else:
        message = 'unknown package \'' + str(command.package_id) + '\''
        exec_result = ExecResult(
            is_success=False,
            outputs=ModuleOutputs(stderr=[TextOutput(message)])
        )
    # Notify the workflow controller that the task has finished
    if exec_result.is_success:
        controller.set_success(
            task_id=task_id,
            datasets=exec_result.datasets,
            outputs=exec_result.outputs,
            provenance=exec_result.provenance
        )
    else:
        controller.set_error(
            task_id=task_id,
            outputs=exec_result.outputs
        )


if __name__ == '__main__':
    config = WorkerConfig()
