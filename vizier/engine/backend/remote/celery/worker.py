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

"""Remote Celery worker to execute workflow commands."""

from celery import Task
from celery.signals import worker_init

from vizier.engine.backend.remote.celery.app import celeryapp
from vizier.config.worker import WorkerConfig
from vizier.core.timestamp import get_current_time
from vizier.engine.backend.base import exec_command
from vizier.engine.backend.remote.celery.env import get_env
from vizier.engine.task.base import TaskContext
from vizier.engine.task.processor import ExecResult
from vizier.viztrail.command import ModuleCommand
from vizier.viztrail.module.output import ModuleOutputs, TextOutput


"""Initialize global objects."""
# The worker environment will only be initialized when the Celery worker is
# started
@worker_init.connect()
def init(signal=None, sender=None, **kwargs):
    """Initialize the global worker environment."""
    config = WorkerConfig()
    global worker_env
    worker_env = get_env(config)


@celeryapp.task
def execute(task_id, project_id, command_doc, context, resources):
    """Execute the givven command.

    Parameters:
    -----------
    task_id: string
        Unique task identifier
    project_id: string
        Unique project identifier
    command_doc : dict
        Dictionary serialization of the module command
    context: dict
        Dictionary of available resources in the database state. The key is
        the resource name. Values are resource identifiers.
    resources: dict
        Optional information about resources that were generated during a
        previous execution of the command
    """
    # Create a remote workflow controller for the given task
    controller = worker_env.get_controller(project_id)
    # Notify the workflow controller that the task started to run
    controller.set_running(task_id=task_id, started_at=get_current_time())
    # Get the processor and execute the command. In case of an unknown package
    # the result is set to error.
    command = ModuleCommand.from_dict(command_doc)
    if command.package_id in worker_env.processors:
        processor = worker_env.processors[command.package_id]
        _, exec_result = exec_command(
            task_id=task_id,
            command=command,
            context=TaskContext(
                datastore=worker_env.datastores.get_datastore(project_id),
                filestore=worker_env.filestores.get_filestore(project_id),
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
            outputs=exec_result.outputs,
            provenance=exec_result.provenance
        )
    else:
        controller.set_error(
            task_id=task_id,
            outputs=exec_result.outputs
        )
