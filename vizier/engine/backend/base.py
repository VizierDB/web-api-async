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

"""Interface for the vizier backend that executes workflow modules. Different
implementations for the backend are possible.
"""

from abc import abstractmethod


class VizierBackend(object):
    """The backend interface defines two main methods: execute and cancel. The
    first method is used by the API to request execution of a command that
    defines a module in a data curation workflow. The second method is used
    by the API to cancel execution (on user request).

    """
    @abstractmethod
    def cancel_task(self, task):
        """Request to cancel execution of the given task.

        Parameters
        ----------
        task: vizier.engine.packages.processor.TaskHandle
            Handle for task for which execution is requested to be canceled
        """
        raise NotImplementedError

    @abstractmethod
    def execute_task(self, api, task, command, context):
        """Request execution of a given task. The task handle is used to
        identify the task when interacting with the API. The executed task
        itself is defined by the given command specification. The given context
        contains the names and identifier of resources in the database state
        against which the task is executed.

        Parameters
        ----------
        api: vizier.api.engine.WorkflowEngineApi
            Reference to the engine API instance to modify task states
        task: vizier.engine.packages.processor.TaskHandle
            Handle for task for which execution is requested to be canceled
        command : vizier.viztrail.command.ModuleCommand
            Specification of the command that is to be executed
        context: dict
            Dictionary of available resource in the database state. The key is
            the resource name. Values are resource identifiers.
        """
        raise NotImplementedError
