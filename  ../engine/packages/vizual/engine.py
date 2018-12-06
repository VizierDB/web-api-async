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

"""Task engine for the vizual package."""


from vizier.engine.packages.task import ExecResult, TaskEngine


class VizualTaskEngine(TaskEngine):
    """Implmentation of the task engine for the vizual package. The task engine
    uses an instance of the vizual API to allow running on different types of
    datastores (e.g., the default datastore or the Mimir datastore).
    """
    def __init__(self, api):
        """Initialize the vizual API instance.

        Parameters
        ----------
        api: vizier.engine.packages.vizual.api.base.VizualApi
            Instance of the vizual API
        """
        self.api = api

    def compute(self, command_id, arguments, context):
        """Compute results for the given vizual command using the set of user-
        provided arguments and the current database state. Return an execution
        result is case of success or error.

        Parameters
        ----------
        command_id: string
            Unique identifier for a command in a package declaration
        arguments: vizier.viztrail.command.ModuleArguments
            User-provided command arguments
        context: vizier.engine.packages.task.TaskContext
            Context in which a task is being executed

        Returns
        -------
        vizier.engine.packages.task.ExecResult
        """
        raise NotImplementedError
