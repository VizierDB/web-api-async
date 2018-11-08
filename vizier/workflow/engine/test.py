# Copyright (C) 2018 New York University
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

"""Simple implementation of the WorkflowEngine for testing purposes."""

from vizier.serialize import PLAIN_TEXT
from vizier.workflow.engine.base import WorkflowExecutionResult, WorkflowEngine


class TestWorkflowEngine(WorkflowEngine):
    """Simple test engine. Implements the abstract WorkflowEngine class. Does
    not execute any modules but simply sets the STDOUT to 'SUCCESS ,module-id>'.
    """
    def copy_workflow(self, version, modules):
        """Make a copy of the given workflow up until the given module
        (including). If module identifier is negative the complete workflow is
        copied.

        Parameters
        ----------
        version: int
            Unique version identifier for new workflow
        modules: list(vizier.workflow.module.ModuleHandle)
            List of modules for the new workflow

        Returns
        -------
        vizier.workflow.engine.base.WorkflowExecutionResult
        """
        return WorkflowExecutionResult(
            version,
            modules[0].identifier,
            [m.copy() for m in modules]
        )

    def execute_workflow(self, viztrail_id, branch_id, version, modules, modified_index):
        """Execute a sequence of modules that define the next version of a given
        workflow in a viztrail. The list of modules is a modified list compared
        to the module in the given workflow. The modified_index points to the
        position (i.e., module) in the list of modules that contains the (first)
        modified module. All modules before modified_index remain the same as in
        the previous version of the workflow.

        The modified index may be negative. In that case execution starts at the
        first module.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id : string
            Unique branch identifier for existing branch
        version: int
            Unique version identifier for new workflow
        modules: list(vizier.workflow.module.ModuleHandle)
            List of modules for the new workflow versions
        modified_index: int
            Index position of the first modified module in modules

        Returns
        -------
        vizier.workflow.engine.base.WorkflowExecutionResult
        """
        if modified_index == -1:
            start_index = 0
        else:
            start_index = modified_index
        wf_modules = list()
        for i in range(len(modules)):
            m = modules[i].copy()
            if i >= start_index:
                m.stdout.append(PLAIN_TEXT('SUCCESS ' + str(m.identifier)))
            elif  m.identifier < 0:
                raise ValueError('invalid module identifier \'' + str(m.identifier) + '\'')
            wf_modules.append(m)
        # Get the identifier of the modified module. The start_index may point
        # beyond the end of the modul elist if we deleted the last module. In
        # this case the result is -1
        if start_index < len(wf_modules):
            mod_id = wf_modules[start_index].identifier
        else:
            mod_id = -1
        return WorkflowExecutionResult(version, mod_id, wf_modules)
