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

"""Vizier Workflow API - Implements all methods of the API to interact with
workflows in vizier projects.
"""

from vizier.viztrail.command import ModuleCommand

import vizier.api.serialize.module as serialmd
import vizier.api.serialize.project as serialpr
import vizier.api.serialize.workflow as serialwf


class VizierWorkflowApi(object):
    """The Vizier workflow API implements the methods that correspond to
    requests that access and manipulate workflows in vizier projects.
    """
    def __init__(self, engine, urls):
        """Initialize the API components.

        Parameters
        ----------
        engine: vizier.engine.base.VizierEngine
            Instance of the API engine
        urls: vizier.api.routes.base.UrlFactory
            Factory for resource urls
        """
        self.engine = engine
        self.urls = urls

    def append_workflow_module(self, project_id, branch_id, package_id, command_id, arguments):
        """Append a new module to the head of the identified project branch.
        The module command is identified by the package and command identifier.
        Arguments is a list of command arguments.

        Raises ValueError if the command is unknown or the command arguments
        cannot be validated.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier
        package_id: string
            Unique package identifier
        command_id: string
            Unique command identifier
        arguments: list
            List of dictionaries representing the user-provided command
            arguments

        Returns
        -------
        dict()
        """
        # Retrieve the project and branch from the repository to ensure that
        # they exist. Run this part first to ensure that all requested resources
        # exist before validating the command.
        project = self.engine.projects.get_project(project_id)
        if project is None:
            return None
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # Create module command (will ensure that it is a valid command) and
        # append it to the workflow at the branch head. The result is the handle
        # for the modified workflow.
        self.engine.append_workflow_module(
            project_id=project_id,
            branch_id=branch_id,
            command=ModuleCommand(
                package_id=package_id,
                command_id=command_id,
                arguments=arguments,
                packages=self.engine.packages
            )
        )
        return serialwf.WORKFLOW_HANDLE(
            project=project,
            branch=branch,
            workflow=branch.head,
            urls=self.urls
        )

    def cancel_workflow(self, project_id, branch_id):
        """Cancel execution for all running and pending modules in the head
        workflow of a given project branch.

        Returns a handle for the resulting workflow.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier

        Returns
        -------
        dict()
        """
        # Retrieve the project and branch from the repository to ensure that
        # they exist.
        project = self.engine.projects.get_project(project_id)
        if project is None:
            return None
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # Cancel excecution and return the workflow handle
        self.engine.cancel_exec(
            project_id=project_id,
            branch_id=branch_id
        )
        return serialwf.WORKFLOW_HANDLE(
            project=project,
            branch=branch,
            workflow=branch.head,
            urls=self.urls
        )

    def delete_workflow_module(self, project_id, branch_id, module_id):
        """Delete a module in the head workflow of the identified project
        branch.

        Returns the handle for the modified head of the workflow branch. The
        result is None if either of the identified resources is unknown.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier
        module_id: string, optional
            Unique identifier for module

        Returns
        -------
        dict
        """
        # Retrieve the project and branch from the repository to ensure that
        # they exist.
        project = self.engine.projects.get_project(project_id)
        if project is None:
            return None
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # Delete the module
        modules = self.engine.delete_workflow_module(
            project_id=project_id,
            branch_id=branch_id,
            module_id=module_id
        )
        if not modules is None:
            return serialwf.WORKFLOW_HANDLE(
                project=project,
                branch=branch,
                workflow=branch.head,
                urls=self.urls
            )
        return None

    def get_workflow(self, project_id, branch_id, workflow_id=None):
        """Retrieve a workflow from a given project branch. If the workflow
        identifier is omitted, the handle for the head of the branch is
        returned.

        Returns None if the project, branch, or workflow do not exist.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier
        workflow_id: string, optional
            Unique identifier for workflow

        Returns
        -------
        dict
        """
        # Retrieve the project and branch from the repository to ensure that
        # they exist.
        project = self.engine.projects.get_project(project_id)
        if project is None:
            return None
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # If the branch is empty we return a special empty workflow handle
        if branch.head is None:
            return serialwf.EMPTY_WORKFLOW_HANDLE(
                project=project,
                branch=branch,
                urls=self.urls
            )
        else:
            if workflow_id is None:
                workflow = branch.head
            else:
                workflow = branch.get_workflow(workflow_id)
            if not workflow is None:
                return serialwf.WORKFLOW_HANDLE(
                    project=project,
                    branch=branch,
                    workflow=workflow,
                    urls=self.urls
                )
        return None

    def get_workflow_module(self, project_id, branch_id, module_id):
        """Get handle for a module in the head workflow of a given project
        branch. The result is a pair of module handle and list of dataset
        descriptors for the datasets in the module state (if the module is
        not active and not in an error state).

        Returns None if the project, branch, or module do not exist.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier
        module_id: string, optional
            Unique identifier for module

        Returns
        -------
        dict
        """
        # Retrieve the project from the repository to ensure that it exists
        project = self.engine.projects.get_project(project_id)
        if project is None:
            return None
        # Get the specified branch to ensure that it exists
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # If the branch is empty we return a special empty workflow handle
        if not branch.head is None:
            workflow = branch.head
            for module in workflow.modules:
                if module.identifier == module_id:
                    charts = list()
                    if module.is_success:
                        # Compute available charts only if the module is in
                        # a success state.
                        charts = get_module_charts(workflow, module_id)
                    return  serialmd.MODULE_HANDLE(
                        project=project,
                        branch=branch,
                        module=module,
                        urls=self.urls,
                        workflow=workflow,
                        charts=charts,
                        include_self=True
                    )
        return None

    def insert_workflow_module(self, project_id, branch_id, before_module_id, package_id, command_id, arguments):
        """Append a new module to the head of the identified project branch.
        The module command is identified by the package and command identifier.
        Arguments is a list of command arguments.

        Raises ValueError if the command is unknown or the command arguments
        cannot be validated.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier
        package_id: string
            Unique package identifier
        command_id: string
            Unique command identifier
        arguments: list
            List of dictionaries representing the user-provided command
            arguments

        Returns
        -------
        dict()
        """
        # Retrieve the project and branch from the repository to ensure that
        # they exist. Run this part first to ensure that all requested resources
        # exist before validating the command.
        project = self.engine.projects.get_project(project_id)
        if project is None:
            return None
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # Create module command (will ensure that it is a valid command) and
        # insert it into the workflow at the branch head. The result is the list
        # of affected modules.
        modules = self.engine.insert_workflow_module(
            project_id=project_id,
            branch_id=branch_id,
            before_module_id=before_module_id,
            command=ModuleCommand(
                package_id=package_id,
                command_id=command_id,
                arguments=arguments,
                packages=self.engine.packages
            ),
        )
        if not modules is None:
            return serialwf.WORKFLOW_HANDLE(
                project=project,
                branch=branch,
                workflow=branch.head,
                urls=self.urls
            )
        return None

    def replace_workflow_module(self, project_id, branch_id, module_id, package_id, command_id, arguments):
        """Append a new module to the head of the identified project branch.
        The module command is identified by the package and command identifier.
        Arguments is a list of command arguments.

        Raises ValueError if the command is unknown or the command arguments
        cannot be validated.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier
        package_id: string
            Unique package identifier
        command_id: string
            Unique command identifier
        arguments: list
            List of dictionaries representing the user-provided command
            arguments

        Returns
        -------
        dict()
        """
        # Retrieve the project and branch from the repository to ensure that
        # they exist. Run this part first to ensure that all requested resources
        # exist before validating the command.
        project = self.engine.projects.get_project(project_id)
        if project is None:
            return None
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # Create module command (will ensure that it is a valid command) and
        # replace it in the workflow at the branch head. The result is the list
        # of affected modules.
        modules = self.engine.replace_workflow_module(
            project_id=project_id,
            branch_id=branch_id,
            module_id=module_id,
            command=ModuleCommand(
                package_id=package_id,
                command_id=command_id,
                arguments=arguments,
                packages=self.engine.packages
            )
        )
        if not modules is None:
            return serialwf.WORKFLOW_HANDLE(
                project=project,
                branch=branch,
                workflow=branch.head,
                urls=self.urls
            )
        return None


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def get_module_charts(workflow, module_id):
    """Get the list of charts that are available for a given module.

    Parameters
    ----------
    workflow: vizier.viztrail.workflow.WorkflowHandle
        Handle for workflow containing the module
    module_id: string
        Unique module identifier

    Returns
    -------
    list(vizier.view.chart.ChartViewHandle)
    """
    result = list()
    charts = dict()
    for m in workflow.modules:
        if not m.provenance.charts is None:
            for c_handle in m.provenance.charts:
                charts[c_handle.chart_name.lower()] = c_handle
        # Only include charts for modules that have any datasets. Otherwise the
        # result is empty by definition.
        if m.identifier == module_id:
            if not m.datasets is None:
                for c_handle in charts.values():
                    if c_handle.dataset_name in m.datasets:
                        result.append(c_handle)
            break
    return result
