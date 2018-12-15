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

"""Vizier API - Implements all methods of the API to interact with a running
Vizier instance.

The API orchestrates the interplay
between different components such as the viztrail repository that manages
viztrails and the workflow engine that executes modules in viztrail workflow
versions.

Internally the API is further divided into four parts that deal with the file
store, data store, viztrail repository and the workflow execution engine.
"""

from vizier.api.webservice.routes import UrlFactory
from vizier.core import VERSION_INFO
from vizier.core.timestamp import get_current_time, to_datetime
from vizier.viztrail.base import PROPERTY_NAME
from vizier.viztrail.branch import BranchProvenance
from vizier.viztrail.command import ModuleCommand

import vizier.api.serialize.base as serialize
import vizier.api.serialize.branch as serialbr
import vizier.api.serialize.deserialize as deserialize
import vizier.api.serialize.files as serialfs
import vizier.api.serialize.labels as labels
import vizier.api.serialize.module as serialmd
import vizier.api.serialize.project as serialpr
import vizier.api.serialize.workflow as serialwf
import vizier.viztrail.module.base as states


class VizierApi(object):
    """The Vizier API implements the methods that correspond to requests that
    are supported by the Vizier Web Service. the API, however, can also be used
    in a stand-alone manner, e.g., via the command line interpreter tool.

    This class is a wrapper around the different components of the Vizier system
    that are necessary for the Web Service, i.e., file store, data store,
    viztrail repository, and workflow execution engine.
    """
    def __init__(self, config):
        """Initialize the API components.

        Parameters
        ----------
        filestore: vizier.filestore.base.Filestore
            Backend store for uploaded files
        viztrails_repository: vizier.viztrail.repository.ViztrailRepository
            Viztrail manager for the API instance
        """
        self.engine = config.get_api_engine()
        self.urls = UrlFactory(base_url=config.app_base_url, api_doc_url=config.webservice.doc_url)
        self.service_descriptor = {
            'name': config.webservice.name,
            'version': VERSION_INFO,
            'startedAt': get_current_time().isoformat(),
            'defaults': {
                'maxFileSize': config.webservice.defaults.max_file_size
            },
            'environment': {
                'name': self.engine.name,
                'version': self.engine.version
            },
            'links': serialize.HATEOAS({
                'self': self.urls.service_descriptor(),
                'doc': self.urls.api_doc(),
                'project:create': self.urls.create_project(),
                'project:list': self.urls.list_projects()
            })
        }

    def init(self):
        """Initialize the API before the first request."""
        self.engine.init()

    # --------------------------------------------------------------------------
    # Service
    # --------------------------------------------------------------------------
    def service_overview(self):
        """Returns a dictionary containing essential information about the web
        service including HATEOAS links to access resources and interact with
        the service.

        Returns
        -------
        dict
        """
        return self.service_descriptor

    # --------------------------------------------------------------------------
    # Projects
    # --------------------------------------------------------------------------
    def create_project(self, properties):
        """Create a new project. All the information about a project is
        currently stored as part of the viztrail.

        Parameters
        ----------
        properties : dict
            Dictionary of user-defined project properties

        Returns
        -------
        dict
        """
        # Create a new project and return the serialized descriptor
        project = self.engine.create_project(properties=properties)
        return serialpr.PROJECT_DESCRIPTOR(project=project, urls=self.urls)

    def delete_project(self, project_id):
        """Delete the project with given identifier. Deletes all resources that
        are associated with the project.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        bool
            True, if project existed and False otherwise
        """
        # Delete entry in repository. The result indicates whether the project
        # existed or not.
        return self.engine.delete_project(project_id)

    def get_project(self, project_id):
        """Get comprehensive information for the project with the given
        identifier.

        Returns None if no project with the given identifier exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        dict
        """
        # Retrieve the project from the repository to ensure that it exists
        project = self.engine.get_project(project_id)
        if project is None:
            return None
        # Get serialization for project handle.
        return serialpr.PROJECT_HANDLE(
            project=project,
            packages=self.engine.packages,
            urls=self.urls
        )

    def list_projects(self):
        """Returns a list of descriptors for all projects that are currently
        contained in the project repository.

        Returns
        ------
        dict
        """
        return {
            'projects': [
                serialpr.PROJECT_DESCRIPTOR(project=project, urls=self.urls)
                    for project in self.engine.list_projects()
            ],
            'links': serialize.HATEOAS({
                'self': self.urls.list_projects(),
                'project:create': self.urls.create_project()
            })
        }

    def update_project(self, project_id, properties):
        """Update the set of user-defined properties for a project with given
        identifier.

        Returns None if no project with given identifier exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        properties : dict
            Dictionary representing property update statements

        Returns
        -------
        dict
            Serialization of the project handle
        """
        # Retrieve the project from the repository to ensure that it exists
        project = self.engine.get_project(project_id)
        if project is None:
            return None
        # Update properties that are associated with the viztrail. Make sure
        # that a new project name, if given, is not the empty string.
        validate_name(properties, message='not a valid project name')
        project.viztrail.properties.update(properties)
        # Return serialization for project handle.
        return serialpr.PROJECT_DESCRIPTOR(project=project, urls=self.urls)

    # --------------------------------------------------------------------------
    # Branches
    # --------------------------------------------------------------------------
    def create_branch(
        self, project_id, branch_id=None, workflow_id=None, module_id=None,
        properties=None
    ):
        """Create a new branch for a given project. The branch point is
        specified by the branch_id, workflow_id, and module_id parameters. If
        all values are None an empty branch is created.

        The properties for the new branch are set from the given properties
        dictionary.

        Returns None if the specified project does not exist. Raises ValueError
        if the specified branch point does not exists.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string, optional
            Unique branch identifier
        workflow_id: string, optional
            Unique workflow identifier
        module_id: string, optional
            Unique module identifier
        properties: dict(), optional
            Properties for new workflow branch

        Returns
        -------
        dict
        """
        # Retrieve the project from the repository to ensure that it exists
        project = self.engine.get_project(project_id)
        if project is None:
            return None
        if branch_id is None and workflow_id is None and module_id is None:
            # Create an empty branch if the branch point is not specified
            branch = project.viztrail.create_branch(properties=properties)
        else:
            # Ensure that the branch point exist and get the index position of
            # the source module
            source_branch = project.viztrail.get_branch(branch_id)
            if source_branch is None:
                raise ValueError('unknown source branch \'' + str(branch_id) + '\'')
            workflow = source_branch.get_workflow(workflow_id)
            if workflow is None:
                    raise ValueError('unknown workflow \'' + str(workflow_id) + '\'')
            module_index = -1
            for i in range(len(workflow.modules)):
                module = workflow.modules[i]
                if module.identifier == module_id:
                    module_index = i
                    break
            if module_index == -1:
                raise ValueError('unknown module \'' + str(module_id) + '\'')
            modules = [m.identifier for m in workflow.modules[:module_index+1]]
            # Create a new branch that contains all source modules including
            # the specified one.
            branch = project.viztrail.create_branch(
                provenance=BranchProvenance(
                    source_branch=source_branch.identifier,
                    workflow_id=workflow_id,
                    module_id=module_id
                ),
                properties=properties,
                modules=modules
            )
        return serialbr.BRANCH_DESCRIPTOR(
            branch=branch,
            project=project,
            urls=self.urls
        )

    def delete_branch(self, project_id, branch_id):
        """Delete the branch with the given identifier from the given
        project.

        Returns True if the branch existed and False if the project or branch
        are unknown.

        Raises ValueError if an attempt is made to delete the default branch.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: int
            Unique branch identifier

        Returns
        -------
        bool
        """
        # Retrieve the project from the repository to ensure that it exists
        project = self.engine.get_project(project_id)
        if project is None:
            return False
        # Delete viztrail branch. The result indicates if the branch existed
        # or not.
        return project.viztrail.delete_branch(branch_id=branch_id)

    def get_branch(self, project_id, branch_id):
        """Retrieve a branch from a given project.

        Returns None if the project or the branch do not exist.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique branch identifier

        Returns
        -------
        dict
            Serialization of the project workflow
        """
        # Retrieve the project and branch from the repository to ensure that
        # they exist.
        project = self.engine.get_project(project_id)
        if project is None:
            return None
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # Return serialization of the branch handle
        return serialbr.BRANCH_HANDLE(
            project=project,
            branch=branch,
            urls=self.urls
        )

    def update_branch(self, project_id, branch_id, properties):
        """Update properties for a given project workflow branch. Returns the
        handle for the modified workflow or None if the project or branch do not
        exist.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier
        properties: dict()
            Properties that are being updated. A None value for a property
            indicates that the property is to be deleted.

        Returns
        -------
        dict
        """
        # Retrieve the project and branch from the repository to ensure that
        # they exist.
        project = self.engine.get_project(project_id)
        if project is None:
            return None
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # Update branch properties and return branch descriptor. Ensure that
        # the branch name is not set to an invalid value.
        validate_name(properties, message='not a valid branch name')
        branch.properties.update(properties)
        return serialbr.BRANCH_DESCRIPTOR(
            branch=branch,
            project=project,
            urls=self.urls
        )

    # --------------------------------------------------------------------------
    # Workflows
    # --------------------------------------------------------------------------
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
        arguments: list()
            List of dictionaries representing the user-provided command
            arguments

        Returns
        -------
        dict()
        """
        # Retrieve the project and branch from the repository to ensure that
        # they exist. Run this part first to ensure that all requested resources
        # exist before validating the command.
        project = self.engine.get_project(project_id)
        if project is None:
            return None
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # Create module command (will ensure that it is a valid command) and
        # append it to the workflow at the branch head. The result is the handle
        # for the appended module.
        module = project.append_workflow_module(
            branch_id=branch_id,
            command=ModuleCommand(
                package_id=package_id,
                command_id=command_id,
                arguments=arguments,
                packages=self.engine.packages
            )
        )
        return serialmd.MODULE_HANDLE(
            project=project,
            branch=branch,
            module=module,
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
        project = self.engine.get_project(project_id)
        if project is None:
            return None
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # Cancel excecution and return the workflow handle
        return serialwf.WORKFLOW_HANDLE(
            project=project,
            branch=branch,
            workflow=project.cancel_exec(branch_id),
            urls=self.urls
        )

    def delete_workflow_module(self, project_id, branch_id, module_id):
        """Delete a module in the head workflow of the identified project
        branch.

        Returns a list of affected modules or None if either of the identified
        resources is unknown.

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
        project = self.engine.get_project(project_id)
        if project is None:
            return None
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # Delete the module
        modules = project.delete_workflow_module(
            branch_id=branch_id,
            module_id=module_id
        )
        if not modules is None:
            return serialmd.MODULE_HANDLE_LIST(
                project=project,
                branch=branch,
                modules=modules,
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
        project = self.engine.get_project(project_id)
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
        branch.

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
        project = self.engine.get_project(project_id)
        if project is None:
            return None
        # Get the specified branch to ensure that it exists
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # If the branch is empty we return a special empty workflow handle
        if not branch.head is None:
            for module in branch.head.modules:
                if module.identifier == module_id:
                    return serialmd.MODULE_HANDLE(
                        project=project,
                        branch=branch,
                        module=module,
                        urls=self.urls
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
        arguments: list()
            List of dictionaries representing the user-provided command
            arguments

        Returns
        -------
        dict()
        """
        # Retrieve the project and branch from the repository to ensure that
        # they exist. Run this part first to ensure that all requested resources
        # exist before validating the command.
        project = self.engine.get_project(project_id)
        if project is None:
            return None
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # Create module command (will ensure that it is a valid command) and
        # insert it into the workflow at the branch head. The result is the list
        # of affected modules.
        modules = project.insert_workflow_module(
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
            return serialmd.MODULE_HANDLE_LIST(
                project=project,
                branch=branch,
                modules=modules,
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
        arguments: list()
            List of dictionaries representing the user-provided command
            arguments

        Returns
        -------
        dict()
        """
        # Retrieve the project and branch from the repository to ensure that
        # they exist. Run this part first to ensure that all requested resources
        # exist before validating the command.
        project = self.engine.get_project(project_id)
        if project is None:
            return None
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # Create module command (will ensure that it is a valid command) and
        # replace it in the workflow at the branch head. The result is the list
        # of affected modules.
        modules = project.replace_workflow_module(
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
            return serialmd.MODULE_HANDLE_LIST(
                project=project,
                branch=branch,
                modules=modules,
                urls=self.urls
            )
        return None

    # --------------------------------------------------------------------------
    # Tasks
    # --------------------------------------------------------------------------
    def update_task_state(self, project_id, task_id, state, body):
        """Update that state pf a given task. The contents of the request body
        depend on the value of the new task state.

        Raises a ValueError if the request body is invalid. The result is None
        if the project or task are unknown. Otherwise, the result is a
        dictionary with a single result value. The result is 0 if the task state
        did not change. A positive value signals a successful task state change.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        task_id: string
            Unique task identifier
        state: int
            The new state of the task
        body: dict()
            State-dependent additional information

        Returns
        -------
        dict
        """
        # Retrieve the project from the repository to ensure thatit exists.
        project = self.engine.get_project(project_id)
        if project is None:
            return None
        # Depending on the requested state change call the respective method
        # after extracting additional parameters from the request body.
        result = None
        if state == states.MODULE_RUNNING:
            if labels.STARTED_AT in body:
                result = project.set_running(
                    task_id=task_id,
                    started_at=to_datetime(body[labels.STARTED_AT])
                )
            else:
                result = project.set_running(task_id=task_id)
        elif state == states.MODULE_ERROR:
            finished_at = None
            if labels.FINISHED_AT in body:
                finished_at = to_datetime(body[labels.FINISHED_AT])
            outputs = None
            if labels.OUTPUTS in body:
                outputs = deserialize.OUTPUTS(body[labels.OUTPUTS])
            result = project.set_error(
                task_id=task_id,
                finished_at=finished_at,
                outputs=outputs
            )
        elif state == states.MODULE_SUCCESS:
            finished_at = None
            if labels.FINISHED_AT in body:
                finished_at = to_datetime(body[labels.FINISHED_AT])
            outputs = None
            if labels.OUTPUTS in body:
                outputs = deserialize.OUTPUTS(body[labels.OUTPUTS])
            datasets = None
            if labels.DATASETS in body:
                datasets = deserialize.DATASETS(body[labels.DATASETS])
            provenance = None
            if labels.PROVENANCE in body:
                provenance = deserialize.PROVENANCE(body[labels.PROVENANCE])
            result = project.set_success(
                task_id=task_id,
                finished_at=finished_at,
                datasets=datasets,
                outputs=outputs,
                provenance=provenance
            )
        else:
            raise ValueError('invalid state change')
        # Create state change result
        if not result is None:
            return {serialize.RESULT: len(result)}
        return None

    # --------------------------------------------------------------------------
    # Files
    # --------------------------------------------------------------------------
    def get_file(self, project_id, file_id):
        """Get the handle for given file. The result is None if the project or
        file are unknown.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        file_id: string
            Unique file identifier

        Returns
        -------
        vizier.filestore.base.FileHandle
        """
        # Retrieve the project from the repository to ensure that it exists
        project = self.engine.get_project(project_id)
        if project is None:
            return None
        return project.filestore.get_file(file_id)

    def upload_file(self, project_id, file, file_name):
        """Upload file to the filestore that is associated with the given
        project. The file is uploaded from a given file stream.

        Returns serialization of the handle for the created file or None if the
        project is unknown.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        file: werkzeug.datastructures.FileStorage
            File object (e.g., uploaded via HTTP request)
        file_name: string
            Name of the file

        Returns
        -------
        vizier.filestore.base.FileHandle
        """
        # Retrieve the project from the repository to ensure that it exists
        project = self.engine.get_project(project_id)
        if project is None:
            return None
        # Upload file to projects filestore and return a serialization of the
        # returned file handle
        f_handle = project.filestore.upload_stream(
            file=file,
            file_name=file_name
        )
        return serialfs.FILE_HANDLE(
            f_handle=f_handle,
            project=project,
            urls=self.urls
        )


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def validate_name(properties, message='not a valid name'):
    """Ensure that a name property (if given) is not None or the empty string.
    Will raise a ValueError in case of an invalid name.

    Parameters
    ----------
    properties: dict()
        Dictionary of resource properties
    message: string, optional
        Error message if exception is thrown
    """
    if PROPERTY_NAME in properties:
        project_name = properties[PROPERTY_NAME]
        if not project_name is None:
            if project_name.strip() == '':
                raise ValueError(message)
