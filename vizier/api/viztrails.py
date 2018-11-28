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

"""Vizier API - Viztrail repository - Implements the methods of the API that
directly interact with the viztrail repository component of the Vizier instance.
"""

class ViztrailRepositoryApi(object):
    """API wrapper around the viztrails repository object that manages viztrails
    for the Vizier instance. Note that viztrails are originally referred to as
    projects in the API.
    """
    def __init__(self, viztrails_repository):
        """Initialize the object that manages viztrails for the Vizier instance.

        Parameters
        ----------
        viztrail_repository : vizier.viztrail.ViztrailRepository
            Repository for viztrails
        """
        self.repository = viztrails_repository

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
        vizier.viztrail.base.ViztrailHandle
        """
        return self.repository.create_viztrail(properties=properties)

    def delete_project(self, project_id):
        """Delete the project with given identifier. Deletes the viztrail that
        represents the project.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        bool
        """
        return self.repository.delete_viztrail(viztrail_id=project_id)

    def get_project(self, project_id):
        """Get the viztrail handle that represents the project with the given
        identifier.

        Returns None if no project with the given identifier exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        vizier.viztrail.base.ViztrailHandle
        """
        return self.viztrails.get_viztrail(viztrail_id=project_id)

    def list_projects(self):
        """Returns a list of viztrail handles for all projects that are
        currently maintained by the viztrail repository.

        Returns
        ------
        list(vizier.viztrail.base.ViztrailHandle)
        """
        return self.viztrails.list_viztrails()

    def update_project_properties(self, project_id, properties):
        """Update the set of user-defined properties for a project with given
        identifier.

        The (key, value)-pairs in the properties dictionary define the update
        operations. Values are expected to be either None, a scalar value (i.e.,
        int, float, or string) or a list of scalar values. If the value is None
        the corresponding project property is deleted. Otherwise, the
        corresponding property will be replaced by the value or the values in a
        given list of values.

        Returns None if no project with given identifier exists. Raises a
        ValueError if the name property of the viztrail is deleted or updated to
        an empty string.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        properties : dict
            Dictionary representing property update statements

        Returns
        -------
        vizier.viztrail.base.ViztrailHandle
        """
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Update properties that are associated with the viztrail. Make sure
        # that a new project name, if given, is not the empty string.
        if 'name' in properties:
            project_name = properties['name']
            if not project_name is None:
                if project_name == '':
                    raise ValueError('not a valid project name')
        viztrail.properties.update_properties(properties)
        # Return handle for modified viztrail
        return viztrail

    # --------------------------------------------------------------------------
    # Workflows
    # --------------------------------------------------------------------------
    def append_module(self, project_id, branch_id, workflow_version, command, before_id=-1, includeDataset=None):
        """Insert module to existing workflow and execute the resulting
        workflow. If before_id is equal or greater than zero the module will be
        inserted at the specified position in the workflow otherwise it is
        appended at the end of the workflow.

        Raise a ValueError if the given command does not specify a valid
        workflow command.

        Returns None if no project, branch, or workflow with given identifiers
        exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique branch identifier
        workflow_version: int
            Version number of the modified workflow
        command : vizier.workflow.module.ModuleCommand
            Specification of the workflow module
        before_id : int, optional
            Insert new module before module with given identifier. Append at end
            of the workflow if negative
        includeDataset: dict, optional
            If included the result will contain the modified dataset rows
            starting at the given offset. Expects a dictionary containing name
            and offset keys.

        Returns
        -------
        dict
            Serialization of the modified workflow handle.
        """
        # Evaluate command against the HEAD of the current work trail branch.
        # The result is None if the viztrail or branch is unknown
        viztrail = self.viztrails.append_workflow_module(
            viztrail_id=project_id,
            branch_id=branch_id,
            workflow_version=workflow_version,
            command=command,
            before_id=before_id
        )
        if viztrail is None:
            return None
        # Get modified workflow to return workflo handle
        branch = viztrail.branches[branch_id]
        workflow = self.viztrails.get_workflow(
            viztrail_id=project_id,
            branch_id=branch_id,
            workflow_version=branch.workflows[-1].version
        )
        return serialize.WORKFLOW_UPDATE_RESULT(
            viztrail,
            workflow,
            config=self.config,
            dataset_cache=self.get_dataset_handle,
            urls=self.urls,
            includeDataset=includeDataset,
            dataset_serializer=self.get_dataset
        )

    def create_branch(self, project_id, branch_id, workflow_version, module_id, properties):
        """Create a new workflow branch for a given project. The version and
        module identifier specify the parent of the new branch. The new branch
        will have it's properties set according to the given dictionary.

        Returns None if the specified project does not exist. Raises ValueError
        if the specified branch or module identifier do not exists.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: int
            Unique branch identifier
        workflow_version: int
            Version number of the modified workflow
        module_id: int
            Module identifier in given workflow version
        properties: dict()
            Properties for new workflow branch

        Returns
        -------
        dict
        """
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Create new branch. The result is None if the project of branch do not
        # exit.
        branch = self.viztrails.create_branch(
            viztrail_id=project_id,
            source_branch=branch_id,
            workflow_version=workflow_version,
            module_id=module_id,
            properties=properties
        )
        if branch is None:
            return None
        return serialize.BRANCH_HANDLE(viztrail, branch, self.urls)

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
        # Delete viztrail branch. The result is None if either the viztrail or
        # the branch does not exist.
        viztrail = self.viztrails.delete_branch(
            viztrail_id=project_id,
            branch_id=branch_id
        )
        return not viztrail is None

    def delete_module(self, project_id, branch_id, workflow_version, module_id):
        """Delete a module in a project workflow branch and execute the
        resulting workflow.

        Returns the modified workflow descriptor on success. The result is None
        if no project, branch, or module with given identifier exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier
        workflow_version: int
            Version number of the modified workflow
        module_id : int
            Module identifier

        Returns
        -------
        dict
        """
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Delete module from the viztrail branch. The result is False if the
        # viztrail branch or module is unknown
        success = self.viztrails.delete_workflow_module(
            viztrail_id=project_id,
            branch_id=branch_id,
            workflow_version=workflow_version,
            module_id=module_id
        )
        if not success:
            return None
        # Return workflow handle on success
        branch = viztrail.branches[branch_id]
        workflow = self.viztrails.get_workflow(
            viztrail_id=project_id,
            branch_id=branch_id,
            workflow_version=branch.workflows[-1].version
        )
        return serialize.WORKFLOW_UPDATE_RESULT(
            viztrail,
            workflow,
            dataset_cache=self.get_dataset_handle,
            config=self.config,
            urls=self.urls
        )

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
        # Get viztrail to ensure that it exist.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Return serialization if branch does exist, otherwise None
        if branch_id in viztrail.branches:
            branch = viztrail.branches[branch_id]
            return serialize.BRANCH_HANDLE(viztrail, branch, self.urls)

    def get_dataset_chart_view(self, project_id, branch_id, version, module_id, view_id):
        """
        """
        # Get viztrail to ensure that it exist.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Retrieve workflow from repository. The result is None if the branch
        # does not exist.
        workflow = self.viztrails.get_workflow(
            viztrail_id=project_id,
            branch_id=branch_id,
            workflow_version=version
        )
        if workflow is None:
            return None
        # Find the workflow module and ensure that the referenced view is
        # defined for the module.
        datasets = None
        v_handle = None
        for module in workflow.modules:
            for obj in module.stdout:
                if obj['type'] == serialize.O_CHARTVIEW:
                    view = ChartViewHandle.from_dict(obj['data'])
                    if view.identifier == view_id:
                        v_handle = view
            if module.identifier == module_id:
                datasets = module.datasets
                break
        if not datasets is None and not v_handle is None:
            if not v_handle.dataset_name in datasets:
                raise ValueError('unknown dataset \'' + v_handle.dataset_name + '\'')
            dataset_id = datasets[v_handle.dataset_name]
            rows = self.datastore.get_dataset_chart(dataset_id, v_handle)
            ref = self.urls.workflow_module_view_url(project_id, branch_id,  version, module_id,  view_id)
            return serialize.DATASET_CHART_VIEW(
                view=v_handle,
                rows=rows,
                self_ref=self.urls.workflow_module_view_url(
                    project_id,
                    branch_id,
                    version,
                    module_id,
                    view_id
                )
            )

    def get_workflow(self, project_id, branch_id, workflow_version=-1):
        """Retrieve a workflow from a given project.

        Returns None if no project, branch, or workflow with given identifiers
        exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier
        workflow_version: int, optional
            Version number of the modified workflow

        Returns
        -------
        dict
            Serialization of the project workflow
        """
        # Get viztrail to ensure that it exist.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Retrieve workflow from repository. The result is None if the branch
        # does not exist.
        workflow = self.viztrails.get_workflow(
            viztrail_id=project_id,
            branch_id=branch_id,
            workflow_version=workflow_version
        )
        if workflow is None:
            return None
        # If an explicit workflow version was requested the workflow will be
        # marked as read only.
        return serialize.WORKFLOW_HANDLE(
            viztrail,
            workflow,
            dataset_cache=self.get_dataset_handle,
            config=self.config,
            urls=self.urls,
            read_only=(workflow_version != -1)
        )

    def get_workflow_modules(self, project_id, branch_id, workflow_version=-1):
        """Get list of module handles for a workflow from a given project.

        Returns None if no project, branch, or workflow with given identifiers
        exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier
        workflow_version: int, optional
            Version number of the modified workflow

        Returns
        -------
        dict
            Serialization of the project workflow modules
        """
        # Get viztrail to ensure that it exist.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Retrieve workflow from repository. The result is None if the branch
        # does not exist.
        workflow = self.viztrails.get_workflow(
            viztrail_id=project_id,
            branch_id=branch_id,
            workflow_version=workflow_version
        )
        if workflow is None:
            return None
        # If an explicit workflow version was requested the workflow will be
        # marked as read only.
        return serialize.WORKFLOW_MODULES(
            viztrail,
            workflow,
            dataset_cache=self.get_dataset_handle,
            config=self.config,
            urls=self.urls,
            read_only=(workflow_version != -1)
        )

    def list_branches(self, project_id):
        """Get a list of all branches for a given project. The result contains a
        list of branch descriptors. The result is None, if the specified project
        does not exist.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        dict
        """
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if not viztrail is None:
            return serialize.BRANCH_LISTING(viztrail, self.urls)

    def replace_module(self, project_id, branch_id, workflow_version, module_id, command, includeDataset=None):
        """Replace a module in a project workflow and execute the result.

        Raise a ValueError if the given command does not specify a valid
        workflow command.

        Returns None if no project with given identifier exists or if the
        specified module identifier is unknown.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier
        workflow_version: int
            Version number of the modified workflow
        module_id : int
            Module identifier
        command : vizier.workflow.module.ModuleCommand
            Specification of the workflow module
        includeDataset: dict, optional
            If included the result will contain the modified dataset rows
            starting at the given offset. Expects a dictionary containing name
            and offset keys.

        Returns
        -------
        dict
            Serialization of the modified workflow handle.
        """
        # Evaluate command against the HEAD of the current work trail branch.
        # The result is None if the viztrail or branch is unknown
        viztrail = self.viztrails.replace_workflow_module(
            viztrail_id=project_id,
            branch_id=branch_id,
            workflow_version=workflow_version,
            module_id=module_id,
            command=command
        )
        if viztrail is None:
            return None
        # Get modified workflow to return workflow handle
        branch = viztrail.branches[branch_id]
        workflow = self.viztrails.get_workflow(
            viztrail_id=project_id,
            branch_id=branch_id,
            workflow_version=branch.workflows[-1].version
        )
        return serialize.WORKFLOW_UPDATE_RESULT(
            viztrail,
            workflow,
            dataset_cache=self.get_dataset_handle,
            config=self.config,
            urls=self.urls,
            includeDataset=includeDataset,
            dataset_serializer= self.get_dataset
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
        # Get the viztrail to ensure that it exists
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Get the specified branch
        if not branch_id in viztrail.branches:
            return None
        # Update properties that are associated with the workflow
        viztrail.branches[branch_id].properties.update_properties(properties)
        return serialize.BRANCH_HANDLE(
            viztrail,
            viztrail.branches[branch_id],
            urls=self.urls
        )
