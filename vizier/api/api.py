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

"""Vizier DB Web API - Implements the methods of the Web API that correspond
to valid Http requests for the Vizier Web Service.

The separate API module separates the implementation from the specifics of the
Web server (e.g., Flask or Tornado). The module orchestrates the interplay
between different components such as the notbook repository that manages
noretbook metadata and the VizTrails module.
"""

class VizierApi(object):
    """The Web Service API implements the methods that correspond to the Http
    requests that are handled by the Web server.

    This class is a wrapper around the different components of the Vizier system
    that are necessary for the Web Service (i.e., viztrail repository, data
    store, and file server).
    """
    def __init__(self, filestore):
        """Initialize the API from a dictionary. Expects the following keys to
        be present in the dictionary:
        - APP_NAME : Application (short) name for the service description
        - API_DOC : Url for API documentation
        - SERVER_APP_PATH : Application path part of the Url to access the app
        - SERVER_URL : Base Url of the server where the app is running
        - SERVER_PORT : Port the server is running on

        Parameters
        ----------
        viztrail_repository : workflow.ViztrailRepository
            Repository for viztrails (aka projects)
        datastore : database.Datastore
            Backend store for datasets
        filestore: vizier.filestore.base.Filestore
            Backend store for uploaded CSV files
        config : vizier.config.AppConfig
            Application configuration parameters
        """
        self.filestore = filestore

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

    def system_build(self):
        """Returns a dictionary with information about individual system
        components, including version information for software components.

        Returns
        -------
        dict
        """
        components = list()
        components.extend(self.datastore.components())
        components.extend(self.filestore.components())
        components.extend(self.viztrails.components())
        return serialize.SERVICE_BUILD(components, self.urls)

    # --------------------------------------------------------------------------
    # Files
    # --------------------------------------------------------------------------
    def upload_file(self, filename):
        """Upload a given file to the file server. Expects either a CSV or TSV
        file. The file type is determined by the file suffix.

        Parameters
        ----------
        filename : string
            path to file on local disk

        Returns
        -------
        vizier.filestore.base.FileHandle
        """
        return self.filestore.upload_file(filename)


    # --------------------------------------------------------------------------
    # Datasets
    # --------------------------------------------------------------------------
    def get_dataset(self, dataset_id, offset=None, limit=None):
        """Get dataset with given identifier. The result is None if no dataset
        with the given identifier exists.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier
        offset: int, optional
            Number of rows at the beginning of the list that are skipped.
        limit: int, optional
            Limits the number of rows that are returned.

        Returns
        -------
        dict
            Dictionary representation for dataset state
        """
        # Get dataset with given identifier from data store. If the dataset
        # does not exist the result is None.
        dataset = self.get_dataset_handle(dataset_id)
        if not dataset is None:
            # Determine offset and limits
            if not offset is None:
                offset = max(0, int(offset))
            else:
                offset = 0
            if not limit is None:
                result_size = int(limit)
            else:
                result_size = self.config.defaults.row_limit
            if result_size < 0 and self.config.defaults.max_row_limit > 0:
                result_size = self.config.defaults.max_row_limit
            elif self.config.defaults.max_row_limit >= 0:
                result_size = min(result_size, self.config.defaults.max_row_limit)
            # Serialize the dataset schema and cells
            return serialize.DATASET(
                dataset=dataset,
                rows=dataset.fetch_rows(offset=offset, limit=result_size),
                config=self.config,
                urls=self.urls,
                offset=offset,
                limit=limit
            )

    def get_dataset_annotations(self, dataset_id, column_id=-1, row_id=-1):
        """Get annotations for dataset with given identifier. The result is None
        if no dataset with the given identifier exists.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier

        Returns
        -------
        dict
            Dictionary representation for dataset annotations
        """
        # Get dataset with given identifier from data store. If the dataset
        # does not exist the result is None.
        dataset = self.get_dataset_handle(dataset_id)
        if dataset is None:
            return None
        anno = dataset.get_annotations(column_id=column_id, row_id=row_id)
        return serialize.DATASET_ANNOTATIONS(
            dataset_id,
            annotations=anno,
            column_id=column_id,
            row_id=row_id,
            urls=self.urls
        )

    def get_dataset_handle(self, dataset_id):
        """Get handle for dataset with given identifier. The result is None if
        no dataset with the given identifier exists.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier

        Returns
        -------
        vizier.datastore.base.DatasetHandle
        """
        if dataset_id in self.datasets:
            dataset = self.datasets[dataset_id]
        else:
            dataset = self.datastore.get_dataset(dataset_id)
            if not dataset is None:
                self.datasets[dataset_id] = dataset
        return dataset

    def update_dataset_annotation(self, dataset_id, column_id=-1, row_id=-1, anno_id=-1, key=None, value=None):
        """Update the annotations for a component of the datasets with the given
        identifier. Returns the modified object annotations or None if the
        dataset does not exist.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier
        column_id: int, optional
            Unique column identifier
        row_id: int, optional
            Unique row identifier
        anno_id: int
            Unique annotation identifier
        key: string, optional
            Annotation key
        value: string, optional
            Annotation value

        Returns
        -------
        dict
        """
        # Get dataset with given identifier from data store. If the dataset
        # does not exist the result is None.
        result = self.datastore.update_annotation(
            dataset_id,
            column_id=column_id,
            row_id=row_id,
            anno_id=anno_id,
            key=key,
            value=value
        )
        if result is None:
            return None
        # Get updated annotations. Need to ensure that the dataset is removed
        # from the cache
        if dataset_id in self.datasets:
            del self.datasets[dataset_id]
        return self.get_dataset_annotations(
            dataset_id,
            column_id=column_id,
            row_id=row_id
        )


    # --------------------------------------------------------------------------
    # Projects
    # --------------------------------------------------------------------------
    def create_project(self, env_id, properties):
        """Create a new project. All the information about a project is
        currently stored as part of the viztrail.

        Parameters
        ----------
        env_id: string
            Unique identifier of the execution environment for the new viztrail
        properties : dict
            Dictionary of user-defined project properties

        Returns
        -------
        dict
            Handle for new project
        """
        # Create a new viztrail.
        viztrail = self.viztrails.create_viztrail(env_id, properties)
        # Return a serialization of the new project.
        return serialize.PROJECT_DESCRIPTOR(viztrail, self.urls)

    def delete_project(self, project_id):
        """Delete the project with given identifier. Deletes the Vistrails
        workflow that is associated with the project and the entry in the
        repository. Both are identified by the same id.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        bool
            True, if project existed and False otherwise
        """
        # Delete entry in repository. The result indicates whether the project/
        # viztrail existed or not.
        return self.viztrails.delete_viztrail(viztrail_id=project_id)

    def get_project(self, project_id, branch_id=None, version=None):
        """Get comprehensive information for the project with the given
        identifier.

        Returns None if no project with the given identifier exists.

        If both branch_id and version are given the reference to the workflow
        that is identified by these two values is included in the results link
        list.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique branch identifier
        version: int, optional
            Workflow version identifier

        Returns
        -------
        dict
            Serialization of the project handle
        """
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Get serialization for project handle.
        return serialize.PROJECT_HANDLE(
            viztrail,
            self.urls,
            branch_id=branch_id,
            version=version
        )

    def list_module_specifications_for_project(self, project_id):
        """Retrieve list of parameter specifications for all supported modules
        for the given project. Returns None if no project (viztrail) with
        given identifier exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        dict
        """
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if not viztrail is None:
            return serialize.PROJECT_MODULE_SPECIFICATIONS(viztrail, self.urls)

    def list_projects(self):
        """Returns a list of descriptors for all projects that are currently
        contained in the project repository.

        Returns
        ------
        dict
        """
        return serialize.PROJECT_LISTING(self.viztrails.list_viztrails(), self.urls)

    def update_project_properties(self, project_id, properties):
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
        # Return serialization for project handle.
        return serialize.PROJECT_DESCRIPTOR(viztrail, self.urls)

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

    # --------------------------------------------------------------------------
    # Notebook
    # --------------------------------------------------------------------------
    def get_notebook(self, project_id, branch_id=None, version=None):
        """Retrieve a workflow notebook from a given project.

        Returns None if no project, branch, or workflow with given identifiers
        exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string, optional
            Unique workflow branch identifier. Defaults to master
        version: int, optional
            Version number of the modified workflow. Defaults to head

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
        if version is None:
            version = -1
        workflow = self.viztrails.get_workflow(
            viztrail_id=project_id,
            branch_id=branch_id if not branch_id is None else DEFAULT_BRANCH,
            workflow_version=version
        )
        if workflow is None:
            return None
        # If an explicit workflow version was requested the workflow will be
        # marked as read only.
        return serialize.NOTEBOOK_HANDLE(
            viztrail,
            workflow,
            dataset_cache=self.get_dataset_handle,
            config=self.config,
            urls=self.urls,
            read_only=(version != -1)
        )
