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

import json
import requests

from vizier.api.client.resources.module import ModuleResource
from vizier.api.client.resources.workflow import WorkflowResource
from vizier.api.client.datastore.base import DatastoreClient

import vizier.api.serialize.hateoas as ref
import vizier.api.serialize.labels as labels


class DatastoreClientUrlFactory(object):
    """Url factory for API client datastores that access and manipulate datasets
    for a vizier project. This class is a wrapper around a dataset descriptor
    returned by the API and that contains all the urls as HATEOAS references.
    """
    def __init__(self, dataset):
        """Intialize the dataset descriptor containing the references.

        Parameters
        ----------
        dataset: vizier.api.client.resources.dataset.DatasetDescriptor
            Descriptor for dataset
        """
        self.dataset = dataset

    # --------------------------------------------------------------------------
    # Datasets
    # --------------------------------------------------------------------------
    def get_dataset(self, dataset_id):
        """Url to retrieve dataset rows.

        Parameters
        ----------
        dataset_id: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.dataset.links[ref.DATASET_FETCH_ALL]


class Notebook(object):
    """A notebook is a wrapper around a workflow instance for a particular
    project.
    """
    def __init__(self, project_id, workflow):
        """Initialize the internal components.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        workflow: vizier.api.client.resources.workflow.WorkflowResource
            Workflow that defines the notebook
        """
        self.project_id = project_id
        self.workflow = workflow
        self.links = workflow.links

    def append_cell(self, command):
        """Append a new module to the notebook that executes te given command.

        Parameters
        ----------
        command: vizier.viztrail.command.ModuleCommand

        Returns
        -------
        vizier.api.client.resources.workflow.WorkflowResource
        """
        # Get append url and create request body
        url = self.links[ref.WORKFLOW_APPEND]
        data = {
            labels.COMMAND_PACKAGE: command.package_id,
            labels.COMMAND_ID: command.command_id,
            labels.COMMAND_ARGS: command.arguments.to_list()
        }
        # Send request. Raise exception if status code indicates that the
        # request was not successful
        r = requests.post(url, json=data)
        r.raise_for_status()
        # The returned update statement is a reduced version of the workflow
        # handle that contains the modules that were affected by the append
        # operation
        return WorkflowResource.from_dict(json.loads(r.text))

    def cancel_exec(self):
        """Cancel exection of tasks for the notebook.

        Returns
        -------
        vizier.api.client.resources.workflow.WorkflowResource
        """
        url = self.workflow.links[ref.WORKFLOW_CANCEL]
        r = requests.post(url)
        r.raise_for_status()
        # The returned update statement is a reduced version of the workflow
        # handle that contains the modules that were affected by the operation
        return WorkflowResource.from_dict(json.loads(r.text))

    def delete_module(self, module_id):
        """Delete the notebook module with the given identifier.

        Returns
        -------
        vizier.api.client.resources.workflow.WorkflowResource
        """
        module = self.get_module(module_id)
        url = module.links[ref.MODULE_DELETE]
        r = requests.delete(url)
        r.raise_for_status()
        # The returned update statement is a reduced version of the
        # workflow handle that contains the modules that were affected
        # by the delete operation
        return WorkflowResource.from_dict(json.loads(r.text))

    def download_dataset(self, dataset, target_file):
        """Download the given datast to the given target path.

        Parameters
        ----------
        dataset: vizier.api.client.resources.DatasetDescriptor
            Descriptor for dataset that is downloaded
        target_file: string
            Target path for storing downloaded file
        """
        url = dataset.links[ref.DATASET_DOWNLOAD]
        r = requests.get(url, allow_redirects=True)
        with open(target_file, 'wb') as f:
            f.write(r.content)

    def fetch_dataset(self, dataset):
        """Fetch handle for the dataset with the given identifier.

        Parameters
        ----------
        dataset: vizier.api.client.resources.dataset.DatasetDescriptor
            Descriptor for dataset

        Returns
        -------
        vizier.datastore.base.DatasetHandle
        """
        # Use the remote datastore client to get the dataset handle
        db = DatastoreClient(urls=DatastoreClientUrlFactory(dataset=dataset))
        return db.get_dataset(identifier=dataset.identifier)

    def get_dataset(self, identifier):
        """Get descriptor for dataset with given identifier. If the dataset does
        not exist a ValueError exception is raised.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier

        Returns
        -------
        """
        if not identifier in self.workflow.datasets:
            raise ValueError('unknown datasets \'' + identifier + '\'')
        return self.workflow.datasets[identifier]

    def get_module(self, module_id):
        """Get the workflow module with the given identifier. Returns None if
        no module with the given identifier exists.

        Parameters
        ----------
        module_id: string
            Unique module identifier

        Returns
        -------
        vizier.api.client.resources.module.ModuleResource
        """
        if module_id is None and len(self.workflow.modules) > 0:
            return self.workflow.modules[-1]
        for module in self.workflow.modules:
            if module.identifier == module_id:
                return module
        return None

    def insert_cell(self, command, before_module):
        """Insert a new module to the notebook that executes te given command.
        The module is inserted before the module with the given identifier.

        Parameters
        ----------
        command: vizier.viztrail.command.ModuleCommand
            The command that is executed in the new notebook cell
        before_module: string
            Unique module identifier

        Returns
        -------
        vizier.api.client.resources.workflow.WorkflowResource
        """
        # Get the module identified by the before module identifier
        module = self.get_module(before_module)
        url = module.links[ref.MODULE_INSERT]
        data = {
            labels.COMMAND_PACKAGE: command.package_id,
            labels.COMMAND_ID: command.command_id,
            labels.COMMAND_ARGS: command.arguments.to_list()
        }
        # Send request. Raise exception if status code indicates that the
        # request was not successful
        r = requests.post(url, json=data)
        r.raise_for_status()
        # handle that contains the modules that were affected by the insert
        # operation
        return WorkflowResource.from_dict(json.loads(r.text))

    def replace_cell(self, command, module_id):
        """Replace the command for the module with the given identifier.

        Parameters
        ----------
        command: vizier.viztrail.command.ModuleCommand
            The command that is executed in the new notebook cell
        module_id: string
            Unique module identifier

        Returns
        -------
        vizier.api.client.resources.workflow.WorkflowResource
        """
        # Get the module with the given identifier
        module = self.get_module(module_id)
        url = module.links[ref.MODULE_REPLACE]
        data = {
            labels.COMMAND_PACKAGE: command.package_id,
            labels.COMMAND_ID: command.command_id,
            labels.COMMAND_ARGS: command.arguments.to_list()
        }
        # Send request. Raise exception if status code indicates that the
        # request was not successful
        r = requests.put(url, json=data)
        r.raise_for_status()
        # handle that contains the modules that were affected by the replace
        # operation
        return WorkflowResource.from_dict(json.loads(r.text))

    def upload_file(self, filename):
        """Upload a file from local disk to notebooks filestore. Returns the
        identifier of the uploaded file.

        Parameters
        ----------
        filename: string
            Path to file on local disk

        Returns
        -------
        string
        """
        # Get the request Url and create the request body
        url = self.links[ref.FILE_UPLOAD]
        files = {'file': open(filename,'rb')}
        # Send request. The result is the handle for the uploaded file.
        r = requests.post(url, files=files)
        r.raise_for_status()
        # The result is the file identifier
        return json.loads(r.text)['id']
