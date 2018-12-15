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

"""Client-size apit for a remote vizier instance."""

import json
import requests
import urllib2

from vizier.client.api.resources.branch import BranchResource
from vizier.client.api.resources.project import ProjectResource
from vizier.client.api.resources.workflow import WorkflowResource

import vizier.api.serialize.base as serialize
import vizier.api.serialize.labels as labels
import vizier.api.serialize.dataset as serialds
import vizier.api.serialize.module as serialmd
import vizier.viztrail.module.base as states


class VizierApiClient(object):
    """Client-size API to remote vizier instances provides access to resources
    that are available at the instance.
    """
    def __init__(self, urls):
        """Initialize the url factory that is used to access and manipulate
        resoures on the vizier instance.

        Parameters
        ----------
        urls: vizier.api.webservice.routes.UrlFactory
            Factory for resource urls
        """
        self.urls = urls

    def get_branch(self, project_id, branch_id):
        """Fetch a project branch from the remote web service API.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier

        Returns
        -------
        vizier.client.api.resources.branch.BranchResource
        """
        # Fetch project resource
        url = self.urls.get_branch(project_id=project_id, branch_id=branch_id)
        response = urllib2.urlopen(url)
        data = json.loads(response.read())
        # Convert result into instance of the project resource class
        return BranchResource.from_dict(data)

    def get_project(self, project_id):
        """Fetch list of project from remote web service API.

        Returns
        -------
        list(vizier.client.api.resources.project.ProjectResource)
        """
        # Fetch project resource
        url = self.urls.get_project(project_id)
        response = urllib2.urlopen(url)
        data = json.loads(response.read())
        # Convert result into instance of the project resource class
        return ProjectResource.from_dict(data)

    def get_workflow(self, project_id, branch_id, workflow_id=None):
        """Get a workflow resource. If the identifier is None the branch head
        will be returned.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier
        workflow_id: string, optional
            Unique workflow identifier

        Returns
        -------
        vizier.client.api.resources.workflow.WorkflowResource

        """
        # Fetch workflow handle from server
        if workflow_id is None:
            url = self.urls.get_branch_head(
                project_id=project_id,
                branch_id=branch_id
            )
        else:
            url = self.urls.get_workflow(
                project_id=project_id,
                branch_id=branch_id,
                workflow_id=workflow_id
            )
        response = urllib2.urlopen(url)
        data = json.loads(response.read())
        # Convert result into instance of a workflow resource
        return WorkflowResource.from_dict(data)


    def list_branches(self, project_id):
        """Fetch list of project from remote web service API.

        Returns
        -------
        list(vizier.client.api.resources.project.ProjectResource)
        """
        # There is no separate API call to fetch all project branches. Fetch
        # the project handle instead which will contain the branch descriptors.
        url = self.urls.get_project(project_id)
        response = urllib2.urlopen(url)
        data = json.loads(response.read())
        # Convert branchs in result into list of branch resources
        branches = list()
        for obj in data['branches']:
            branches.append(BranchResource.from_dict(obj))
        return branches

    def list_projects(self):
        """Fetch list of project from remote web service API.

        Returns
        -------
        list(vizier.client.api.resources.project.ProjectResource)
        """
        # Fetch projects listing
        url = self.urls.list_projects()
        response = urllib2.urlopen(url)
        data = json.loads(response.read())
        # Convert result into list of project resources
        projects = list()
        for obj in data['projects']:
            projects.append(ProjectResource.from_dict(obj))
        return projects

    def set_error(self, project_id, task_id, finished_at=None, outputs=None):
        """Set status of a given task to error.

        If the web service signals that the task status was updated the result
        will be True, otherwise False. If the project or task were unknown the
        result is None.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        task_id : string
            Unique task identifier
        finished_at: datetime.datetime, optional
            Timestamp when module started running
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
            Output streams for module

        Returns
        -------
        bool
        """
        # Get the request url and request body
        url = self.urls.set_task_state(project_id=project_id, task_id=task_id)
        data = {labels.STATE: states.MODULE_ERROR}
        if not finished_at is None:
            data[labels.FINISHED_AT] = finished_at.isoformat()
        if not outputs is None:
            data[labels.OUTPUTS] = serialize.OUTPUTS(outputs)
        # Send request
        r = requests.put(url, json=data)
        # A status code 200 will signal success. All other codes indicate an
        # error or unknown project or task identifier.
        if r.status_code != 200:
            return None
        result = json.loads(r.text)[labels.RESULT]
        return (result > 0)

    def set_running(self, project_id, task_id, started_at=None):
        """Set status of a given task to running. If the started_at argument is
        None the current time will be used instaed.

        If the returned value is 0  the task state was not changed. A positive
        value signals that the task state was changed successfully.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        task_id : string
            Unique task identifier
        started_at: datetime.datetime, optional
            Timestamp when module started running

        Returns
        -------
        int
        """
        # Get the request url and request body
        url = self.urls.set_task_state(project_id=project_id, task_id=task_id)
        data = {labels.STATE: states.MODULE_RUNNING}
        if not started_at is None:
            data[labels.STARTED_AT] = started_at.isoformat()
        # Send request
        r = requests.put(url, json=data)
        # A status code 200 will signal success. All other codes indicate an
        # error or unknown project or task identifier.
        if r.status_code != 200:
            return None
        result = json.loads(r.text)[labels.RESULT]
        return (result > 0)

    def set_success(self, project_id, task_id, finished_at=None, datasets=None, outputs=None, provenance=None):
        """Set status of a given task to success.

        If the returned value is 0  the task state was not changed. A positive
        value signals that the task state was changed successfully.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        task_id : string
            Unique task identifier
        finished_at: datetime.datetime, optional
            Timestamp when module started running
        datasets : dict(), optional
            Dictionary of resulting datasets. The user-specified name is the key
            for the dataset identifier.
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
            Output streams for module
        provenance: vizier.viztrail.module.provenance.ModuleProvenance, optional
            Provenance information about datasets that were read and writen by
            previous execution of the module.

        Returns
        -------
        int
        """
        # Get the request url and request body
        url = self.urls.set_task_state(project_id=project_id, task_id=task_id)
        data = {labels.STATE: states.MODULE_SUCCESS}
        if not finished_at is None:
            data[labels.FINISHED_AT] = finished_at.isoformat()
        if not outputs is None:
            data[labels.OUTPUTS] = serialize.OUTPUTS(outputs)
        if not datasets is None:
            data['datasets'] = list()
            for name in datasets:
                data['datasets'].append(
                    serialds.DATASET_DESCRIPTOR(
                        dataset=datasets[name],
                        name=name
                    )
                )
        if not provenance is None:
            data[labels.PROVENANCE] = serialmd.PROVENANCE(provenance)
        # Send request
        r = requests.put(url, json=data)
        # A status code 200 will signal success. All other codes indicate an
        # error or unknown project or task identifier.
        if r.status_code != 200:
            return None
        result = json.loads(r.text)[labels.RESULT]
        return (result > 0)
