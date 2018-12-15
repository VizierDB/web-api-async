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
import urllib2

from vizier.api.client.resources.branch import BranchResource
from vizier.api.client.resources.project import ProjectResource
from vizier.api.client.resources.workflow import WorkflowResource


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
        vizier.api.client.resources.branch.BranchResource
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
        list(vizier.api.client.resources.project.ProjectResource)
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
        vizier.api.client.resources.workflow.WorkflowResource

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
        list(vizier.api.client.resources.project.ProjectResource)
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
        list(vizier.api.client.resources.project.ProjectResource)
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
