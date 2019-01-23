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

"""Client-size apit for a remote vizier instance."""

import json
import requests
import urllib2

from vizier.api.client.resources.branch import BranchResource
from vizier.api.client.resources.notebook import Notebook
from vizier.api.client.resources.project import ProjectResource
from vizier.api.client.resources.view import ChartView
from vizier.api.client.resources.workflow import WorkflowResource

import vizier.api.serialize.base as serialize
import vizier.api.serialize.deserialize as deserialize
import vizier.api.serialize.labels as labels


"""Annotation keys for default values."""
KEY_DEFAULT_BRANCH = 'branch'
KEY_DEFAULT_PROJECT = 'project'

"""Default error messages."""
MSG_NO_DEFAULT_BRANCH = 'Default branch not set'
MSG_NO_DEFAULT_PROJECT = 'Default project not set'


class VizierApiClient(object):
    """Client-size API to remote vizier instances provides access to resources
    that are available at the instance.
    """
    def __init__(self, urls, defaults=None):
        """Initialize the url factory that is used to access and manipulate
        resoures on the vizier instance.

        Parameters
        ----------
        urls: vizier.api.routes.base.UrlFactory
            Factory for request urls
        defaults: vizier.core.annotation.base.ObjectAnnotationSet
            Annotation set for default values
        """
        self.urls = urls
        # We only set the defaults if a value is given. Otherwise, the local
        # variables are not initialized.
        if not defaults is None:
            self.defaults = defaults
            # Set the default project
            project_id = self.defaults.find_one(KEY_DEFAULT_PROJECT)
            if not project_id is None:
                self.default_project = project_id
            else:
                self.default_project = None
            branch_id = self.defaults.find_one(KEY_DEFAULT_BRANCH)
            if not branch_id is None:
                self.default_branch = branch_id
            else:
                self.default_branch = None

    def create_branch(self, project_id, properties, branch_id=None, workflow_id=None, module_id=None):
        """Create a new project resource at the server. The combination of
        branch_id, workflow_id and module_id specifies the branch point. If all
        values are None an empty branch is created.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        properties: dict
            Dictionary of project properties
        branch_id: string
            Unique branch identifier
        workflow_id: string
            Unique workflow identifier
        module_id: string
            Unique module identifier

        Results
        -------
        vizier.api.client.resources.branch.BranchResource
        """
        # Get the request url and create the request body
        url = self.urls.create_branch(project_id)
        data = {labels.PROPERTIES: serialize.PROPERTIES(properties)}
        if not branch_id is None and not workflow_id is None and not module_id is None:
            data['source'] = {
                'branchId': branch_id,
                'workflowId': workflow_id,
                'moduleId': module_id
            }
        elif not branch_id is None or not workflow_id is None or not module_id is None:
            raise ValueError('invalid branch source')
        # Send request. Raise exception if status code indicates that the
        # request was not successful
        r = requests.post(url, json=data)
        r.raise_for_status()
        # The result is the new branch descriptor
        return BranchResource.from_dict(json.loads(r.text))

    def create_project(self, properties):
        """Create a new project resource at the server.

        Parameters
        ----------
        properties: dict
            Dictionary of project properties

        Results
        -------
        vizier.api.client.resources.project.ProjectResource
        """
        # Get the request url and create the request body
        url = self.urls.create_project()
        data = {labels.PROPERTIES: serialize.PROPERTIES(properties)}
        # Send request. Raise exception if status code indicates that the
        # request was not successful
        r = requests.post(url, json=data)
        r.raise_for_status()
        # The result is the new project descriptor
        return ProjectResource.from_dict(json.loads(r.text))

    def delete_branch(self, project_id, branch_id):
        """Delete the project branch at the server.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier

        Results
        -------
        bool
        """
        # Get the request url
        url = self.urls.delete_branch(
            project_id=project_id,
            branch_id=branch_id
        )
        # Send request. Raise exception if status code indicates that the
        # request was not successful. Otherwise return True.
        r = requests.delete(url)
        r.raise_for_status()
        return True

    def delete_project(self, project_id):
        """Delete the project resource at the server.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Results
        -------
        bool
        """
        # Get the request url
        url = self.urls.delete_project(project_id)
        # Send request. Raise exception if status code indicates that the
        # request was not successful. Otherwise return True.
        r = requests.delete(url)
        r.raise_for_status()
        return True

    def fetch_chart(self, url):
        """Fetch data series for a chart in a workflow module.

        Parameters
        ----------
        url: string
            Url to retrieve chart data

        Returns
        -------
        vizier.api.client.resources.view.ChartView
        """
        r = requests.get(url)
        r.raise_for_status()
        return ChartView.from_dict(json.loads(r.text))

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

    def get_default_branch(self):
        """Get the identifier of the selected default branch. Raises ValueError
        if the branch is not selected.

        Returns
        -------
        string
        """
        if self.default_branch is None:
            raise ValueError(MSG_NO_DEFAULT_BRANCH)
        return self.default_branch

    def get_default_project(self):
        """Get the identifier of the selected default project. Raises a
        ValueError if the project is not set.

        Returns
        -------
        string
        """
        if self.default_project is None:
            raise ValueError(MSG_NO_DEFAULT_PROJECT)
        return self.default_project

    def get_notebook(self, project_id=None, branch_id=None):
        """Get the notebook that is defined by the workflow that is at the
        head of the given branch. If values for project or branch identifier
        are omitted the default values will be used.

        Parameters
        ----------
        project_id: string, optional
            Unique project identifier
        branch_id: string, optional
            Unique branch identifier

        Returns
        -------
        vizier.api.client.resources.notebook.Notebook
        """
        # Use the default project if not project identifier is given
        if project_id is None:
            project_id = self.get_default_project()
        # Use the default branch if not branch identifier is given
        if branch_id is None:
            branch_id = self.get_default_branch()
        return Notebook(
            project_id=project_id,
            workflow=self.get_workflow(
                project_id=project_id,
                branch_id=branch_id
            )
        )

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

    def info(self):
        """Print information about the API (from the API service descriptor)."""
        r = requests.get(self.urls.service_descriptor())
        r.raise_for_status()
        print 'Name    : ' + doc['name']
        print 'URL     : ' + deserialize.HATEOAS(doc['links'])['self']
        print 'Engine  : ' + doc['environment']['name']
        print 'Backend : ' + doc['environment']['backend']
        print 'Packages: ' + ', '.join(doc['environment']['packages'])
        print 'Version : ' + doc['environment']['version']
        print 'Started : ' + doc['startedAt']

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

    def update_branch(self, project_id, branch_id, properties):
        """Update the properties of a given project branch.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier
        properties: dict
            Properties update statements

        Returns
        -------
        vizier.api.client.resources.branch.BranchResource
        """
        # Get the request url and create the request body
        url = self.urls.update_branch(project_id, branch_id)
        data = {labels.PROPERTIES: serialize.PROPERTIES(properties)}
        # Send request. Raise exception if status code indicates that the
        # request was not successful
        r = requests.put(url, json=data)
        r.raise_for_status()
        # The result is the new project descriptor
        return BranchResource.from_dict(json.loads(r.text))

    def update_project(self, project_id, properties):
        """Update the properties of a given project.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        properties: dict
            Properties update statements

        Returns
        -------
        vizier.api.client.resources.project.ProjectResource
        """
        # Get the request url and create the request body
        url = self.urls.update_project(project_id)
        data = {labels.PROPERTIES: serialize.PROPERTIES(properties)}
        # Send request. Raise exception if status code indicates that the
        # request was not successful
        r = requests.put(url, json=data)
        r.raise_for_status()
        # The result is the new project descriptor
        return ProjectResource.from_dict(json.loads(r.text))
