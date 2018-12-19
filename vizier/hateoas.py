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

"""Hypermedia As The Engine of Application State - Factory for Web API resource
Urls.

The Web API attempts to follow the Hypermedia As The Engine of Application State
(HATEOAS) constraint. Thus, every serialized resource contains a list of
references for clients to interact with the API.

The URLFactory class in this s contains all methods to generate HATEOAS
references for resources that are accessible via the Vizier Web API.
"""


"""Pagination query parameter."""
PAGE_LIMIT = 'limit'
PAGE_OFFSET = 'offset'

"""HATEOAS relation identifier."""
REL_ANNOTATED = 'annotated'
REL_ANNOTATIONS='annotations'
REL_APIDOC = 'doc';
REL_APPEND = 'append';
REL_BRANCH = 'branch'
REL_BRANCHES = 'branches'
REL_CREATE = 'create'
REL_CURRENT_VERSION = 'currentVersion'
REL_DATASET = 'dataset'
REL_DELETE = 'delete'
REL_DOWNLOAD = 'download'
REL_FILES = 'files'
REL_HEAD = 'head'
REL_INSERT = 'insert'
REL_MODULES = 'modules'
REL_MODULE_SPECS = 'environment'
REL_NOTEBOOK = 'notebook'
REL_NOTEBOOKS = 'notebooks'
REL_PAGE = 'page'
REL_PAGE_FIRST = REL_PAGE + 'first'
REL_PAGE_LAST = REL_PAGE + 'last'
REL_PAGE_NEXT = REL_PAGE + 'next'
REL_PAGE_PREV = REL_PAGE + 'prev'
REL_PROJECT = 'project'
REL_PROJECTS = 'projects'
REL_RENAME = 'rename'
REL_REPLACE = 'replace'
REL_SERVICE = 'home'
REL_SYSTEM_BUILD = 'build'
REL_UPDATE = 'update'
REL_UPLOAD = 'upload'
REL_WORKFLOW = 'workflow'


class UrlFactory:
    """Factory for API resource Urls. Contains the definitions of Url's for any
    resource that is accessible through the Web API in a single class.

    Attributes
    ----------
    base_url: string
        Prefix for all resource Url's
    """
    def __init__(self, config):
        """Intialize the common Url prefix for all API resources.

        Parameters
        ----------
        config: vizier.config.app.AppConfig
            Application configuration parameters
        """
        # Construct base Url from server url, port, and application path.
        self.base_url = config.api.app_base_url
        # Ensure that base_url does not end with a slash
        while len(self.base_url) > 0:
            if self.base_url[-1] == '/':
                self.base_url = self.base_url[:-1]
            else:
                break

    def branches_url(self, project_id):
        """Url to retrieve (GET) the list of branches for a project with the
        given identifier.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        string
        """
        return self.project_url(project_id) + '/branches'

    def branch_url(self, project_id, branch_id):
        """Url to retrieve (GET) the branch with given identifier for a given
        project.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Workflow branch identifier

        Returns
        -------
        string
        """
        return self.branches_url(project_id) + '/' + branch_id

    def branch_head_url(self, project_id, branch_id):
        """Url to access the workflow at the branch HEAD.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Workflow branch identifier

        Returns
        -------
        string
        """
        return self.branch_url(project_id, branch_id) + '/head'

    def branch_head_append_url(self, project_id, branch_id):
        """Url to append a module to the workflow at the branch HEAD.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Workflow branch identifier

        Returns
        -------
        string
        """
        return self.branch_head_url(project_id, branch_id) + '/modules'

    def branch_update_url(self, project_id, branch_id):
        """Url to update (POST) the properties of a workflow branch.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Workflow branch identifier

        Returns
        -------
        string
        """
        return self.branch_url(project_id, branch_id) + '/properties'

    def datasets_url(self):
        """Base Url for dataset resources.

        Returns
        -------
        string
        """
        return self.base_url + '/datasets'

    def datasets_upload_url(self):
        """Url to create dataset from CSV or TSV file.

        Returns
        -------
        string
        """
        return self.datasets_url()

    def dataset_pagination_url(self, dataset_id, offset=0, limit=None):
        """Get Url for dataset row pagination.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier
        offset: int, optional
            Pagination offset. The returned Url always includes an offset
            parameter
        limit: int, optional
            Dataset row limit. Only included if not None

        Returns
        -------
        string
        """
        query = PAGE_OFFSET + '=' + str(offset)
        if not limit is None:
            query += '&' + PAGE_LIMIT + '=' + str(limit)
        else:
            return self.dataset_url(dataset_id) + '?' + query

    def dataset_url(self, dataset_id):
        """Url to retrieve dataset state in Json format.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.datasets_url() + '/' + dataset_id

    def dataset_annotations_url(self, dataset_id):
        """Url to retrieve dataset annotations.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.dataset_url(dataset_id) + '/annotations'

    def dataset_download_url(self, dataset_id):
        """Url to retrieve a dataset in CSV format.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.dataset_url(dataset_id) + '/csv'

    def dataset_with_annotations_url(self, dataset_id):
        """Url to retrieve a dataset together with all of its annotations.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.dataset_url(dataset_id) + '?includeAnnotations=true'

    def files_url(self):
        """Base Url for file server resources.

        Returns
        -------
        string
        """
        return self.base_url + '/files'

    def files_upload_url(self):
        """Url to upload CSV or TSV file.

        Returns
        -------
        string
        """
        return self.files_url() + '/upload'

    def file_url(self, name):
        """Url to retrieve uploaded file handle.

        Parameters
        ----------
        name : string
            File name

        Returns
        -------
        string
        """
        return self.files_url() + '/' + name

    def file_download_url(self, name):
        """Url to retrieve a file server resource in CSV format.

        Parameters
        ----------
        name : string
            File name

        Returns
        -------
        string
        """
        return self.file_url(name) + '/download'

    def notebooks_url(self):
        """Url to retrieve (GET) complete information for a workflow's
        notebook.

        Returns
        -------
        string
        """
        return self.service_url() + '/notebooks'

    def projects_url(self):
        """Url to retrieve project listing (GET) and to create new project
        (POST).

        Returns
        -------
        string
        """
        return self.service_url() + '/projects'

    def project_url(self, project_id):
        """Url to retrieve (GET) or delete (DELETE) project with given
        identifier.

        Returns
        -------
        string
        """
        return self.projects_url() + '/' + project_id

    def project_module_specs_url(self, project_id):
        """Url to retrieve the list of available module specification for a
        given project.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        string
        """
        return self.project_url(project_id) + '/modulespecs'

    def service_url(self):
        """Base Url for the Web API server.

        Returns
        -------
        string
        """
        return self.base_url

    def system_build_url(self):
        """Url to retrieve system configuration information.

        Returns
        -------
        string
        """
        return self.service_url() + '/build'

    def update_project_properties_url(self, project_id):
        """Url to update (POST) the set of user-defined properties for the
        project with given identifier.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        string
        """
        return self.project_url(project_id) + '/properties'

    def workflow_url(self, project_id, branch_id, version):
        """Url to retrieve (GET) a project workflow.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique branch identifier
        version: int
            Workflow version identifier

        Returns
        -------
        string
        """
        # Set negative versions to 0. Otherwise, the API will not recognize them
        # as integers
        if version < 0:
            version = 0
        branch_url = self.branch_url(project_id, branch_id)
        return branch_url + '/workflows/' + str(version)

    def workflow_append_url(self, project_id, branch_id, version):
        """Url to to append (POST) a module at the end of a given workflow.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique branch identifier
        version: int
            Workflow version identifier

        Returns
        -------
        string
        """
        return self.workflow_modules_url(project_id, branch_id, version)

    def workflow_module_url(self, project_id, branch_id, version, module_id):
        """Url for workflow module. Used to delete, insert, and replace a
        module in a workflow.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Workflow branch identifier
        version: int
            Workflow version identifier
        module_id: int
            Unique module identifier

        Returns
        -------
        string
        """
        workflow_url = self.workflow_url(project_id, branch_id, version)
        return workflow_url + '/modules/' + str(module_id)

    def workflow_module_view_url(self, project_id, branch_id, version, module_id, view_id):
        """Url to access the content of a dataset view that is associated with a
        workflow module.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Workflow branch identifier
        version: int
            Workflow version identifier
        module_id: int
            Unique module identifier
        view_id: string
            Unique view identifier

        Returns
        -------
        string
        """
        module_url = self.workflow_module_url(project_id, branch_id, version, module_id)
        return module_url + '/views/' + view_id

    def workflow_modules_url(self, project_id, branch_id, version):
        """Url to retrieve (GET) all modules of a given workflow.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique branch identifier
        version: int
            Workflow version identifier

        Returns
        -------
        string
        """
        return self.workflow_url(project_id, branch_id, version) + '/modules'



# ------------------------------------------------------------------------------
#
# Helper Methods
#
# ------------------------------------------------------------------------------

def reference(rel, href):
    """Get HATEOAS reference object containing the Url 'href' and the link
    relation 'rel' that defines the type of the link.

    Parameters
    ----------
    rel : string
        Descriptive attribute defining the link relation
    href : string
        Http Url

    Returns
    -------
    dict
        Dictionary containing elements 'rel' and 'href'
    """
    return {'rel' : rel, 'href' : href}


def self_reference(url):
    """Get HATEOAS self reference for a API resources.

    Parameters
    ----------
    url : string
        Url to resource

    Returns
    -------
    dict
        Dictionary containing elements 'rel' and 'href'
    """
    return reference('self', url)
