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

"""The url factory is used to generate url for all resources (routes) that are
accessible via the web service.
"""


class UrlFactory:
    """Factory to create urls for all routes that the webservice supports."""

    def __init__(self, config):
        """Intialize the base url for the web service.

        Parameters
        ----------
        config: vizier.config.AppConfig
            Application configuration parameters
        """
        self.config = config
        # Construct base Url from server url, port, and application path.
        self.base_url = config.app_base_url
        # Ensure that base_url does not end with a slash
        while len(self.base_url) > 0:
            if self.base_url[-1] == '/':
                self.base_url = self.base_url[:-1]
            else:
                break

    # --------------------------------------------------------------------------
    # Service
    # --------------------------------------------------------------------------

    def service_descriptor(self):
        """Base Url for the webservice. Provides access to the service
        descriptor.

        Returns
        -------
        string
        """
        return self.base_url

    def api_doc(self):
        """Url to the service API documentation.

        Returns
        -------
        string
        """
        return self.config.webservice.doc_url

    # --------------------------------------------------------------------------
    # Projects
    # --------------------------------------------------------------------------
    def create_project(self):
        """Url to create a new project.

        Returns
        -------
        string
        """
        return self.list_projects()

    def delete_project(self, project_id):
        """Url to delete the project with the given identifier.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        string
        """
        return self.get_project(project_id)

    def get_project(self, project_id):
        """Url to retrieve the project with the given identifier.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        string
        """
        return self.list_projects() + '/' + project_id

    def list_projects(self):
        """Url to retrieve the list of active projects.

        Returns
        -------
        string
        """
        return self.base_url + '/projects'

    def update_project(self, project_id):
        """Url to update properties for the project with the given identifier.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        string
        """
        return self.get_project(project_id)

    # --------------------------------------------------------------------------
    # Branches
    # --------------------------------------------------------------------------
    def create_branch(self, project_id):
        """Url to delete the project branch with the given identifier.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier

        Returns
        -------
        string
        """
        return self.get_project(project_id) + '/branches'

    def delete_branch(self, project_id, branch_id):
        """Url to delete the project branch with the given identifier.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier

        Returns
        -------
        string
        """
        return self.get_branch(project_id, branch_id)

    def get_branch(self, project_id, branch_id):
        """Url to retrieve the project branch with the given identifier.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier

        Returns
        -------
        string
        """
        return self.create_branch(project_id) + '/' + branch_id

    def get_branch_head(self, project_id, branch_id):
        """Url to retrieve the workflow that is at the head of the given
        project branch.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier

        Returns
        -------
        string
        """
        return self.get_branch(project_id, branch_id) + '/head'

    def update_branch(self, project_id, branch_id):
        """Url to update properties for the project branch with the given
        identifier.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier

        Returns
        -------
        string
        """
        return self.get_branch(project_id, branch_id)

    # --------------------------------------------------------------------------
    # Workflows
    # --------------------------------------------------------------------------
    def cancel_workflow(self, project_id, branch_id):
        """Url to cancel the execution of the workflow at the branch head.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier

        Returns
        -------
        string
        """
        return self.get_branch_head(project_id, branch_id) + '/cancel'

    def get_workflow_module(self, project_id, branch_id, module_id):
        """Url to get the current state of the specified module in the head of
        the identified project branch.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier
        module_id: string
            Unique module identifier

        Returns
        -------
        string
        """
        return self.get_branch_head(project_id, branch_id) + '/modules/' + module_id

    def workflow_append(self, project_id, branch_id):
        """Url to append a module to a given branch. Modules can only be
        appended to the branch head. Therefore, the workflow identifier is not
        needed to identify the target workflow.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier

        Returns
        -------
        string
        """
        return self.get_branch_head(project_id, branch_id)

    # --------------------------------------------------------------------------
    # Files
    # --------------------------------------------------------------------------
    def file_download(self, project_id, file_id):
        """File download url.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        file_id: string
            Unique file identifier

        Returns
        -------
        string
        """
        return self.get_project(project_id) + '/files/' + file_id

    def file_upload(self, project_id):
        """File upload url for the given project.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        string
        """
        return self.get_project(project_id) + '/upload'
