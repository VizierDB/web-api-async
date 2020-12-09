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

"""The url factory is used to generate urls for all resources (routes) that are
accessible via the web service.
"""
from typing import Optional, Dict, List, Any, Tuple
import vizier.api.serialize.labels as labels


"""Element keys for initialization using a properties dictionary."""
PROPERTIES_BASEURL = 'baseUrl'
PROPERTIES_APIDOCURL = 'apiDocUrl'


"""Pagination query parameter."""
PAGE_LIMIT = 'limit'
PAGE_OFFSET = 'offset'

"""Profiling."""
FORCE_PROFILER = 'profile'


class UrlFactory(object):
    """Factory to create urls for all routes that the webservice supports."""
    def __init__(self, 
            base_url: Optional[str] = None, 
            api_doc_url: Optional[str] = None, 
            properties: Optional[Dict[str, str]] = None
        ):
        """Intialize the base url for the web service. The object can be
        initialized from a properties dictionary that may contain the base url
        and api doc url. Values in the properties dictionary override the other
        two arguments. If no base url is given a ValueError is raised.

        Parameters
        ----------
        base_url: string, optional
            Prefix for all urls
        api_doc_url: string, optional
            Url for the API documentation
        properties: dict, optional
            Dictionary that may contain the base url an the API documentation
            url
        """
        self.api_doc_url = api_doc_url
        self.base_url:str
        if properties is not None and PROPERTIES_BASEURL in properties:
            self.base_url = properties[PROPERTIES_BASEURL]
        else:
            assert(base_url is not None)
            self.base_url = base_url

        if properties is not None and PROPERTIES_APIDOCURL in properties:
            self.api_doc_url = properties[PROPERTIES_APIDOCURL]

        # Ensure that base_url does not end with a slash
        while len(self.base_url) > 0:
            if self.base_url[-1] == '/':
                self.base_url = self.base_url[:-1]
            else:
                break

    # --------------------------------------------------------------------------
    # Service
    # --------------------------------------------------------------------------

    def service_descriptor(self) -> str:
        """Base Url for the webservice. Provides access to the service
        descriptor.

        Returns
        -------
        string
        """
        return self.base_url

    def api_doc(self) -> Optional[str]:
        """Url to the service API documentation.

        Returns
        -------
        string
        """
        return self.api_doc_url

    # --------------------------------------------------------------------------
    # Projects
    # --------------------------------------------------------------------------
    def create_project(self) -> str:
        """Url to create a new project.

        Returns
        -------
        string
        """
        return self.list_projects()
    
    def import_project(self) -> str:
        """Url to create a new project.

        Returns
        -------
        string
        """
        return self.base_url + '/projects/import'

    def delete_project(self, project_id: str) -> str:
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

    def get_project(self, project_id: str) -> str:
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

    def list_projects(self) -> str:
        """Url to retrieve the list of active projects.

        Returns
        -------
        string
        """
        return self.base_url + '/projects'

    def update_project(self, project_id: str) -> str:
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
    def create_branch(self, project_id: str) -> str:
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

    def delete_branch(self, project_id: str, branch_id: str) -> str:
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

    def get_branch(self, project_id: str, branch_id: str) -> str:
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

    def get_branch_head(self, project_id: str, branch_id: str) -> str:
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

    def update_branch(self, project_id: str, branch_id: str) -> str:
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
    def cancel_workflow(self, project_id: str, branch_id: str) -> str:
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

    def get_workflow(self, project_id: str, branch_id: str, workflow_id: str) -> str:
        """Url to get the handle for a specified workflow.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier
        workflow_id: string
            Unique workflow identifier

        Returns
        -------
        string
        """
        return self.get_branch(project_id, branch_id) + '/workflows/' + workflow_id

    def get_workflow_module(self, project_id: str, branch_id: str, module_id: str) -> str:
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

    # --------------------------------------------------------------------------
    # Module
    # --------------------------------------------------------------------------
    def workflow_module_append(self, project_id: str, branch_id: str) -> str:
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

    def workflow_module_delete(self, project_id: str, branch_id: str, module_id: str) -> str:
        """Url to delete a module in the head workflow of a given branch.
        Modules can only be deleted from the branch head. Therefore, the
        workflow identifier is not needed to identify the target workflow.

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
        return self.get_workflow_module(project_id, branch_id, module_id)

    def workflow_module_insert(self, project_id: str, branch_id: str, module_id: str) -> str:
        """Url to insert a module to a given branch before the module with the
        given module identifier. Modules can only be inserted into the branch
        head. Therefore, the workflow identifier is not needed to identify the
        target workflow.

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
        return self.get_workflow_module(project_id, branch_id, module_id)

    def workflow_module_replace(self, project_id: str, branch_id: str, module_id: str) -> str:
        """Url to replace a module in the head workflow of a given branch.
        Modules can only be replaced in the branch head. Therefore, the
        workflow identifier is not needed to identify the target workflow.

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
        return self.get_workflow_module(project_id, branch_id, module_id)

    # --------------------------------------------------------------------------
    # Datasets
    # --------------------------------------------------------------------------
    def create_dataset(self, project_id: str) -> str:
        """Url to create a new dataset.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        string
        """
        return self.get_project(project_id) + '/datasets'

    def dataset_pagination(self, project_id: str, dataset_id: str, offset: int = 0, limit: Optional[int] = None) -> str:
        """Get Url for dataset row pagination.

        Parameters
        ----------
        project_id: string
            Unique project identifier
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
        query = format_args([
            (PAGE_OFFSET, offset),
            (PAGE_LIMIT, limit)
        ])
        return self.get_dataset(project_id, dataset_id) + query

    def download_dataset(self, project_id: str, dataset_id: str) -> str:
        """Url to download a dataset in csv format.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        dataset_id: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.get_dataset(project_id, dataset_id) + '/csv'

    def get_dataset(self, project_id: str, dataset_id: str, force_profiler: Optional[bool] = None) -> str:
        """Url to retrieve dataset rows.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        dataset_id: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        url = self.get_project(project_id) + '/datasets/' + dataset_id
        if force_profiler is not None and force_profiler:
            url += "?{}=true".format(FORCE_PROFILER)
        return url

    def get_dataset_caveats(self, project_id: str, dataset_id: str, column_id: Optional[int] = None, row_id: Optional[str]=None) -> str:
        """Url to retrieve dataset annotations.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        dataset_id: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        args = format_args([
                    (labels.COLUMN, column_id), 
                    (labels.ROW, row_id)
                ])
        return self.get_dataset(project_id, dataset_id) + '/annotations' + args

    def get_dataset_descriptor(self, project_id: str, dataset_id: str) -> str:
        """Url to retrieve dataset descriptor.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        dataset_id: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.get_dataset(project_id, dataset_id) + '/descriptor'

    def get_dataset_profiling(self, project_id:str, dataset_id:str) -> str:
        """Url to retrieve dataset profiling results.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        dataset_id: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.get_dataset(project_id, dataset_id) + '/profiling'

    # --------------------------------------------------------------------------
    # Charts
    # --------------------------------------------------------------------------
    def get_chart_view(self, project_id: str, branch_id: str, workflow_id: str, module_id: str, chart_id: str) -> str:
        """Get chart view result for a given workflow module.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier
        workflow_id: string
            Unique workflow identifier
        module_id: string
            Unique module identifier
        chart_id: string
            Unique chart identifier

        Returns
        -------
        string

        Returns
        -------
        string
        """
        url = self.get_workflow(project_id, branch_id, workflow_id)
        return url + '/modules/' + module_id + '/charts/' + chart_id


    # --------------------------------------------------------------------------
    # Files
    # --------------------------------------------------------------------------
    def download_file(self, project_id: str, file_id: str) -> str:
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
        return self.upload_file(project_id) + '/' + file_id

    def upload_file(self, project_id: str) -> str:
        """File upload url for the given project.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        string
        """
        return self.get_project(project_id) + '/files'


def format_args(args: List[Tuple[str, Any]]) -> str:
    args = [ arg for arg in args if arg[1] is not None ]
    if len(args) == 0:
        return ""
    return "?"+"&".join(
            "{}={}".format(arg, value) 
            for arg, value in args
            if value is not None
        )