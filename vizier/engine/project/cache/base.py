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

"""The project cache is an in-memory cache for project handles. Different
configurations of the vizier engine may have different implementations for the
project handle and project cache.
"""

from abc import abstractmethod


class ProjectCache(object):
    """The project cache is an im-memory cache of project wrappers around the
    viztrails in a viztrails repository.
    """
    @abstractmethod
    def create_project(self, properties=None):
        """Create a new project. Will create a viztrail in the underlying
        viztrail repository. The initial set of properties is an optional
        dictionary of (key,value)-pairs where all values are expected to either
        be scalar values or a list of scalar values. The properties are passed
        to the create method for the viztrail repository.

        Returns the handle for the created project.

        Parameters
        ----------
        properties: dict, optional
            Set of properties for the new viztrail

        Returns
        -------
        vizier.engine.project.base.ProjectHandle
        """
        raise NotImplementedError

    @abstractmethod
    def delete_project(self, project_id):
        """Delete all resources that are associated with the given project.
        Returns True if the project existed and False otherwise.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @abstractmethod
    def get_branch(self, project_id, branch_id):
        """Get the branch with the given identifier for the specified project.
        The result is None if the project of branch does not exist.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier

        Returns
        -------
        vizier.viztrail.branch.BranchHandle
        """
        raise NotImplementedError

    @abstractmethod
    def get_project(self, project_id):
        """Get the handle for the project with the given identifier. Returns
        None if the project does not exist.

        Parameters
        ---------
        project_id: string
            Unique project identifier

        Returns
        -------
        vizier.engine.project.base.ProjectHandle
        """
        raise NotImplementedError

    @abstractmethod
    def list_projects(self):
        """Get a list of handles for all projects.

        Returns
        -------
        list(vizier.engine.project.base.ProjectHandle)
        """
        raise NotImplementedError
