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

"""Vizier Filestore API - Implements all methods of the API to interact with
the filestores that are associated with vizier projects.
"""

import vizier.api.serialize.files as serialize


class VizierFilestoreApi(object):
    """The Vizier filestore API implements the methods that correspond to
    requests that upload and download files.
    """
    def __init__(self, projects, urls):
        """Initialize the API components.

        Parameters
        ----------
        projects: vizier.engine.project.cache.base.ProjectCache
            Cache for project handles
        urls: vizier.api.routes.base.UrlFactory
            Factory for resource urls
        """
        self.projects = projects
        self.urls = urls

    def get_file(self, project_id, file_id):
        """Get the handle for given file. The result is None if the project or
        file are unknown.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        file_id: string
            Unique file identifier

        Returns
        -------
        vizier.filestore.base.FileHandle
        """
        # Retrieve the project from the repository to ensure that it exists
        project = self.projects.get_project(project_id)
        if project is None:
            return None
        return project.filestore.get_file(file_id)

    def upload_file(self, project_id, file, file_name):
        """Upload file to the filestore that is associated with the given
        project. The file is uploaded from a given file stream.

        Returns serialization of the handle for the created file or None if the
        project is unknown.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        file: werkzeug.datastructures.FileStorage
            File object (e.g., uploaded via HTTP request)
        file_name: string
            Name of the file

        Returns
        -------
        vizier.filestore.base.FileHandle
        """
        # Retrieve the project from the repository to ensure that it exists
        project = self.projects.get_project(project_id)
        if project is None:
            return None
        # Upload file to projects filestore and return a serialization of the
        # returned file handle
        f_handle = project.filestore.upload_stream(
            file=file,
            file_name=file_name
        )
        return serialize.FILE_HANDLE(
            f_handle=f_handle,
            project=project,
            urls=self.urls
        )
