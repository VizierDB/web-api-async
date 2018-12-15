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

"""Filestore factory implementation for the default filestore."""

import os
import shutil

from vizier.filestore.factory import FilestoreFactory
from vizier.filestore.fs.base import DefaultFilestore, PARA_DIRECTORY


class DefaultFilestoreFactory(FilestoreFactory):
    """Filestore factory implementation for the default filestore."""
    def __init__(self, properties):
        """Initialize the reference to the base directory that contains all
        filestore folders.

        Expects a dictionary with a single entry that contains the base path
        for all created datastores.

        Parameters
        ----------
        properties: dict
            Dictionary of configuration properties
        """
        self.base_path = os.path.abspath(properties[PARA_DIRECTORY])

    def delete_filestore(self, identifier):
        """Delete a filestore. This method is normally called when the project
        with which the filestore is associated is deleted.

        Deletes the directory and all subfolders that contain the filestore
        resources.

        Parameters
        ----------
        identifier: string
            Unique identifier for filestore
        """
        filestore_dir = os.path.join(self.base_path, identifier)
        if os.path.isdir(filestore_dir):
            shutil.rmtree(filestore_dir)

    def get_filestore(self, identifier):
        """Get the filestore instance for the project with the given identifier.

        Paramaters
        ----------
        identifier: string
            Unique identifier for filestore

        Returns
        -------
        vizier.filestore.fs.base.DefaultFilestore
        """
        filestore_dir = os.path.join(self.base_path, identifier)
        return DefaultFilestore(filestore_dir)
