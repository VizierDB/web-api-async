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

"""Datastore factory implementation for the file system based datastore."""

import os
import shutil

from vizier.datastore.factory import DatastoreFactory
from vizier.datastore.fs.base import FileSystemDatastore


"""Configuration parameter."""
PARA_DIRECTORY = 'directory'


class FileSystemDatastoreFactory(DatastoreFactory):
    """Datastore factory for file system based datastores."""
    def __init__(self, base_path=None, properties=None):
        """Initialize the reference to the base directory that contains all
        datastore folders.

        Expects a base path or a dictionary with a single entry that contains
        the base path for all created datastores. Raises ValueError if no base
        path is given.

        Parameters
        ----------
        base_path: string, optional
            Datastore base path
        properties: dict, optional
            Dictionary of configuration properties
        """
        self.base_path = base_path
        if not properties is None:
            self.base_path = os.path.abspath(properties[PARA_DIRECTORY])
        if self.base_path is None:
            raise ValueError('no base path given')

    def delete_datastore(self, identifier):
        """Delete a datastore. This method is normally called when the project
        with which the datastore is associated is deleted.

        Deletes the directory and all subfolders that contain the datastore
        resources.

        Parameters
        ----------
        identifier: string
            Unique identifier for datastore
        """
        datastore_dir = os.path.join(self.base_path, identifier)
        if os.path.isdir(datastore_dir):
            shutil.rmtree(datastore_dir)

    def get_datastore(self, identifier):
        """Get the datastore instance for the project with the given identifier.

        Paramaters
        ----------
        identifier: string
            Unique identifier for datastore

        Returns
        -------
        vizier.datastore.base.Datastore
        """
        datastore_dir = os.path.join(self.base_path, identifier)
        return FileSystemDatastore(datastore_dir)
