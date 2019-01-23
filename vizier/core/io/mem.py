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

"""In-memory object store. This class is primarily used for test purposes."""

from vizier.core.io.base import ObjectStore, MAX_ATTEMPS
from vizier.core.io.base import PARA_KEEP_DELETED, PARA_LONG_IDENTIFIER
from vizier.core.util import get_short_identifier, get_unique_identifier


class MemObjectStore(ObjectStore):
    """Implements an in-memory object store. Maintains all object in a
    dictionary indexed by their object path.
    """
    def __init__(self, properties=None, identifier_factory=None, keep_deleted_files=False):
        """Initialize the identifier_factory and keep_deleted_files flag. By
        default the get_unique_identifier function is used to generate new
        folder and resource identifier.

        Parameters
        ----------
        properties: dict
            Dictionary for object properties. Overwrites the default values.
        identifier_factory: func, optional
            Function to create a new unique identifier
        keep_deleted_files: bool, optional
            Flag indicating whether files and folder are actually deleted or not
        """
        self.store = dict()
        self.folders = dict()
        self.identifier_factory = identifier_factory if not identifier_factory is None else get_unique_identifier
        self.keep_deleted_files = keep_deleted_files
        if not properties is None:
            if PARA_KEEP_DELETED in properties:
                self.keep_deleted_files = properties[PARA_KEEP_DELETED]
            if PARA_LONG_IDENTIFIER in properties and not properties[PARA_LONG_IDENTIFIER]:
                self.identifier_factory = get_short_identifier

    def create_folder(self, parent_folder, identifier=None):
        """Create a new folder in the given parent folder. The folder name is
        either given as the identifier argument or a new unique identifier is
        created if the argument is None. Returns the identifier for the created
        folder.

        Parameters
        ----------
        parent_folder: string
            Path to parent folder
        identifier: string, optional
            Folder identifier

        Returns
        -------
        string
        """
        count = 0
        while identifier is None:
            # Allow repeated calls to the identifier factory until an identifier
            # is returned that does not reference an existing folder. The max.
            # attemps counter is used to avoid an endless loop.
            candidate = self.identifier_factory()
            if not self.exists(self.join(parent_folder, candidate)):
                identifier = candidate
            else:
                count += 1
                if count >= MAX_ATTEMPS:
                    raise RuntimeError('could not generate unique identifier')
        # Create the new folder and return the identifier
        self.folders[self.join(parent_folder, identifier)] = None
        return identifier

    def create_object(self, parent_folder, identifier=None, content=None):
        """Create a new file in the given parent folder. The file name is
        either given as the identifier argument or a new unique identifier is
        created if the argument is None. Returns the identifier for the created
        object.

        Parameters
        ----------
        parent_folder: string
            Path to parent folder
        identifier: string, optional
            Folder identifier

        Returns
        -------
        string
        """
        count = 0
        object_path = identifier
        while identifier is None:
            # Allow repeated calls to the identifier factory until an identifier
            # is returned that does not reference an existing folder. The max.
            # attemps counter is used to avoid an endless loop.
            candidate = self.identifier_factory()
            object_path = candidate
            if not self.exists(self.join(parent_folder, object_path)):
                identifier = candidate
            else:
                count += 1
                if count >= MAX_ATTEMPS:
                    raise RuntimeError('could not generate unique identifier')
        # Create an empty file
        self.store[self.join(parent_folder, object_path)] = content
        return identifier

    def delete_folder(self, folder_path, force_delete=False):
        """Delete the folder with the given path and all of its files and
        subfolders.

        Parameters
        ----------
        folder_path: string
            Path to the folder that is being deleted
        force_delete: bool, optional
            Force deletion of the resource
        """
        if force_delete or not self.keep_deleted_files:
            if folder_path in self.folders:
                del self.folders[folder_path]
                subfolders = set()
                for folder in self.folders:
                    if folder.startswith(folder_path + '::'):
                        subfolders.add(folder)
                for folder in subfolders:
                    del self.folders[folder]
                objects = set()
                for object_path in self.store:
                    if object_path.startswith(folder_path + '::'):
                        objects.add(object_path)
                for object_path in objects:
                    del self.store[object_path]

    def delete_object(self, object_path, force_delete=False):
        """Delete the object with the given path.

        Parameters
        ----------
        object_path: string
            Path to the resource that is being deleted
        force_delete: bool, optional
            Force deletion of the resource
        """
        if force_delete or not self.keep_deleted_files:
            if object_path in self.store:
                del self.store[object_path]

    def exists(self, resource_path):
        """Returns True if a resource at the given path exists.

        Parameters
        ----------
        resource_path: string
            Path to resource

        Returns
        -------
        bool
        """
        return resource_path in self.store or resource_path in self.folders

    def join(self, parent_folder, identifier):
        """Concatenate the identifier for a given folder and a folder resource.

        Parameters
        ----------
        parent_folder: string
            Path to the parent folder
        identifier: string
            Identifier for resource in the parent folder

        Returns
        -------
        string
        """
        return parent_folder + '::' + identifier

    def list_folders(self, parent_folder, create=True):
        """Get a list of all subfolders in the given folder. If the folder does
        not exist it is created if the create flag is True.

        Parameters
        ----------
        parent_folder: string
            Path to the parent folder
        create: bool, optional
            Flag indicating that the parent folder should be created if it does
            not exist
        """
        result = list()
        if not parent_folder in self.folders:
            # Create the folder if the create flag is True.
            if create:
                self.folders[parent_folder] = None
        else:
            for folder in self.folders:
                if folder.startswith(parent_folder + '::'):
                    folder_id = folder[len(parent_folder + '::'):]
                    if not '::' in folder_id:
                        result.append(folder_id)
        return result

    def list_objects(self, folder_path):
        """Get a list of all objects in the given folder. Returns a list of
        resource names.

        Parameters
        ----------
        folder_path: string
            Path to the resource folder

        Returns
        -------
        list(string)
        """
        result = list()
        for object_path in self.store:
            if object_path.startswith(folder_path + '::'):
                resource_name = object_path[len(folder_path)+2:]
                if not '::' in resource_name:
                    result.add(resource_name)

    def read_object(self, object_path):
        """Read Json document from given path.

        Parameters
        ----------
        object_path: string
            Path identifier for a resource object

        Returns
        -------
        dict or list
        """
        return self.store[object_path]

    def write_object(self, object_path, content):
        """Write content as Json document to given path.

        Parameters
        ----------
        object_path: string
            Path identifier for a resource object
        content: dict or list
            Json object or array
        """
        self.store[object_path] = content
