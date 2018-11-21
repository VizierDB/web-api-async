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

"""In-memory object store. This class is primarily used for test purposes."""

from vizier.core.io.base import ObjectStore, MAX_ATTEMPS
from vizier.core.util import init_value, get_unique_identifier


class MemObjectStore(ObjectStore):
    """Implements an in-memory object store. Maintains all object in a
    dictionary indexed by their object path.
    """
    def __init__(self, identifier_factory=None, keep_deleted_files=False):
        """Initialize the identifier_factory and keep_deleted_files flag. By
        default the get_unique_identifier function is used to generate new
        folder and resource identifier.

        Parameters
        ----------
        identifier_factory: func, optional
            Function to create a new unique identifier
        keep_deleted_files: bool, optional
            Flag indicating whether files and folder are actually deleted or not
        """
        self.store = dict()
        self.folders = dict()
        self.identifier_factory = init_value(identifier_factory, get_unique_identifier)
        self.keep_deleted_files = keep_deleted_files

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

    def create_object(self, parent_folder, identifier=None, content=None, suffix=None):
        """Create a new file in the given parent folder. The file name is
        either given as the identifier argument or a new unique identifier is
        created if the argument is None. in the latter case the file name will
        be the concatenation of identifier and suffix. the newReturns the
        identifier for the created file.

        Parameters
        ----------
        parent_folder: string
            Path to parent folder
        identifier: string, optional
            Folder identifier
        suffix: string, optional
            File name suffix

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
            if not suffix is None:
                object_path += suffix
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
