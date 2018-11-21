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

"""Definition and implementation for different object stores that read and
write resources as Json objects.
"""

from abc import abstractmethod

import json
import os
import shutil

from vizier.core.util import init_value, get_unique_identifier


"""Maximium nuber attempts to generate a unique identifier before an exception
is raised.
"""
MAX_ATTEMPS = 100


class ObjectStore(object):
    """Abstract object store class that defines the interface methods to read
    and write objects and to maintain folders.
    """

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def create_object(self, parent_folder, identifier=None, suffix=None, content=None):
        """Create a new object in the given parent folder. The object path is
        either given as the identifier argument or a new unique identifier is
        created if the argument is None. in the latter case the object name will
        be the concatenation of identifier and suffix. Returns the path for the
        created object.

        Parameters
        ----------
        parent_folder: string
            Path to parent folder
        identifier: string, optional
            Folder identifier
        suffix: string, optional
            File name suffix
        content: list or dict, optional
            Default content for the new resource

        Returns
        -------
        string
        """
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def delete_object(self, object_path, force_delete=False):
        """Delete the object with the given path.

        Parameters
        ----------
        file_path: string
            Path to the object that is being deleted
        force_delete: bool, optional
            Force deletion of the resource
        """
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def list_folders(self, folder_path, create=True):
        """Get a list of all subfolders in the given folder. If the folder does
        not exist it is created if the create flag is True.

        Parameters
        ----------
        folder_path: string
            Path to the parent folder
        create: bool, optional
            Flag indicating that the parent folder should be created if it does
            not exist

        Returns
        -------
        list(string)
        """
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def write_object(self, object_path, content):
        """Write content as Json document to given path.

        Parameters
        ----------
        object_path: string
            Path identifier for a resource object
        content: dict or list
            Json object or array
        """
        raise NotImplementedError


class DefaultObjectStore(ObjectStore):
    """Default implementation of the object store. If the keep_deleted_files
    flag is set to True none of the delete methods will have any effect. The
    flag allows to switch between scenarios where we want to keep the full
    history of any resource that was ever created.
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
            if not os.path.exists(os.path.join(parent_folder, candidate)):
                identifier = candidate
            else:
                count += 1
                if count >= MAX_ATTEMPS:
                    raise RuntimeError('could not generate unique identifier')
        # Create the new folder and return the identifier
        os.makedirs(os.path.join(parent_folder, identifier))
        return identifier

    def create_object(self, parent_folder, identifier=None, suffix=None, content=None):
        """Create a new object in the given parent folder. The object path is
        either given as the identifier argument or a new unique identifier is
        created if the argument is None. in the latter case the object name will
        be the concatenation of identifier and suffix. Returns the path for the
        created object.

        Parameters
        ----------
        parent_folder: string
            Path to parent folder
        identifier: string, optional
            Folder identifier
        suffix: string, optional
            File name suffix
        content: list or dict, optional
            Default content for the new resource

        Returns
        -------
        string
        """
        count = 0
        filename = identifier
        while identifier is None:
            # Allow repeated calls to the identifier factory until an identifier
            # is returned that does not reference an existing folder. The max.
            # attemps counter is used to avoid an endless loop.
            candidate = self.identifier_factory()
            filename = candidate
            if not suffix is None:
                filename += suffix
            if not os.path.exists(os.path.join(parent_folder, filename)):
                identifier = candidate
            else:
                count += 1
                if count >= MAX_ATTEMPS:
                    raise RuntimeError('could not generate unique identifier')
        # Create an empty file
        file_path = os.path.join(parent_folder, filename)
        if not content is None:
            self.write_object(object_path=file_path, content=content)
        else:
            with open(file_path, 'w') as f:
                pass
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
            shutil.rmtree(folder_path)

    def delete_object(self, object_path, force_delete=False):
        """Delete the object with the given path.

        Parameters
        ----------
        file_path: string
            Path to the object that is being deleted
        force_delete: bool, optional
            Force deletion of the resource
        """
        if force_delete or not self.keep_deleted_files:
            os.remove(object_path)

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
        return os.path.exists(resource_path)

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
        return os.path.join(parent_folder, identifier)

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
        if not os.path.exists(parent_folder):
            # Create the folder if the create flag is True. The result is an
            # empty list.
            if create:
                os.makedirs(parent_folder)
        else:
            for filename in os.listdir(parent_folder):
                if os.path.isdir(os.path.join(parent_folder, filename)):
                    result.append(filename)
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
        with open(object_path, 'r') as f:
            return json.load(f)

    def write_object(self, object_path, content):
        """Write content as Json document to given path.

        Parameters
        ----------
        object_path: string
            Path identifier for a resource object
        content: dict or list
            Json object or array
        """
        with open(object_path, 'w') as f:
            json.dump(content, f)
