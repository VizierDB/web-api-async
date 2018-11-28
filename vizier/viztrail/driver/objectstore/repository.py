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

"""Implementation for a viztrail repository that maintains all resources as
objects and folders in an object store.

The default implentation for the object store will maintain all resources as
directories and files on the file system. Other implementations might maintain
the resources as documents in a document store.
"""

from vizier.core.io.base import DefaultObjectStore
from vizier.core.util import get_short_identifier, get_unique_identifier
from vizier.core.system import build_info
from vizier.core.util import init_value
from vizier.viztrail.driver.objectstore import VERSION_INFO
from vizier.viztrail.driver.objectstore.viztrail import OSViztrailHandle
from vizier.viztrail.repository import ViztrailRepository


"""Configuration parameter."""
PARA_DIRECTORY = 'directory'
PARA_KEEP_DELETED = 'keepDeletedFiles'
PARA_LONG_IDENTIFIER = 'useLongIdentifier'


"""Resource identifier"""
OBJ_VIZTRAILINDEX = 'viztrails'


class OSViztrailRepository(ViztrailRepository):
    """Repository for viztrails. This implementation maintains all resources
    that are managed by the repository as objects in an object store. The base
    path is the identifier prefix for all managed resources.

    By default all resources are mantained as directories and files on the local
    file system. The viztrails index is a list object that contains the
    identifier of active viztrails.

    Folders and Resources
    ---------------------
    viztrails        : List of active viztrails
    <vt-identifier>/ : Folder with resources for individual viztrail
    """
    def __init__(self, base_path, object_store=None):
        """Initialize the repository. Expects the path to a local directory that
        contains all resources.

        Parameters
        ---------
        base_path: string
            Identifier prefix for all resources
        object_store: vizier.core.io.base.ObjectStore, optional
            Object store implementation to create, delete, read, and write
            resource objects
        """
        # Initialize the base directory, object store, and the identifier for
        # the viztrails index resource identifier. By default all resources are
        # maintained on the file system.
        self.base_path = base_path
        self.object_store = init_value(object_store, DefaultObjectStore())
        self.viztrails_index = self.object_store.join(self.base_path, OBJ_VIZTRAILINDEX)
        # Create the index file if it does not exist
        if not self.object_store.exists(self.viztrails_index):
            self.object_store.create_object(
                parent_folder=self.base_path,
                identifier=OBJ_VIZTRAILINDEX,
                content=list()
            )
        # Load viztrails and intialize the remaining instance variables by
        # calling the constructor of the super class
        super(OSViztrailRepository, self).__init__(
            build=build_info(
                "vizier.viztrail.driver.objectstore.viztrail.repository.OSViztrailRepository",
                version_info=VERSION_INFO
            ),
            viztrails=OSViztrailRepository.load_repository(
                base_path=self.base_path,
                viztrails_index=self.viztrails_index,
                object_store=self.object_store
            )
        )

    def create_viztrail(self, properties=None):
        """Create a new viztrail. The initial set of properties is an optional
        dictionary of (key,value)-pairs where all values are expected to either
        be scalar values or a list of scalar values.

        Parameters
        ----------
        properties: dict, optional
            Set of properties for the new viztrail

        Returns
        -------
        vizier.viztrail.driver.objectstore.viztrail.OSViztrailHandle
        """
        # Get unique identifier for new viztrail and viztrail directory. Raise
        # runtime error if the returned identifier is not unique.
        identifier = self.object_store.create_folder(parent_folder=self.base_path)
        viztrail_path = self.object_store.join(self.base_path, identifier)
        # Create materialized viztrail resource
        vt = OSViztrailHandle.create_viztrail(
            identifier=identifier,
            properties=properties,
            base_path=viztrail_path,
            object_store=self.object_store
        )
        # Add the new resource to the viztrails index. Write updated index to
        # object store before returning the new viztrail handle
        self.viztrails[vt.identifier] = vt
        self.object_store.write_object(
            object_path=self.viztrails_index,
            content=[vt_id for vt_id in self.viztrails]
        )
        return vt

    def delete_viztrail(self, viztrail_id):
        """Delete the viztrail with given identifier. The result is True if a
        viztrail with the given identifier existed, False otherwise.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier

        Returns
        -------
        bool
        """
        # Get the viztrail handle if it exists
        if viztrail_id in self.viztrails:
            # Call the delete method of the OSViztrailHandle to delete the
            # files that are associated with the viztrail
            self.viztrails[viztrail_id].delete_viztrail()
            # Remove viztrail from the internal cache and write the updated
            # viztrails index
            del self.viztrails[viztrail_id]
            self.object_store.write_object(
                object_path=self.viztrails_index,
                content=[vt for vt in self.viztrails]
            )
            return True
        else:
            return False

    @staticmethod
    def init(properties):
        """Create an instance of the viztrails repository from a given
        dictionary of configuration arguments. At this point only the base
        directory is expected as to be present in the properties dictionary.

        Parameter
        ---------
        properties: dict()
            Dictionary of configuration arguments

        Returns
        -------
        vizier.viztrail.driver.objectstore.repository.OSViztrailRepository
        """
        # Raise an exception if the pase directory argument is not given
        if not PARA_DIRECTORY in properties:
            raise ValueError('missing value for argument \'' + PARA_DIRECTORY + '\'')
        # If the keep deleted files argument is given use it. By default all
        # files are deleted and not kept.
        if PARA_KEEP_DELETED in properties:
            keep_deleted = properties[PARA_KEEP_DELETED]
        else:
            keep_deleted = False
        # By default a factory for short identifier is used
        if PARA_LONG_IDENTIFIER in properties and properties[PARA_LONG_IDENTIFIER]:
            identifier_factory = get_unique_identifier
        else:
            identifier_factory = get_short_identifier
        return OSViztrailRepository(
            base_path=properties[PARA_DIRECTORY],
            object_store=DefaultObjectStore(
                identifier_factory=identifier_factory,
                keep_deleted_files=keep_deleted
            )
        )

    @staticmethod
    def load_repository(base_path, viztrails_index, object_store=None):
        """Load viztrails resources. Each entry in the list identified by the
        viztrails_index argument is expected to identify a subfolder in the
        given parent folder that contains a viztrail resource.

        If the parent folder does not exist it is created.

        Parameter
        ---------
        viztrails_folder: string
            Identifier for the folder containing the viztrails as subsolders
        object_store: vizier.core.io.base.ObjectStore, optional
            Object store implementation to access and maintain resources

        Returns
        -------
        list(vizier.viztrail.driver.objectstore.viztrail.OSViztrailHandle)
        """
        viztrails = list()
        for identifier in object_store.read_object(viztrails_index):
            viztrails.append(
                OSViztrailHandle.load_viztrail(
                    base_path=object_store.join(base_path, identifier),
                    object_store=object_store
                )
            )
        return viztrails
