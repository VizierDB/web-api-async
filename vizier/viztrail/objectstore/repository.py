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

"""Implementation for a viztrail repository that maintains all resources as
objects and folders in an object store.

The default implentation for the object store will maintain all resources as
directories and files on the file system. Other implementations might maintain
the resources as documents in a document store.
"""

import os

from vizier.core.io.base import DefaultObjectStore
from vizier.core.loader import ClassLoader
from vizier.core.util import init_value
from vizier.viztrail.objectstore.viztrail import OSViztrailHandle
from vizier.viztrail.repository import ViztrailRepository


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
        """Initialize the repository from a configuration dictionary. Expects
        a dictionary that contains at least the base path for the repository.
        The definition of the object store is optional. If none is given the
        default object store will be used.

        Parameters
        ---------
        base_path: string
            Path to the base directory for viztrail resources
        object_store: vizier.core.io.base.ObjectStore, optional
            Store for objects that represent viztrail resources
            not
        """
        # Raise an exception if the base directory argument is not given
        if base_path is None:
            raise ValueError('missing path for base directory')
        # Create the base directory if it does not exist
        self.base_path = base_path
        if not os.path.isdir(self.base_path):
            os.makedirs(self.base_path)
        # The object store element is optional. If not given the default object
        # store is used.
        if not object_store is None:
            self.object_store = object_store
        else:
            self.object_store = DefaultObjectStore()
        # Initialize the viztrails index. Create the index file if it does not
        # exist.
        self.viztrails_index = self.object_store.join(
            self.base_path,
            OBJ_VIZTRAILINDEX
        )
        if not self.object_store.exists(self.viztrails_index):
            self.object_store.create_object(
                parent_folder=self.base_path,
                identifier=OBJ_VIZTRAILINDEX,
                content=list()
            )
        # Load viztrails and intialize the remaining instance variables by
        # calling the constructor of the super class
        self.viztrails = dict()
        for identifier in self.object_store.read_object(self.viztrails_index):
            vt = OSViztrailHandle.load_viztrail(
                base_path=self.object_store.join(self.base_path, identifier),
                object_store=self.object_store
            )
            self.viztrails[vt.identifier] = vt

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
        vizier.viztrail.objectstore.viztrail.OSViztrailHandle
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

    def get_viztrail(self, viztrail_id):
        """Retrieve the viztrail with the given identifier. The result is None
        if no viztrail with given identifier exists.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier

        Returns
        -------
        vizier.viztrail.base.ViztrailHandle
        """
        if viztrail_id in self.viztrails:
            return self.viztrails[viztrail_id]
        else:
            return None

    def list_viztrails(self):
        """List handles for all viztrails in the repository.

        Returns
        -------
        list(vizier.viztrail.base.ViztrailHandle)
        """
        return self.viztrails.values()
