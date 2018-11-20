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
separate files on the file system.
"""

import os

from vizier.core.util import get_unique_identifier
from vizier.core.system import build_info
from vizier.viztrail.driver.fs import VERSION_INFO
from vizier.viztrail.driver.fs.io import DefaultObjectStore
from vizier.viztrail.driver.fs.viztrail import FSViztrailHandle
from vizier.viztrail.repository import ViztrailRepository


class FSViztrailRepository(ViztrailRepository):
    """Repository for viztrails. This implementation maintains all resources
    that are managed by the repository as files on the file system. Expects the
    path to a directory on the local disk that contains the files for all
    managed resources.

    Files and Directories
    ---------------------
    /viztrails/                 : Directory for viztrails
    /viztrails/<vt-identifier>/ : Resources for individual viztrail
    """
    def __init__(self, base_dir, object_store=None):
        """Initialize the repository. Expects the path to a local directory that
        contains all resources.

        Parameters
        ---------
        base_dir: string
            Path to base directory containing all repository resources
        object_store: vizier.viztrail.driver.fs.io.ObjectStore, optional
            Object store implementation to read and write Json files
        """
        # Initialize the base directory and object store
        self.base_dir = os.path.abspath(base_dir)
        self.object_store = object_store if not object_store is None else DefaultObjectStore()
        # Load viztrails and intialize the remaining instance variables by
        # calling the constructor of the super class
        super(FSViztrailRepository, self).__init__(
            build=build_info(
                "vizier.viztrail.driver.fs.repository.FSViztrailRepository",
                version_info=VERSION_INFO
            ),
            viztrails=FSViztrailRepository.load_repository(
                repo_dir=DIR_VIZTRAILS(self.base_dir),
                object_store=self.object_store
            )
        )

    def create_viztrail(self, exec_env_id, properties=None):
        """Create a new viztrail. Every viztrail is associated with an execution
        environment. The environment is set when the viztrail is created and
        can not change throughout the life-cycle of the viztrail.

        Parameters
        ----------
        exec_env_id: string
            Identifier of the execution environment that is used for the
            viztrail
        properties: dict, optional
            Set of properties for the new viztrail

        Returns
        -------
        vizier.viztrail.base.ViztrailHandle
        """
        # Get unique identifier for new viztrail and viztrail directory. Raise
        # runtime error if the returned identifier is not unique.
        identifier = get_unique_identifier()
        if identifier in self.viztrails:
            raise RuntimeError('non-unique identifier \'' + str(identifier) + '\'')
        viztrail_dir = os.path.join(DIR_VIZTRAILS(self.base_dir), identifier)
        # Create materialized viztrail resource
        vt = FSViztrailHandle.create_viztrail(
            identifier=identifier,
            exec_env_id=exec_env_id,
            properties=properties,
            base_dir=viztrail_dir,
            object_store=self.object_store
        )
        # Add the new resource to the viztrails index and return the new
        # viztrail handle
        self.viztrails[identifier] = vt
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
            # Call the delete method of the FSViztrailHandle to delete the
            # files that are associated with the viztrail
            self.viztrails[viztrail_id].delete_viztrail()
            del self.viztrails[viztrail_id]
            return True
        else:
            return False

    @staticmethod
    def load_repository(repo_dir, object_store=None):
        """Load viztrails from disk. Each subfolder in the given directory
        references an active viztrail.

        If the repository directory does not exist it is created.

        Parameter
        ---------
        repo_dir: string
            Path to the base directory that contains the viztrail resources
        object_store: vizier.viztrail.driver.fs.io.ObjectStore, optional
            Object store implementation to read and write Json files

        Returns
        -------
        list(vizier.viztrail.driver.fs.viztrail.FSViztrailHandle)
        """
        viztrails = list()
        if not os.path.isdir(repo_dir):
            # Create base directory if it doesn't exist and return an empty
            # dictionary
            os.makedirs(repo_dir)
        else:
            # Each subfolder in the repository is expected to contain resources
            # for a viztrail
            for filename in os.listdir(repo_dir):
                viztrail_dir = os.path.join(repo_dir, filename)
                if os.path.isdir(viztrail_dir):
                    viztrails.append(
                        FSViztrailHandle.load_viztrail(
                            base_dir=viztrail_dir,
                            object_store=object_store
                        )
                    )
        return viztrails


# ------------------------------------------------------------------------------
# Files and Directories
# ------------------------------------------------------------------------------

def DIR_VIZTRAILS(base_dir):
    """Get the absolute path to the subfolder containing viztrail resources.

    Parameters
    ----------
    base_dir: string
        Path to the base directory for the repository

    Returns
    -------
    string
    """
    return os.path.join(base_dir, 'viztrails')
