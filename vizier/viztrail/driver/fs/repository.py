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
from vizier.viztrail.driver.fs.viztrail import FSViztrailHandle
from vizier.viztrail.repository import ViztrailRepository


"""Name of the index file containing the list of active viztrails."""
INDEX_FILE = 'index.tsv'


class FSViztrailRepository(ViztrailRepository):
    """Repository for viztrails. This implementation maintains all resources
    that are managed by the repository as files on the file system. Expects the
    path to a directory on the local disk that contains the files for all
    managed resources.

    Maintains the identifier of active viztrails in a simple text file
    containing one identifier per line.
    """
    def __init__(self, base_dir):
        """Initialize the repository. Expects the path to a local directory that
        contains all resources.

        Parameters
        ---------
        base_dir: string
            Path to base directory containing all repository resources
        """
        # Make sure to initialize the base directory first before loading the
        # viztrails
        self.base_dir = base_dir
        # Load viztrails and intialize the remaining instance variables by
        # calling the constructor of the super class
        super(FSViztrailRepository, self).__init__(
            build=build_info(
                "vizier.viztrail.driver.fs.repository.FSViztrailRepository",
                version_info=VERSION_INFO
            ),
            viztrails=FSViztrailRepository.load(self.index_file())
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
        # Get unique identifier for new viztrail and create viztrail directory.
        # This will raise a runtime error if the returned identifier is not
        # unique.
        identifier = get_unique_identifier()
        if identifier in self.viztrails:
            raise RuntimeError('non-unique identifier \'' + str(identifier) + '\'')
        viztrail_dir = os.path.join(self.base_dir, identifier)
        os.makedirs(viztrail_dir)
        # Create materialized viztrail resource
        vt = FSViztrailHandle.create(
            identifier=identifier,
            properties=properties,
            exec_env_id=exec_env_id,
            base_dir=viztrail_dir
        )
        # Add the new resource to the viztrails index and write the modified
        # repository list to disk.
        self.viztrails[identifier] = vt
        self.write_index()
        # Return the handle for the newly created viztrail
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
            self.viztrails[viztrail_id].delete()
            del self.viztrails[viztrail_id]
            # Write modified repository list to disk
            self.write_index()
            return True
        else:
            return False

    def index_file(self):
        """Get path to index file containing the list of active viztrails.

        Parameters
        ----------
        base_dir: string
            Path to base directory for the repository
        Returns
        -------
        string
        """
        return os.path.join(self.base_dir, INDEX_FILE)

    @staticmethod
    def load(repo_dir):
        """Load viztrails from disk. Each line in the index file references an
        active viztrail. The viztrail resources are expected to be in a
        subfolder in the given reposiroty directory that is named after the
        viztrail identifier.

        If the repository directory does not exist it is created.

        Parameter
        ---------
        repo_dir: string
            Path to the base directory that contains the resource files

        Returns
        -------
        dict(vizier.viztrail.base.ViztrailHandle)
        """
        viztrails = dict()
        index_file = self.index_file()
        if not os.path.isdir(repo_dir):
            # Create base directory if it doesn't exist and return an empty
            # dictionary
            os.makedirs(repo_dir)
        elif os.path.isfile(index_file):
            # Each line in the index file references a subfolder in the base
            # directory that is assumed to contain a viztrail resource
            with open(index_file, 'r') as f:
                for line in f:
                    viztrail_dir = os.path.join(repo_dir, line.strip())
                    vt = FSViztrailHandle.load(viztrail_dir)
                    viztrails[vt.identifier] = vt
        return viztrails

    def write_index(self):
        """Write list of active viztrails to disk."""
        with open(self.index_file(), 'w') as f:
            for viztrail_id in self.viztrails:
                f.write(viztrail_id + '\n')
