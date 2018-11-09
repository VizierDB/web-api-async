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

"""Definition of execution environment for workflows."""

from vizier.datastore.base import DATASTORE_DEFAULT, DATASTORE_MIMIR
from vizier.datastore.fs import FileSystemDataStore
from vizier.datastore.mimir import MimirDataStore


class ExecEnv(object):
    """Execution environment for workflows. Maintains references to the
    environment specific data store, command registry and the file server.

    Attributes
    ----------
    datastore: vizier.datastore.base.DataStore
        Environment-specific data store.
    fileserver: vizier.filestore.FileServer
        System-wide file server
    identifier: string
        Unique environment identifier
    """
    def __init__(self, config, packages, fileserver):
        """Initialize the environment from a configuration object. The structure
        of the configuration object is expected to contain at least the
        following:

        - identifier: Unique environment identifier
        - datastore:
            - properties: Type-specific dictionary of data store properties
            - type: Datastore type
        - packages: list of package identifier

        Parameters
        ----------
        config: vizier.config.ConfigObject
            Configuration object
        packages: dict()
            Dictionary of package specifications
        fileserver: vizier.filestore.base.FileServer
            System-wide file server
        """
        # identifier
        self.identifier = config.identifier
        # datastore
        if self.datastore.type == DATASTORE_DEFAULT:
            self.datastore = FileSystemDataStore(config.datastore.properties)
        elif self.datastore.type == DATASTORE_MIMIR:
            self.datastore = MimirDataStore(config.datastore.properties)
        else:
            raise RuntimeError('unknown datastore type \'' + self.datastore.type + '\'')
        # fileserver
        self.fileserver = fileserver
        # packages
        self.packages = dict()
        for pckg_id in config.packages:
            self.packages[pckg_id] = packages[pckg_id]
