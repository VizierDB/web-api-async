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

"""Vizier API - Implements all methods of the API to interact with a running
Vizier instance.

The API orchestrates the interplay
between different components such as the viztrail repository that manages
viztrails and the workflow engine that executes modules in viztrail workflow
versions.

Internally the API is further divided into four parts that deal with the file
store, data store, viztrail repository and the workflow execution engine.
"""

from vizier.api.filestore import FileStoreApi
from vizier.api.viztrails import ViztrailRepositoryApi


class VizierApi(object):
    """The Vizier API implements the methods that correspond to requests that
    are supported by the Vizier Web Service. the API, however, can also be used
    in a stand-alone manner, e.g., via the command line interpreter tool.

    This class is a wrapper around the different components of the Vizier system
    that are necessary for the Web Service, i.e., file store, data store,
    viztrail repository, and workflow execution engine.
    """
    def __init__(self, filestore, viztrails_repository):
        """Initialize the API components.

        Parameters
        ----------
        filestore: vizier.filestore.base.FileStore
            Backend store for uploaded files
        viztrails_repository: vizier.viztrail.repository.ViztrailRepository
            Viztrail manager for the API instance
        """
        self.filestore = FileStoreApi(filestore)
        self.projects = ViztrailRepositoryApi(viztrails_repository)

    # --------------------------------------------------------------------------
    # Service
    # --------------------------------------------------------------------------
    def service_overview(self):
        """Returns a dictionary containing essential information about the web
        service including HATEOAS links to access resources and interact with
        the service.

        Returns
        -------
        dict
        """
        return self.service_descriptor

    def system_build(self):
        """Returns a dictionary with information about individual system
        components, including version information for software components.

        Returns
        -------
        dict
        """
        components = list()
        components.extend(self.datastore.components())
        components.extend(self.filestore.components())
        components.extend(self.projects.components())
        return serialize.SERVICE_BUILD(components, self.urls)
