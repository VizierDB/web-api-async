# Copyright (C) 2019 New York University,
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

"""Cache for datastores and filestores that are associated with a project."""


class ProjectContext(object):
    """Pair of datastore and filestore that are associated with a project."""
    def __init__(self, datastore, filestore):
        """Initialize the context components.

        Parameters
        ----------
        datastore: vizier.datastore.base.Datastore
            Project datastore
        filestore: vizier.filestore.base.Filestore
            Project filestore
        """
        self.datastore = datastore
        self.filestore = filestore


class ContextCache(object):
    """Maintain a cache for datastore and filestore pairs that are associated
    with each project.
    """
    def __init__(self, datastores, filestores):
        """Initialize an empty cache. The datastore and filesotre pairs are
        keyed by the project identifier. The datastore and filestore factories
        are used to generate context objects for new projects.

        Parameters
        ----------
        datastores: vizier.core.loader.ClassLoader
            Class loader for datastore factory
        filestores: vizier.core.loader.ClassLoader
            Class loader for filestore factory
        """
        self.datastores = datastores
        self.filestores = filestores
        self.cache = dict()

    def get_context(self, project_id):
        """Get the datastore and filestore for the project with the given
        identifier. The context is created if it is not in the cache.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        vizier.engine.backend.cache.ProjectContext
        """
        if project_id in self.cache:
            return self.cache[project_id]
        # Create a new context object if this is the first access for the given
        # identifier
        context = ProjectContext(
            datastore=self.datastores.get_datastore(project_id),
            filestore=self.filestores.get_filestore(project_id)
        )
        self.cache[project_id] = context
        return context
