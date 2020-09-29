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

"""Vizier Datastore API - Implements all methods of the API to interact with
the datastores that are associated with vizier projects.
"""

from typing import Any, Dict, Optional, Tuple, List

import vizier.api.serialize.dataset as serialize
from vizier.engine.project.cache.base import ProjectCache
from vizier.api.routes.base import UrlFactory
from vizier.engine.project.base import ProjectHandle
from vizier.datastore.dataset import DatasetHandle


class VizierDatastoreApi(object):
    """The Vizier datastore API implements the methods that correspond to
    requests that access and manipulate datasets and their annotations.
    """
    def __init__(self, 
            projects: ProjectCache, 
            urls: UrlFactory, 
            defaults: Any # vizier.config.base.ConfigObject w/ dynamic attributes
        ):
        """Initialize the API components.

        Parameters
        ----------
        projects: vizier.engine.project.cache.base.ProjectCache
            Cache for project handles
        urls: vizier.api.routes.base.UrlFactory
            Factory for resource urls
        defaults : vizier.config.base.ConfigObject
            Web service default values
        """
        self.projects = projects
        self.urls = urls
        self.defaults = defaults

    def create_dataset(self, project_id, columns, rows, properties=None):
        """Create a new dataset in the datastore for the given project. Expects
        a` list of columns and rows for the dataset. The dataset properties
        are optional.

        Returns the serialized descriptor for the new dataset. The result is
        None if the project is unknown.

        Raises ValueError if (1) the column identifier are not unique, (2) the
        row identifier are not uniqe, (3) the number of columns and values in a
        row do not match, (4) any of the column or row identifier have a
        negative value, or (5) if the given column or row counter have value
        lower or equal to any of the column or row identifier.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        columns: list(vizier.datastore.dataset.DatasetColumn)
            List of columns. It is expected that each column has a unique
            identifier.
        rows: list(vizier.datastore.dataset.DatasetRow)
            List of dataset rows.
        properties: dict(string, Any), optional
            Annotations for dataset components

        Returns
        -------
        dict
        """
        # Retrieve the project to ensure that it exists.
        project = self.projects.get_project(project_id)
        if project is None:
            return None
        dataset = project.datastore.create_dataset(
            columns=columns,
            rows=rows,
            properties=properties
        )
        return serialize.DATASET_DESCRIPTOR(
            project=project,
            dataset=dataset,
            urls=self.urls
        )

    def get_caveats(self, 
            project_id: str, 
            dataset_id: str, 
            column_id: Optional[int] = None, 
            row_id: Optional[str] = None
        ) -> List[Dict[str, Any]]:
        """Get annotations for dataset with given identifier. The result is None
        if no dataset with the given identifier exists.

        If only the column id is provided annotations for the identifier column
        will be returned. If only the row identifier is given all annotations
        for the specified row are returned. Otherwise, all annotations for the
        specified cell are returned. If both identifier are None all
        annotations for the dataset are returned.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        dataset_id : string
            Unique dataset identifier
        column_id: int, optional
            Unique column identifier
        row_id: int, optional
            Unique row identifier

        Returns
        -------
        list(dict (serialized caveat))
        """
        # Retrieve the dataset. The result is None if the dataset or the
        # project do not exist.
        project, dataset = self.get_dataset_handle(project_id, dataset_id)
        if dataset is None:
            return list()
        return [
            serialize.CAVEAT(caveat)
            for caveat in project.datastore.get_caveats(
                identifier=dataset_id,
                column_id=column_id,
                row_id=row_id
            )
        ]

    def get_dataset(self, 
            project_id: str, 
            dataset_id: str, 
            offset: int = 0, 
            limit: int = -1
        ) -> Optional[Dict[str, Any]]:
        """Get dataset with given identifier. The result is None if no dataset
        with the given identifier exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        dataset_id : string
            Unique dataset identifier
        offset: int, optional
            Number of rows at the beginning of the list that are skipped.
        limit: int, optional
            Limits the number of rows that are returned.

        Returns
        -------
        dict
        """
        # Retrieve the dataset. The result is None if the dataset or the
        # project do not exist.
        project, dataset = self.get_dataset_handle(project_id, dataset_id)
        if dataset is None:
            return None
        # Determine offset and limits
        if offset is not None:
            offset = max(0, int(offset))
        else:
            offset = 0
        if limit is not None:
            result_size = int(limit)
            if(result_size < 0):
                raise Exception("Invalid Result Size: {}".format(result_size))
        else:
            result_size = self.defaults.row_limit
        if result_size < 0 and self.defaults.max_row_limit > 0:
            result_size = self.defaults.max_row_limit
        elif self.defaults.max_row_limit >= 0:
            result_size = min(result_size, self.defaults.max_row_limit)
        # Serialize the dataset schema and cells
        return serialize.DATASET_HANDLE(
            project=project,
            dataset=dataset,
            rows=dataset.fetch_rows(offset=offset, limit=result_size),
            defaults=self.defaults,
            urls=self.urls,
            offset=offset,
            limit=limit
        )

    def get_dataset_descriptor(self, project_id, dataset_id):
        """Get descriptor for dataset with given identifier. The result is None
        if no dataset with the given identifier exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        dataset_id : string
            Unique dataset identifier

        Returns
        -------
        dict
        """
        # Retrieve project to ensure that it exists
        project = self.projects.get_project(project_id)
        if project is None:
            return None
        # Retrieve the dataset descriptor. The result is None if the dataset
        # does not exist.
        dataset = project.datastore.get_descriptor(dataset_id)
        if dataset is None:
            return None
        # Serialize the dataset descriptor
        return serialize.DATASET_DESCRIPTOR(
            project=project,
            dataset=dataset,
            urls=self.urls
        )

    def get_dataset_handle(self, 
            project_id: str, 
            dataset_id: str
        ) -> Tuple[ProjectHandle, Optional[DatasetHandle]]:
        """Get handle for dataset with given identifier. The result is None if
        the dataset or the project do not exist.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        dataset_id : string
            Unique dataset identifier

        Returns
        -------
        vizier.engine.project.base.ProjectHandle
        vizier.datastore.base.DatasetHandle
        """
        # Retrieve the project and dataset from the repository to ensure that
        # it exists.
        project = self.projects.get_project(project_id)
        if project is None:
            return None, None
        return project, project.datastore.get_dataset(dataset_id)

    def get_profiling(self, project_id, dataset_id):
        """Get data profiling results for a given dataset.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        dataset_id : string
            Unique dataset identifier

        Returns
        -------
        dict
        """
        # Retrieve the dataset. The result is None if the dataset or the
        # project do not exist.
        project, dataset = self.get_dataset_handle(project_id, dataset_id)
        if dataset is None:
            return None
        return dataset.get_profiling() if dataset is not None else None