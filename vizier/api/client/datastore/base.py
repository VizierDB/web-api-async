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

"""Datastore that uses the web service API to create and retrieve datasets.
This datastore is for example used by python cell processors that do not have
access to a shared file system, e.g., processors that are sandboxed in
containers.
"""

import json
import requests

from vizier.api.client.datastore.dataset import RemoteDatasetHandle
from vizier.datastore.base import Datastore
from vizier.datastore.object.dataobject import DataObjectMetadata

import vizier.api.serialize.dataset as serialize
import vizier.api.serialize.deserialize as deserialize
import vizier.api.serialize.labels as labels

class DatastoreClient(Datastore):
    """Datastore that is a client to a vizier web service API. Datasets are
    retireved from the API and stored at the API via HTTP requests.

    The datastore only allows access to datasets for a single project.
    """
    def __init__(self, urls):
        """Initialize the url factory to retireve and manipulate API resources.

        Parameters
        ----------
        urls: vizier.api.routes.datastore.DatastoreClientUrlFactory
            Factory for urls to access and manipulate datasets
        """
        self.urls = urls

    def create_dataset(self, columns, rows, annotations=None):
        """Create a new dataset in the project datastore using the API. Expects
        a list of columns and the rows for the dataset. All columns and rows
        should have unique non-negative identifier (although this may not
        be enforced by all backend datastores). Depending on the backend
        datastore the given identifier may change. They are, however, required
        to reference the given annotations properly.

        Raises ValueError if the backend datastore rejects the given dataset.

        Parameters
        ----------
        columns: list(vizier.datastore.dataset.DatasetColumn)
            List of columns. It is expected that each column has a unique
            identifier.
        rows: list(vizier.datastore.dataset.DatasetRow)
            List of dataset rows.
        annotations: vizier.datastore.annotation.dataset.DatasetMetadata, optional
            Annotations for dataset components

        Returns
        -------
        vizier.datastore.dataset.DatasetDescriptor
        """
        url = self.urls.create_dataset()
        data = {
            labels.COLUMNS: [serialize.DATASET_COLUMN(col) for col in columns],
            labels.ROWS: [serialize.DATASET_ROW(row) for row in rows]
        }
        if not annotations is None:
            data[labels.ANNOTATIONS] = [
                serialize.ANNOTATION(a) for a in annotations.values
            ]
        # Send request. Raise exception if status code indicates that the
        # request was not successful.
        r = requests.post(url, json=data)
        r.raise_for_status()
        obj = json.loads(r.text)
        return deserialize.DATASET_DESCRIPTOR(obj)

    def get_dataset(self, identifier):
        """Get the handle for the dataset with given identifier from the data
        store. Returns None if no dataset with the given identifier exists.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier

        Returns
        -------
        vizier.datastore.base.DatasetHandle
        """
        url = self.urls.get_dataset(identifier)
        r = requests.get(url)
        if r.status_code == 404:
            return None
        elif r.status_code != 200:
            r.raise_for_status()
        # The result is the workflow handle
        obj = json.loads(r.text)
        ds = deserialize.DATASET_DESCRIPTOR(obj)
        return RemoteDatasetHandle(
            identifier=ds.identifier,
            columns=ds.columns,
            row_count=ds.row_count,
            rows=[deserialize.DATASET_ROW(row) for row in obj[labels.ROWS]],
            store=self
        )

    def get_annotations(self, identifier):
        """Get all dataset annotations.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier

        Returns
        -------
        vizier.datastore.annotation.dataset.DatasetMetadata
        """
        url = self.urls.get_dataset_annotations(identifier)
        r = requests.get(url)
        if r.status_code == 404:
            return None
        elif r.status_code != 200:
            r.raise_for_status()
        # The result is the workflow handle
        obj = json.loads(r.text)
        return deserialize.DATASET_ANNOTATIONS(obj)

    def get_descriptor(self, identifier):
        """Get the descriptor for the dataset with given identifier from the
        data store. Returns None if no dataset with the given identifier exists.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier

        Returns
        -------
        vizier.datastore.base.DatasetDescriptor
        """
        url = self.urls.get_dataset_descriptor(identifier)
        r = requests.get(url)
        if r.status_code == 404:
            return None
        elif r.status_code != 200:
            r.raise_for_status()
        # The result is the workflow handle
        obj = json.loads(r.text)
        return deserialize.DATASET_DESCRIPTOR(obj)

    def get_objects(self, identifier=None, obj_type=None, key=None):
        """Get list of data objects for a resources of a given dataset. If only
        the column id is provided annotations for the identifier column will be
        returned. If only the row identifier is given all annotations for the
        specified row are returned. Otherwise, all annotations for the specified
        cell are returned. If both identifier are None all annotations for the
        dataset are returned.

        Parameters
        ----------
        identifier: string, optional
            Unique object identifier
        obj_type: string, optional
            object type
        key: string, optional
            object key

        Returns
        -------
        vizier.datastore.object.dataobject.DataObjectMetadata
        """
        return DataObjectMetadata()

    def load_dataset(
        self, f_handle=None, url=None, detect_headers=True, infer_types='none',
        load_format='csv', options=[], human_readable_name=None
    ):
        """Create a new dataset from a given file or Url.

        Parameters
        ----------
        f_handle : vizier.filestore.base.FileHandle, optional
            handle for an uploaded file on the associated file server.
        url: string, optional, optional
            Url for the file source
        detect_headers: bool, optional
            Detect column names in loaded file if True
        infer_types: string, optional
            Infer column types for loaded dataset if selected a profiler.
        load_format: string, optional
            Format identifier
        options: list, optional
            Additional options for Mimirs load command
        human_readable_name: string, optional
            Optional human readable name for the resulting table

        Returns
        -------
        vizier.datastore.base.DatasetHandle
        """
        raise NotImplementedError()

    def update_annotation(
        self, identifier, key, old_value=None, new_value=None, column_id=None,
        row_id=None
    ):
        """Update the annotations for a component of the datasets with the given
        identifier. Returns the updated annotations or None if the dataset
        does not exist.

        The distinction between old value and new value is necessary since
        annotations have no unique identifier. We use the key,value pair to
        identify an existing annotation for update. When creating a new
        annotation th old value is None.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        column_id: int, optional
            Unique column identifier
        row_id: int, optional
            Unique row identifier
        key: string, optional
            Annotation key
        old_value: string, optional
            Previous annotation value whan updating an existing annotation.
        new_value: string, optional
            Updated annotation value

        Returns
        -------
        bool
        """
        url = self.urls.update_dataset_annotations(identifier)
        data = {labels.KEY: key}
        if not column_id is None:
            data[labels.COLUMN_ID] = column_id
        if not row_id is None:
            data[labels.ROW_ID] = row_id
        if not old_value is None:
            data[labels.OLD_VALUE] = old_value
        if not new_value is None:
            data[labels.NEW_VALUE] = new_value
        # Send request. Raise exception if status code indicates that the
        # request was not successful. If no exception was raised return True.
        r = requests.post(url, json=data)
        r.raise_for_status()
        return True

    def update_object(
        self, identifier, key, old_value=None, new_value=None, obj_type=None
    ):
        """Update a data object.

        Parameters
        ----------
        identifier : string
            Unique object identifier
        key: string, optional
            object key
        old_value: string, optional
            Previous value when updating an existing annotation.
        new_value: string, optional
            Updated value
        Returns
        -------
        bool
        """
        # TODO: Implementation needed
        raise NotImplementedError()

    def unload_dataset(
        self, filepath, dataset_name, format='csv', options=[], filename=''
    ):
        """Export a dataset from a given name.

        Raises ValueError if the given dataset could not be exported.

        Parameters
        ----------
        dataset_name: string
            Name of the dataset to unload
        format: string
            Format for output (csv, json, ect.)
        options: dict
            Options for data unload
        filename: string
            The output filename - may be empty if outputting to a database

        Returns
        -------
        vizier.filestore.base.FileHandle
        """
        # TODO: Implementation needed
        raise NotImplementedError()
