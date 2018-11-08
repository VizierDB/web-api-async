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

"""Federated Data Store - Provide read-only access to a set of data stores."""

from vizier.core.system import build_info, component_descriptor
from vizier.datastore.base import DataStore


class FederatedDataStore(DataStore):
    """Provide read-only access to a collection of data stores. This class is
    primarily used by the Web API to provide read access to the datastores that
    are associated with different workflow engines.

    This implementation assumes that dataset identifier that are used by
    different data stores are globally unique.
    """
    def __init__(self, datastores):
        """Initialize the collection of federated data stores.

        Parameters
        ---------
        datastores : list(vizier.datastore.base.DataStore)
            List of federated data store instances
        """
        super(FederatedDataStore, self).__init__(
            build_info('FederatedDataStore')
        )
        self.datastores = datastores

    def components(self):
        """List containing component descriptor.

        Returns
        -------
        list
        """
        comp = list()
        comp.append(component_descriptor('datastore', self.system_build()))
        for ds in self.datastores:
            comp.extend(ds.components())
        return comp

    def get_dataset(self, identifier):
        """Read a full dataset from the data store. Returns None if no dataset
        with the given identifier exists.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier

        Returns
        -------
        vizier.datastore.base.DatasetHandle
        """
        # Assumes that at most one data store will contain a dataset with the
        # given identifier
        for store in self.datastores:
            ds = store.get_dataset(identifier)
            if not ds is None:
                return ds
        return None

    def update_annotation(self, identifier, column_id=-1, row_id=-1, anno_id=-1, key=None, value=None):
        """Update the annotations for a component of the datasets with the given
        identifier. Returns the updated annotations or None if the dataset
        does not exist.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        column_id: int, optional
            Unique column identifier
        row_id: int, optional
            Unique row identifier
        anno_id: int
            Unique annotation identifier
        key: string, optional
            Annotation key
        value: string, optional
            Annotation value

        Returns
        -------
        vizier.datastore.metadata.Annotation
        """
        # Try all data stores. At most one would return a non-None value if the
        # dataset exists
        for store in self.datastores:
            result = store.update_annotation(
                identifier,
                anno_id=anno_id,
                column_id=column_id,
                row_id=row_id,
                key=key,
                value=value
            )
            if not result is None:
                return result
        return None
