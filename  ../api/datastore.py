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

"""
"""

class VizierDatastoreApi(object):
    """
    """
    def __init__(self):
        """
        """
        pass

    # --------------------------------------------------------------------------
    # Datasets
    # --------------------------------------------------------------------------
    def get_dataset(self, dataset_id, offset=None, limit=None):
        """Get dataset with given identifier. The result is None if no dataset
        with the given identifier exists.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier
        offset: int, optional
            Number of rows at the beginning of the list that are skipped.
        limit: int, optional
            Limits the number of rows that are returned.

        Returns
        -------
        dict
            Dictionary representation for dataset state
        """
        # Get dataset with given identifier from data store. If the dataset
        # does not exist the result is None.
        dataset = self.get_dataset_handle(dataset_id)
        if not dataset is None:
            # Determine offset and limits
            if not offset is None:
                offset = max(0, int(offset))
            else:
                offset = 0
            if not limit is None:
                result_size = int(limit)
            else:
                result_size = self.config.defaults.row_limit
            if result_size < 0 and self.config.defaults.max_row_limit > 0:
                result_size = self.config.defaults.max_row_limit
            elif self.config.defaults.max_row_limit >= 0:
                result_size = min(result_size, self.config.defaults.max_row_limit)
            # Serialize the dataset schema and cells
            return serialize.DATASET(
                dataset=dataset,
                rows=dataset.fetch_rows(offset=offset, limit=result_size),
                config=self.config,
                urls=self.urls,
                offset=offset,
                limit=limit
            )

    def get_dataset_annotations(self, dataset_id, column_id=-1, row_id=-1):
        """Get annotations for dataset with given identifier. The result is None
        if no dataset with the given identifier exists.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier

        Returns
        -------
        dict
            Dictionary representation for dataset annotations
        """
        # Get dataset with given identifier from data store. If the dataset
        # does not exist the result is None.
        dataset = self.get_dataset_handle(dataset_id)
        if dataset is None:
            return None
        anno = dataset.get_annotations(column_id=column_id, row_id=row_id)
        return serialize.DATASET_ANNOTATIONS(
            dataset_id,
            annotations=anno,
            column_id=column_id,
            row_id=row_id,
            urls=self.urls
        )

    def get_dataset_handle(self, dataset_id):
        """Get handle for dataset with given identifier. The result is None if
        no dataset with the given identifier exists.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier

        Returns
        -------
        vizier.datastore.base.DatasetHandle
        """
        if dataset_id in self.datasets:
            dataset = self.datasets[dataset_id]
        else:
            dataset = self.datastore.get_dataset(dataset_id)
            if not dataset is None:
                self.datasets[dataset_id] = dataset
        return dataset

    def update_dataset_annotation(self, dataset_id, column_id=-1, row_id=-1, anno_id=-1, key=None, value=None):
        """Update the annotations for a component of the datasets with the given
        identifier. Returns the modified object annotations or None if the
        dataset does not exist.

        Parameters
        ----------
        dataset_id : string
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
        dict
        """
        # Get dataset with given identifier from data store. If the dataset
        # does not exist the result is None.
        result = self.datastore.update_annotation(
            dataset_id,
            column_id=column_id,
            row_id=row_id,
            anno_id=anno_id,
            key=key,
            value=value
        )
        if result is None:
            return None
        # Get updated annotations. Need to ensure that the dataset is removed
        # from the cache
        if dataset_id in self.datasets:
            del self.datasets[dataset_id]
        return self.get_dataset_annotations(
            dataset_id,
            column_id=column_id,
            row_id=row_id
        )
