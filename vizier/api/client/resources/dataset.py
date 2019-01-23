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

"""Objects representing descriptors and handles for datasets."""

from vizier.datastore.base import get_column_index
from vizier.datastore.dataset import DatasetColumn

import vizier.api.serialize.deserialize as deserialize
import vizier.api.serialize.labels as labels


class DatasetDescriptor(object):
    """A dataset descriptor is simply an identifier and a dictionary of HATEOAS
    references.
    """
    def __init__(self, identifier, columns, links):
        """Initialize the dataset identifier, list of columns, and HATEOAS
        references.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        columns: list(vizier.datastore.dataset.DatasetColumn)
            Dataset schema
        links: dict
            Dictionary of HATEOS references for the dataset
        """
        self.identifier = identifier
        self.columns = columns
        self.links = links

    @staticmethod
    def from_dict(obj):
        """Create the descriptor from a dictionary serialization.

        Parameters
        ----------
        obj: dict
            Dictionary serialization for dataset descriptor as returned by the
            server.

        Returns
        -------
        vizier.api.client.resources.dataset.DatasetDescriptor
        """
        return DatasetDescriptor(
            identifier=obj[labels.ID],
            columns=[
                DatasetColumn(
                    identifier=col[labels.ID],
                    name=col[labels.NAME],
                    data_type=col[labels.DATATYPE]
                ) for col in obj['columns']
            ],
            links=deserialize.HATEOAS(links=obj[labels.LINKS])
        )

    def get_column(self, column_id):
        """Get a column based on the column name, column spreadsheet label, or
        index position.

        Parameters
        ---------
        column_id: string or int

        Returns
        -------
        vizier.datastore.dataset.DatasetColumn
        """
        col_idx = get_column_index(columns=self.columns, column_id=column_id)
        return self.columns[col_idx]
