# Copyright (C) 2017-2020 New York University,
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

"""Helper class for maintaining metadata information about dataset snapshots.
Metadata is maintained in a folder on the file system (named .viziermeta). For
each snapshot a file <snapshot_id>.schema.json maintains the column names and
types. An optinal file <snapshot_id>.profile.json maintains the results from
the optional data profiler that was run on the dataset snapshot.
"""

import os

from vizier.datastore.dataset import DatasetColumn

import vizier.core.util as util


"""Definition of file names and file name templates."""
FILE = '{}.json'
FOLDER = '.viziermeta'


class SnapshotMetadata(object):
    """Helper class for reading and writing metadata that is associated with
    a dataset snapshot.
    """
    def __init__(self, archivedir):
        """Initialize the path to the folder that contains the dataset archive.
        All metadata is maintained in a sub-folder. If that folder does not
        exist it is created.

        Parameters
        ----------
        archivedir: string
            Path to dataset archive directory.
        """
        self.basedir = util.createdir(os.path.join(archivedir, FOLDER))

    def read(self, snapshot_id):
        """Read metadata information for a dataset snapshot. Returns a list of
        dataset columns and (optional) data profiling results that were stored
        for the dataset snapshot. If no profiling information was stored for
        the snapshot the second value in the result tuple is None.

        Paramaters
        ----------
        snapshot_id: int
            Unique snapshot version identifier.

        Returns
        -------
        (list(vizier.datastore.dataset.DatasetColumn), dict)
        """
        filename = os.path.join(self.basedir, FILE.format(snapshot_id))
        # Read metadata from file.
        with open(filename, 'r') as f:
            doc = util.load_json(f.read())
        # De-serialize columns information.
        columns = list()
        for obj in doc['columns']:
            col = DatasetColumn(
                identifier=obj['id'],
                name=obj['name'],
                data_type=obj['type']
            )
            columns.append(col)
        # Return column list and profiling results.
        return columns, doc['profiling']

    def write(self, snapshot_id, columns, profiling):
        """Write metadata information for a dataset snapshot to the file
        system.

        Parameters
        ----------
        snapshot_id: int
            Unique snapshot version identifier.
        columns: list(vizier.datastore.dataset.DatasetColumn)
            List of columns in the snapshot schema.
        profiling: dict
            Additional profiling metadata.
        """
        filename = os.path.join(self.basedir, FILE.format(snapshot_id))
        # Serialize dataset columns.
        collist = list()
        for col in columns:
            collist.append({
                'id': col.identifier,
                'name': col.name,
                'type': col.data_type
            })
        # Combine all metdata in a single document
        doc = {
            'columns': collist,
            'profiling': profiling
        }
        with open(filename, 'w') as f:
            util.dump_json(obj=doc, stream=f)
