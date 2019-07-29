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

"""Implementation of the vizier datastore interface that uses Mimir as the
storage backend.
"""

import csv
import gzip
import json
import os
import unicodecsv

from StringIO import StringIO

from vizier.core.util import dump_json, load_json
from vizier.core.util import get_unique_identifier, min_max
from vizier.datastore.annotation.dataset import DatasetMetadata
from vizier.datastore.base import DefaultDatastore
from vizier.datastore.mimir.dataset import MimirDatasetColumn, MimirDatasetHandle

import vizier.mimir as mimir
import vizier.datastore.base as helper
import vizier.datastore.mimir.base as base


"""Name of file storing dataset (schema) information."""
DATASET_FILE = 'dataset.json'


class MimirDatastore(DefaultDatastore):
    """Vizier data store implementation using Mimir.

    Maintains information about the dataset schema separately. This is necessary
    because in a dataset column names are not necessarily unique. For each
    dataset a new subfolder is created in the store base directory. In that
    folder a dataset file and an annotation file are maintained. All files are
    in Yaml format.

    Note that every write_dataset call creates a new table in the underlying
    Mimir database. Other datasets are views on these tables.
    """
    def __init__(self, base_path):
        """Initialize the base directory that contains the dataset index and
        metadata files.

        Parameters
        ----------
        base_path: string
            Name of the directory where metadata is stored
        """
        super(MimirDatastore, self).__init__(base_path)

    def create_dataset(self, columns, rows, human_readable_name = None, annotations=None):
        """Create a new dataset in the datastore. Expects at least the list of
        columns and the rows for the dataset.

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
        # Get unique identifier for new dataset
        identifier = 'DS_' + get_unique_identifier()
        # Write rows to temporary file in CSV format
        tmp_file = os.path.abspath(self.base_path + identifier)
        # Create a list of columns that contain the user-vizible column name and
        # the name in the database
        db_columns = list()
        colSql = 'ROWID() AS ' + base.ROW_ID
        for col in map(base.sanitize_column_name, columns):
            db_columns.append(
                MimirDatasetColumn(
                    identifier=col.identifier,
                    name_in_dataset=col.name,
                    name_in_rdb=col.name
                )
            )
            colSql = colSql + ', ' + col.name + ' AS ' + col.name
        # Create CSV file for load
        with open(tmp_file, 'w') as f_out:
            writer = csv.writer(f_out, quoting=csv.QUOTE_MINIMAL)
            writer.writerow([col.name_in_rdb for col in db_columns])
            for row in rows:
                record = helper.encode_values(row.values)
                writer.writerow(record)
        # Load CSV file using Mimirs loadCSV method.
        table_name = mimir.loadDataSource(tmp_file, True, True, human_readable_name = human_readable_name)
        os.remove(tmp_file)
        sql = 'SELECT '+ colSql +' FROM {{input}};'
        view_name = mimir.createView(table_name, sql)
        # Get number of rows in the view that was created in the backend
        #sql = 'SELECT COUNT(*) AS RECCNT FROM ' + view_name + ';'
        #rs_count = mimir.vistrailsQueryMimirJson(sql, False, False)
        #row_count = int(rs_count['data'][0][0])
        # Get unique identifier for all rows in the created dataset
        sql = 'SELECT 1 AS NOP FROM ' + view_name + ';'
        rs = mimir.vistrailsQueryMimirJson(sql, False, False)
        row_ids = rs['prov']
        row_idxs = range(len(row_ids))
        # Insert the new dataset metadata information into the datastore
        return self.register_dataset(
            table_name=view_name,
            columns=db_columns,
            row_idxs=row_idxs,
            row_ids=row_ids,
            row_counter=len(row_idxs) + 1,
            annotations=annotations
        )

    def get_dataset_file(self, identifier):
        """Get the absolute path of the file that maintains the dataset metadata
        such as the order of row id's and column information.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return os.path.join(self.get_dataset_dir(identifier), DATASET_FILE)

    def get_dataset(self, identifier):
        """Read a full dataset from the data store. Returns None if no dataset
        with the given identifier exists.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier

        Returns
        -------
        vizier.datastore.mimir.dataset.MimirDatasetHandle
        """
        # Return None if the dataset file does not exist
        dataset_file = self.get_dataset_file(identifier)
        if not os.path.isfile(dataset_file):
            return None
        annotations = DatasetMetadata.from_file(
            self.get_metadata_filename(identifier)
        )
        return MimirDatasetHandle.from_file(
            dataset_file,
            annotations=annotations
        )

    def get_annotations(self, identifier, column_id=-1, row_id='-1'):
        """Get list of annotations for a dataset component. Expects at least one
        of the given identifier to be a valid identifier (>= 0).

        Parameters
        ----------
        column_id: int, optional
            Unique column identifier
        row_id: int, optiona
            Unique row identifier

        Returns
        -------
        list(vizier.datastore.metadata.Annotation)
        """
        # Return immediately if request is for column or row annotations. At the
        # moment we only maintain uncertainty information for cells. If cell
        # annotations are requested we need to query the database to retrieve
        # any existing uncertainty annotations for the cell.
        return self.get_dataset(identifier).get_annotations(column_id,row_id)
    
    def download_dataset(
        self, url, username=None, password=None, filestore=None, detect_headers=True, 
        infer_types=True, load_format='csv', options=[], human_readable_name=None
    ):
        """Create a new dataset from a given file. Returns the handle for the
        downloaded file only if the filestore has been provided as an argument
        in which case the file handle is meaningful file handle.

        Raises ValueError if the given file could not be loaded as a dataset.

        Parameters
        ----------
        url : string
            Unique resource identifier for external resource that is accessed
        username: string, optional
            Optional user name for authentication
        password: string, optional
            Optional password for authentication
        detect_headers: bool, optional
            Detect column names in loaded file if True
        infer_types: bool, optional
            Infer column types for loaded dataset if True
        load_format: string, optional
            Format identifier
        options: list, optional
            Additional options for Mimirs load command
        filestore: vizier.filestore.base.Filestore, optional
            Optional filestore to save a local copy of the downloaded resource
        human_readable_name: string, optional
            Optional human readable name for the resulting table.
        Returns
        -------
        vizier.datastore.fs.dataset.FileSystemDatasetHandle,
        vizier.filestore.base.FileHandle
        """
        if username is not None:
            options += [("username", username)]
        if password is not None:
            options += [("password", password)]
        return self.load_dataset(
            url = url, 
            options = options,
            detect_headers = detect_headers,
            infer_types = infer_types,
            load_format = load_format,
            human_readable_name = human_readable_name
        )

    def load_dataset(
        self, f_handle=None, url=None, detect_headers=True, infer_types=True,
        load_format='csv', options=[], human_readable_name=None
    ):
        """Create a new dataset from a given file or url. Expects that either
        the file handle or the url are not None. Raises ValueError if both are
        None or not None.


        Parameters
        ----------
        f_handle : vizier.filestore.base.FileHandle, optional
            handle for an uploaded file on the associated file server.
        url: string, optional, optional
            Url for the file source
        detect_headers: bool, optional
            Detect column names in loaded file if True
        infer_types: bool, optional
            Infer column types for loaded dataset if True
        load_format: string, optional
            Format identifier
        options: list, optional
            Additional options for Mimirs load command
        human_readable_name: string, optional
            Optional human readable name for the resulting table

        Returns
        -------
        vizier.datastore.mimir.dataset.MimirDatasetHandle
        """
        if f_handle is None and url is None:
            raise ValueError('no load source given')
        elif not f_handle is None and not url is None:
            raise ValueError('too many load sources given')
        elif url is None:
             # os.path.abspath((r'%s' % os.getcwd().replace('\\','/') ) + '/' + f_handle.filepath)
             abspath = f_handle.filepath
        elif not url is None:
            abspath = url
        # Load dataset into Mimir
        init_load_name = mimir.loadDataSource(
            abspath,
            infer_types,
            detect_headers,
            load_format,
            human_readable_name,
            options
        )
        # Retrieve schema information for the created dataset
        sql = 'SELECT * FROM ' + init_load_name
        mimirSchema = mimir.getSchema(sql)
        # Create list of dataset columns
        columns = list()
        colSql = 'ROWID() AS ' + base.ROW_ID
        for col in mimirSchema:
            col_id = len(columns)
            name_in_dataset = base.sanitize_column_name(col['name'].upper())
            name_in_rdb = base.sanitize_column_name(col['name'].upper())
            col = MimirDatasetColumn(
                identifier=col_id,
                name_in_dataset=name_in_dataset,
                name_in_rdb=name_in_rdb
            )
            colSql = colSql + ', ' + name_in_dataset + ' AS ' + name_in_rdb
            columns.append(col)
        # Create view for loaded dataset
        sql = 'SELECT '+ colSql +' FROM {{input}};'
        view_name = mimir.createView(init_load_name, sql)
        # TODO: this is a hack to speed up this step a bit.
        #  we get the first row id and the count and take a range;
        #  this is fragile and should be made better
        #
        # NOTE: This does not work because ROW_ID appears to be a string.
        # Thus, sorting not necessarily returns the smallest integer value
        # first.
        #
        sql = 'SELECT COUNT(*) AS RECCNT FROM ' + view_name + ';'
        rs = mimir.vistrailsQueryMimirJson(sql, False, False)
        #sql = 'SELECT ' + base.ROW_ID + ' FROM ' + view_name + ' ORDER BY CAST(' + base.ROW_ID + ' AS INTEGER) LIMIT 1;'
        #rsfr = mimir.vistrailsQueryMimirJson(sql, False, False)
        row_count = int(rs['data'][0][0])
        #first_row_id = int(rsfr['data'][0][0])
        #row_ids = map(str, range(first_row_id, first_row_id+row_count))
        # Insert the new dataset metadata information into the datastore
        sql = 'SELECT 1 AS NOP FROM ' + view_name + ' LIMIT 1;'
        rs = mimir.vistrailsQueryMimirJson(sql, False, False)
        first_row_id = int(rs['prov'][0])
        row_ids = range(first_row_id, first_row_id+row_count)
        row_idxs = range(row_count)
        #row_counter = (row_ids[-1] + 1) if len(row_ids) > 0 else 0
        return self.register_dataset(
            table_name=view_name,
            columns=columns,
            row_idxs=row_idxs,
            row_ids=row_ids,
            row_counter=len(row_idxs)+1
        )

    def register_dataset(
        self, table_name, columns, row_idxs, row_ids, row_counter, annotations=None,
        update_rows=False
    ):
        """Create a new record for a database table or view. Note that this
        method does not actually create the table or view in the database but
        adds the datasets metadata to the data store. The table or view will
        have been created by a load command or be the result from executing
        a lens or a VizUAL command.

        Parameters
        ----------
        table_name: string
            Name of relational database table or view containing the dataset.
        columns: list(vizier.datastore.mimir.MimirDatasetColumn)
            List of column names in the dataset schema and their corresponding
            names in the relational database table or view.
        row_ids: list(int)
            List of row ids. Determines the order of rows in the dataset
        row_counter: int
            Counter for unique row ids
        annotations: vizier.datastore.metadata.DatasetMetadata
            Annotations for dataset components
        update_rows: bool, optional
            Flag indicating that the number of rows may have changed and the
            list of row identifier therefore needs to be checked.

        Returns
        -------
        vizier.datastore.mimir.dataset.MimirDatasetHandle
        """
        # Depending on whether we need to update row ids we either query the
        # database or just get the schema. In either case mimir_schema will
        # contain a the returned Mimir schema information.
        sql = base.get_select_query(table_name, columns=columns) + ';'
        mimir_schema = mimir.getSchema(sql)
        if update_rows:
            sql = base.get_select_query(table_name) + ';'
            rs = mimir.vistrailsQueryMimirJson(sql, False, False)
            # Get list of row identifier in current dataset. Row ID's are
            # expected to be the only values in the returned result set.
            dataset_row_ids = set()
            for row in rs['data']:
                dataset_row_ids.add(row[0])
            modified_row_ids = list()
            # Remove row id's that are no longer in the data.
            for row_id in row_ids:
                if row_id in dataset_row_ids:
                    modified_row_ids.append(row_id)
            # Add new row ids
            for row_id in dataset_row_ids:
                if not row_id in modified_row_ids:
                    modified_row_ids.append(row_id)
            # Replace row ids with modified list
            row_ids = modified_row_ids
        # Create a mapping of column name (in database) to column type. This
        # mapping is then used to update the data type information for all
        # column descriptors.
        col_types = dict()
        for col in mimir_schema:
            col_types[base.sanitize_column_name(col['name'].upper())] = col['baseType']
        for col in columns:
            col.data_type = col_types[col.name_in_rdb]
        # Create column for row Identifier
        rowid_column = MimirDatasetColumn(
            name_in_dataset=base.ROW_ID,
            data_type=col_types[base.ROW_ID]
        )
        # Set row counter to max. row id + 1 if None
        #if row_counter is None:
        #    sql = 'SELECT COUNT(*) AS RECCNT FROM ' + table_name
        #    rs = mimir.vistrailsQueryMimirJson(sql, False, False)
        #    row_counter = int(rs['data'][0][0]) + 1
        dataset = MimirDatasetHandle(
            identifier=get_unique_identifier(),
            columns=map(base.sanitize_column_name, columns),
            rowid_column=rowid_column,
            table_name=table_name,
            row_idxs=row_idxs,
            row_ids=row_ids,
            row_counter=row_counter,
            annotations=annotations
        )
        # Create a new directory for the dataset if it doesn't exist.
        dataset_dir = self.get_dataset_dir(dataset.identifier)
        if not os.path.isdir(dataset_dir):
            os.makedirs(dataset_dir)
        # Write dataset and annotation file to disk
        dataset.to_file(self.get_dataset_file(dataset.identifier))
        dataset.annotations.to_file(
            self.get_metadata_filename(dataset.identifier)
        )
        return dataset


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def create_missing_key_view(dataset, lens_name, key_column):
    """ Create a view for missing ROW_ID's on a MISSING_KEY lens.

    Parameters
    ----------
    dataset: vizier.datastore.mimir.MimirDatasetHandle
        Descriptor for the dataset on which the lens was created
    lens_name: string
        Identifier of the created MISSING_KEY lens
    key_column: vizier.datastore.mimir.MimirDatasetColumn
        Name of the column for which the missing values where generated

    Returns
    -------
    string, int
        Returns the name of the created view and the adjusted counter  for row
        ids.
    """
    # Select the rows that have missing row ids
    key_col_name = key_column.name_in_rdb
    sql = 'SELECT ' + key_col_name + ' FROM ' + lens_name
    sql += ' WHERE ' + ROW_ID + ' IS NULL;'
    rs = mimir.vistrailsQueryMimirJson(sql, False, False)
    case_conditions = []
    for row in rs['data']:
        row_id = dataset.row_counter + len(case_conditions)
        val = str(row[0])
        # If the key colum is of type real then we need to convert val into
        # something that looks like a real
        if key_column.data_type.lower() == 'real':
            val += '.0'
        case_conditions.append(
            'WHEN ' + key_col_name + ' = ' + val + ' THEN ' + str(row_id)
        )
    # If no new rows where inserted we are good to go with the existing lens
    if len(case_conditions) == 0:
        return lens_name, dataset.row_counter
    # Create the view SQL statement
    stmt = 'CASE ' + (' '.join(case_conditions)).strip()
    stmt += ' ELSE ' + ROW_ID + ' END AS ' + ROW_ID
    col_list = [stmt]
    for column in dataset.columns:
        col_list.append(column.name_in_rdb)
    sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + lens_name + ';'
    view_name = mimir.createView(dataset.table_name, sql)
    return view_name, dataset.row_counter + len(case_conditions)
