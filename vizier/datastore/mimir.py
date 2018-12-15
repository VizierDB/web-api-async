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

"""Implementation of Vizier data store using Mimir."""

import csv
import gzip
import json
import os
import re
import shutil
import tempfile
import unicodecsv
import yaml

from StringIO import StringIO

import vistrails.packages.mimir.init as mimir

from vizier.core.util import get_unique_identifier, min_max
from vizier.datastore.base import DatasetHandle, DatasetColumn, DatasetRow
from vizier.datastore.base import DataStore, encode_values, max_column_id
from vizier.datastore.metadata import Annotation, DatasetMetadata, ObjectMetadataSet
from vizier.datastore.reader import DatasetReader


"""Configuration parameter."""
PARA_DIRECTORY = 'directory'

"""Mimir annotation keys."""
ANNO_UNCERTAIN = 'mimir:uncertain'

"""Value casts for SQL update statements."""
CAST_TRUE = 'CAST(1 AS BOOL)'
CAST_FALSE = 'CAST(0 AS BOOL)'

"""Compiled regular expressions to identify valid date and datetime values.
Note that this does not check if a date string actually specifies a valid
calendar date. But it appears that Mimir accepts any sting that follows this
format."""
DATE_FORMAT = re.compile('^\d{4}-\d\d?-\d\d?$')
DATETIME_FORMAT = re.compile('^\d{4}-\d\d?-\d\d? \d\d?:\d\d?:\d\d?(\.\d+)?$')

"""Column name prefix (Important: Use upper case!)."""
COL_PREFIX = 'COL'

"""Name of file storing dataset (schema) information."""
DATASETS_FILE = 'dataset-index.yaml'

"""Name of the database column that contains the row id for tuples
(Important: Use upper case!).
"""
ROW_ID = 'RID'


class MimirDatasetColumn(DatasetColumn):
    """Column in a dataset that is stored as a Mimir table or view. Given that
    column names are not necessarily unique in a dataset, there is a need to
    maintain a mapping of dataset names to attribute names for tables/views in
    the relational database.

    Attributes
    ----------
    identifier: int
        Unique column identifier
    name: string
        Name of column in the dataset
    name_in_rdb: string
        Name of the corresponding attribute in a relational table or views
    data_type: string, optional
        String representation of the column type in the database. By now the
        following data_type values are expected: date (format yyyy-MM-dd), int,
        varchar, real, and datetime (format yyyy-MM-dd hh:mm:ss:zzzz).
    """
    def __init__(self, identifier=None, name_in_dataset=None, name_in_rdb=None, data_type=None):
        """Initialize the dataset column.

        Parameters
        ----------
        identifier: int
            Unique column identifier
        name_in_dataset: string
            Name of column in the dataset
        name_in_rdb: string, optional
            Name of the corresponding attribute in a relational table or views
        data_type: string, optional
            Identifier for data type of column values. Default is String
        """
        # Ensure that a valid data type is given
        super(MimirDatasetColumn, self).__init__(
            identifier=identifier,
            name=name_in_dataset
        )
        if not name_in_rdb is None:
            self.name_in_rdb = name_in_rdb.upper()
        else:
            self.name_in_rdb = name_in_dataset.upper()
        self.data_type = data_type

    def __str__(self):
        """Human-readable string representation for the column.

        Returns
        -------
        string
        """
        name = self.name
        if not self.data_type is None:
             name += '(' + str(self.data_type) + ')'
        return name

    @staticmethod
    def from_dict(doc):
        """Create dataset column object from dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for dataset column object

        Returns
        -------
        vizier.datastore.mimir.DatasetColumn
        """
        return MimirDatasetColumn(
            doc['id'],
            doc['name'],
            doc['rdbName'],
            doc['dataType']
        )

    def is_numeric(self):
        """Flag indicating if the data type of this column is numeric, i.e.,
        integer or real.

        Returns
        -------
        bool
        """
        return self.data_type.lower() in ['int', 'real']

    def to_dict(self):
        """Get dictionary serialization for dataset column object.

        Returns
        -------
        dict
        """
        return {
            'id': self.identifier,
            'name': self.name,
            'rdbName': self.name_in_rdb,
            'dataType': self.data_type
        }

    def to_sql_value(self, value):
        """Return an SQL conform representation of the given value based on the
        column's data type.

        Raises ValueError if the column type is numeric but the given value
        cannot be converted to a numeric value.

        Parameters
        ----------
        value: string
            Dataset cell value

        Returns
        -------
        string
        """
        # If the given value is None simply return the keyword NULL
        if value is None:
            return 'NULL'
        # If the data type of the columns is numeric (int or real) try to
        # convert the given argument to check whether it actually is a numeric
        # value. Note that we always return a string beacuse the result is
        # intended to be concatenated as part of a SQL query string.
        if self.is_numeric():
            try:
                int(value)
                return str(value)
            except ValueError:
                return str(float(value))
        elif self.data_type.lower() == 'date':
            if DATE_FORMAT.match(value):
                return 'CAST(\'' + str(value) + '\' AS DATE)'
            raise ValueError('not a date \'' + str(value) + '\'')
        elif self.data_type.lower() == 'datetime':
            if DATETIME_FORMAT.match(value):
                return 'CAST(\'' + str(value) + '\' AS DATETIME)'
            raise ValueError('not a datetime \'' + str(value) + '\'')
        elif self.data_type.lower() == 'bool':
            if isinstance(value, bool):
                if value:
                    return CAST_TRUE
                else:
                    return CAST_FALSE
            elif isinstance(value, int):
                if value == 1:
                    return CAST_TRUE
                elif value == 0:
                    return CAST_FALSE
            else:
                str_val = str(value).upper()
                if str_val in ['TRUE', '1']:
                    return CAST_TRUE
                elif str_val in ['FALSE', '0']:
                    return CAST_FALSE
            # If none of the previous tests returned a bool representation we
            # raise an exception to trigger value casting.
            raise ValueError('not a boolean value \'' + str(value) + '\'')
        #elif self.data_type.lower() in ['date', 'datetime']:
            #return self.data_type.upper() + '(\'' + str(value) + '\')'
        #    return 'DATE(\'' + str(value) + '\')'
        # By default and in case the given value could not be transformed into
        # the target format return a representation for a string value
        return '\'' + str(value) + '\''



class MimirDatasetHandle(DatasetHandle):
    """Internal descriptor for datasets managed by the Mimir data store.
    Contains mapping for column names from a dataset to the corresponding object
    in a relational and a reference to the table or view that contains the
    dataset.
    """
    def __init__(
        self, identifier, columns, rowid_column, table_name, row_ids,
        column_counter, row_counter, annotations=None
    ):
        """Initialize the descriptor.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        columns: list(vizier.datastore.mimir.MimirDatasetColumn)
            List of column names in the dataset schema and their corresponding
            names in the relational database table or view.
        rowid_column: vizier.datastore.mimir.MimirDatasetColumn
            Descriptor for unique row id column
        table_name: string
            Reference to relational database table containing the dataset.
        row_ids: list(int)
            List of row ids. Determines the order of rows in the dataset
        column_counter: int
            Counter for unique column ids
        row_counter: int
            Counter for unique row ids
        annotations: vizier.datastore.metadata.DatasetMetadata
            Annotations for dataset components
        """
        super(MimirDatasetHandle, self).__init__(
            identifier=identifier,
            columns=columns,
            row_count=len(row_ids),
            column_counter=column_counter,
            row_counter=row_counter,
            annotations=annotations
        )
        self.rowid_column = rowid_column
        self.table_name = table_name
        self.row_ids = row_ids

    @staticmethod
    def from_file(filename, annotations=None):
        """Read dataset from file. Expects the file to be in Yaml format which
        is the default serialization format used by to_file().

        Parameters
        ----------
        filename: string
            Name of the file to read.
        annotations: vizier.datastore.metadata.DatasetMetadata, optional
            Annotations for dataset components
        Returns
        -------
        vizier.datastore.base.DatasetHandle
        """
        with open(filename, 'r') as f:
            doc = yaml.load(f.read())
        return MimirDatasetHandle(
            identifier=doc['id'],
            columns=[MimirDatasetColumn.from_dict(obj) for obj in doc['columns']],
            rowid_column=MimirDatasetColumn.from_dict(doc['rowIdColumn']),
            table_name=doc['tableName'],
            row_ids=doc['rows'],
            column_counter=doc['columnCounter'],
            row_counter=doc['rowCounter'],
            annotations=annotations
        )

    def get_annotations(self, column_id=-1, row_id=-1):
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
        if column_id >= 0 and row_id < 0:
            return self.annotations.for_column(column_id).values()
        elif column_id < 0 and row_id >= 0:
            return self.annotations.for_row(row_id).values()
        elif column_id >= 0 and row_id >= 0:
            cell = self.annotations.for_cell(column_id, row_id)
            # Make a copy of the annotation set and fetch annotations from
            # the Mimir backend
            annotations = ObjectMetadataSet(dict(cell.annotations))
            column = None
            for col in self.columns:
                if col.identifier == column_id:
                    column = col
                    break
            sql = 'SELECT ' + column.name_in_rdb + ' '
            sql += 'FROM ' + self.table_name + ' '
            sql += 'WHERE ' + ROW_ID + ' = ' + str(row_id)
            rs = json.loads(
                mimir._mimir.vistrailsQueryMimirJson(sql, True, False)
            )
            has_reasons = not rs['col_taint'][0][0]
            if has_reasons:
                row_prov = str(rs['prov'][0])
                buffer = mimir._mimir.explainCell(sql, 0, row_prov)
                for i in range(buffer.size()):
                    value = str(buffer.array()[i])
                    # Remove references to lenses
                    while 'LENS_' in value:
                        start_pos = value.find('LENS_')
                        end_pos = value.find('.', start_pos)
                        if end_pos > start_pos:
                            value = value[:start_pos] + value[end_pos + 1:]
                        else:
                            value = value[:start_pos]
                    # Replace references to column name
                    value = value.replace(column.name_in_rdb, column.name)
                    # Remove content in double square brackets
                    if '{{' in value:
                        value = value[:value.find('{{')].strip()
                    annotations.add(ANNO_UNCERTAIN, value)
        else:
            raise ValueError('invalid component identifier')
        return annotations.values()

    def reader(self, offset=0, limit=-1):
        """Get reader for the dataset to access the dataset rows. The optional
        offset amd limit parameters are used to retrieve only a subset of
        rows.

        Parameters
        ----------
        offset: int, optional
            Number of rows at the beginning of the list that are skipped.
        limit: int, optional
            Limits the number of rows that are returned.

        Returns
        -------
        vizier.datastore.mimir.MimirDatasetReader
        """
        return MimirDatasetReader(
            table_name=self.table_name,
            columns=self.columns,
            row_ids=self.row_ids,
            rowid_column_numeric=self.rowid_column.is_numeric(),
            offset=offset,
            limit=limit,
            annotations=self.annotations
        )

    def to_file(self, filename):
        """Write dataset to file. The default serialization format is Yaml.

        Parameters
        ----------
        filename: string
            Name of the file to write
        """
        doc = {
            'id': self.identifier,
            'columns': [col.to_dict() for col in self.columns],
            'rowIdColumn': self.rowid_column.to_dict(),
            'rows': self.row_ids,
            'tableName': str(self.table_name),
            'columnCounter': self.column_counter,
            'rowCounter': self.row_counter
        }
        with open(filename, 'w') as f:
            yaml.dump(doc, f, default_flow_style=False)


class MimirDatasetReader(DatasetReader):
    """Dataset reader for Mimir datasets."""
    def __init__(self, table_name, columns, row_ids, rowid_column_numeric=True, offset=0, limit=-1, annotations=None):
        """Initialize information about the delimited file and the file format.

        Parameters
        ----------
        table_name: string
            Name of table or view in database that contains the dataset
        columns: vizier.datastore.mimir.MimirDatasetColumn
            List of descriptors for columns in the database
        row_ids: list(int)
            Sort order for rows in the dataset
        rowid_column_numeric: bool, optional
            Flag indicating if the ROW ID column is numeric (necessary when
            generating the WHERE clause for pagination queries).
        offset: int, optional
            Number of rows at the beginning of the list that are skipped.
        limit: int, optional
            Limits the number of rows that are returned.
        annotations: vizier.datastore.metadata.DatasetMetadata, optional
            Annotations for dataset components
        """
        self.table_name = table_name
        self.columns = columns
        self.rowid_column_numeric = rowid_column_numeric
        self.annotations = annotations if not annotations is None else DatasetMetadata()
        # Convert row id list into row position index. Depending on whether
        # offset or limit parameters are given we also limit the entries in the
        # dictionary. The internal flag .is_range_query keeps track of whether
        # offset or limit where given (True) or not (False). This information is
        # later used to generate the query for the database.
        self.row_ids = dict()
        if offset > 0 or limit > 0:
            skip = offset
            count = 0
            for row_id in row_ids:
                if skip > 0:
                    skip -= 1
                else:
                    self.row_ids[row_id] = count
                    count += 1
                    if limit > 0 and count >= limit:
                        break
            self.is_range_query = True
        else:
            for i in range(len(row_ids)):
                self.row_ids[row_ids[i]] = i
            self.is_range_query = False
        # Keep an in-memory copy of the dataset rows when open
        self.is_open = False
        self.read_index = None
        self.rows = None
        # Index position of columns in dataset rows
        self.col_map = None

    def close(self):
        """Close any open files and set the is_open flag to False."""
        self.rows = None
        self.read_index = None
        self.col_map = None
        self.is_open = False

    def next(self):
        """Return the next row in the dataset iterator. Raises StopIteration if
        end of row list is reached.

        Automatically closes the reader when end of iteration is reached for
        the first time.

        Returns
        -------
        vizier.datastore.base.DatasetRow
        """
        if self.is_open:
            if self.read_index < len(self.rows):
                row = self.rows[self.read_index]
                self.read_index += 1
                return row
            self.close()
        raise StopIteration

    def open(self):
        """Setup the reader by querying the database and creating an in-memory
        copy of the dataset rows.

        Returns
        -------
        vizier.datastore.reader.MimirDatasetReader
        """
        # Query the database to retrieve dataset rows if reader is not already
        # open
        if not self.is_open:
            # Query the database to get the list of rows. Sort rows according to
            # order in row_ids and return a InMemReader
            sql = get_select_query(self.table_name, columns=self.columns)
            if self.is_range_query and self.rowid_column_numeric:
                min_id, max_id = min_max(self.row_ids.keys())
                sql += ' WHERE ' + ROW_ID + ' >= ' + str(min_id)
                sql += ' AND ' + ROW_ID + ' <= ' + str(max_id)
            rs = json.loads(
                mimir._mimir.vistrailsQueryMimirJson(sql, True, False)
            )
            # Initialize mapping of column rdb names to index positions in
            # dataset rows
            self.col_map = dict()
            for i in range(len(rs['schema'])):
                col = rs['schema'][i]
                self.col_map[col['name']] = i
            # Initialize rows (make sure to sort them according to order in
            # row_ids list), read index and open flag
            rowid_idx = self.col_map[ROW_ID]
            # Filter rows if this is a range query (needed until IN works)
            if self.is_range_query:
                rs_rows = list()
                for row in rs['data']:
                    if int(row[rowid_idx]) in self.row_ids:
                        rs_rows.append(row)
            else:
                rs_rows = rs['data']
            self.rows = list()
            for row_index in range(len(rs_rows)):
                row = rs_rows[row_index]
                row_id = int(row[self.col_map[ROW_ID]])
                values = [None] * len(self.columns)
                row_annos = [False] * len(values)
                for i in range(len(self.columns)):
                    col = self.columns[i]
                    col_index = self.col_map[col.name_in_rdb]
                    values[i] = row[col_index]
                    has_anno = self.annotations.has_cell_annotation(
                        col.identifier,
                        row_id
                    )
                    if not has_anno:
                        # Check if the cell taint is true
                        has_anno = not rs['col_taint'][row_index][col_index]
                    row_annos[i] = has_anno
                self.rows.append(DatasetRow(row_id, values, annotations=row_annos))
            self.rows.sort(key=lambda row: self.row_ids[row.identifier])
            self.read_index = 0
            self.is_open = True
        return self


class MimirDataStore(Datastore):
    """Vizier data store implementation using Mimir.

    Maintains information about the dataset schema separately. This is necessary
    because in a dataset column names are not necessarily unique. For each
    dataset a new subfolder is created in the store base directory. In that
    folder a dataset file and an annotation file are maintained. All files are
    in Yaml format.

    Note that every write_dataset call creates a new table in the underlying
    Mimir database. Other datasets are views on these tables.
    """
    def __init__(self, properties):
        """Initialize the base directory that contains the dataset index and
        metadata files.

        Parameters
        ----------
        properties : dict
            Dictionary with configuration parameters. The only parameter that
            currently is expected is 'directory'
        """
        if not PARA_DIRECTORY in properties:
            raise ValueError('missing parameter \'' + PARA_DIRECTORY + '\'')
        self.base_dir = os.path.abspath(properties[PARA_DIRECTORY])
        if not os.path.isdir(self.base_dir):
            os.makedirs(self.base_dir)

    def create_dataset(
        self, identifier=None, columns=list(), rows=list(), column_counter=0,
        row_counter=0, annotations=None
    ):
        """Create a new dataset in the data store for the given data.

        Raises ValueError if the number of values in each row of the dataset
        doesn't match the number of columns in the dataset schema.

        Parameters
        ----------
        dataset : vizier.datastore.base.DatasetHandle
            Dataset object

        Returns
        -------
        vizier.datastore.mimit.MimirDatasetHandle
        """
        # Get unique identifier for new dataset
        identifier = 'DS_' + get_unique_identifier()
        # Write rows to temporary file in CSV format
        tmp_file = get_tempfile()
        # Create a list of columns that contain the user-vizible column name and
        # the name in the database
        db_columns = list()
        for col in columns:
            db_columns.append(
                MimirDatasetColumn(
                    identifier=col.identifier,
                    name_in_dataset=col.name,
                    name_in_rdb=COL_PREFIX + str(len(db_columns))
                )
            )
        # List of row ids in the new dataset
        db_row_ids = list()
        # Create CSV file for load
        with open(tmp_file, 'w') as f_out:
            writer = csv.writer(f_out, quoting=csv.QUOTE_MINIMAL)
            writer.writerow([ROW_ID] + [col.name_in_rdb for col in db_columns])
            for row in rows:
                record = [str(row.identifier)] + encode_values(row.values)
                writer.writerow(record)
                db_row_ids.append(row.identifier)
        # Load CSV file using Mimirs loadCSV method.
        table_name = mimir._mimir.loadCSV(tmp_file, ',', True, True)
        # Delete temporary files
        os.remove(tmp_file)
        # Insert the new dataset metadata information into the datastore
        return self.register_dataset(
            table_name=table_name,
            columns=db_columns,
            row_ids=db_row_ids,
            annotations=annotations
        )

    def delete_dataset(self, identifier):
        """Delete dataset with given identifier. Returns True if dataset existed
        and False otherwise.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier.

        Returns
        -------
        bool
        """
        dataset_dir = self.get_dataset_dir(identifier)
        if os.path.isdir(dataset_dir):
            shutil.rmtree(dataset_dir)
            return True
        return False

    def get_dataset_dir(self, identifier):
        """Get the base directory for a dataset with given identifier. Having a
        separate method makes it easier to change the folder structure used to
        store datasets.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return os.path.join(self.base_dir, identifier)

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
        return os.path.join(self.get_dataset_dir(identifier), 'dataset.yaml')

    def get_dataset(self, identifier):
        """Read a full dataset from the data store. Returns None if no dataset
        with the given identifier exists.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier

        Returns
        -------
        vizier.datastore.mimir.MimirDatasetHandle
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

    def get_metadata_filename(self, identifier):
        """Get filename of meatdata file for the dataset with the given
        identifier.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return os.path.join(self.get_dataset_dir(identifier), 'annotation.yaml')

    def load_dataset(self, f_handle):
        """Create a new dataset from a given file.

        Raises ValueError if the given file could not be loaded as a dataset.

        Parameters
        ----------
        f_handle : vizier.filestore.base.FileHandle
            handle for an uploaded file on the associated file server.

        Returns
        -------
        vizier.datastore.mimir.MimirDatasetHandle
        """
        # Create a copy of the original file under a unique name. If the input
        # file is tab-delimited (and therefore has been successfully parsed on
        # upload) we create a comma-separated version. Otherwise, we simply copy
        # the given file.
        tmp_file = get_tempfile()
        if f_handle.is_verified_csv and f_handle.delimiter == '\t':
            if f_handle.compressed:
                csvfile = gzip.open(f_handle.filepath, 'rb')
            else:
                csvfile = open(f_handle.filepath, 'rb')
            reader = csv.reader(csvfile, delimiter='\t')
            with open(tmp_file, 'w') as f:
                writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                for row in reader:
                    writer.writerow(row)
                csvfile.close()
        else:
            shutil.copyfile(f_handle.filepath, tmp_file)
        # Load dataset and delete temp file
        init_load_name = mimir._mimir.loadCSV(tmp_file, ',', True, True)
        os.remove(tmp_file)
        # Retrieve schema information for the created dataset
        sql = 'SELECT * FROM ' + init_load_name
        rs = json.loads(mimir._mimir.vistrailsQueryMimirJson(sql, False, False))
        # Write retieved result to a new temporary file. During output create a
        # list of column descriptors and row ids.
        columns = list()
        row_ids = list()
        tmp_file = get_tempfile()
        with open(tmp_file, 'w') as csvfile:
            writer = unicodecsv.writer(
                csvfile,
                delimiter=',',
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
                encoding='utf-8'
            )
            # Start by creating descriptors for all columns and writing the
            # header line (out_schema). We add a column to keep track of the
            # row identifier
            out_schema = [ROW_ID.upper()]
            for col in rs['schema']:
                col_id = len(columns)
                name_in_dataset = col['name']
                name_in_rdb = COL_PREFIX + str(col_id)
                col = MimirDatasetColumn(
                    identifier=col_id,
                    name_in_dataset=name_in_dataset,
                    name_in_rdb=name_in_rdb
                )
                columns.append(col)
                out_schema.append(name_in_rdb)
            writer.writerow(out_schema)
            # Output dataset rows. Add unique row identifier for each row.
            for row in rs['data']:
                row_id = len(row_ids)
                row_ids.append(row_id)
                writer.writerow([str(row_id)] + row)
        table_name = mimir._mimir.loadCSV(tmp_file, ',', True, True)
        os.remove(tmp_file)
        # Insert the new dataset metadata information into the datastore
        return self.register_dataset(
            table_name=table_name,
            columns=columns,
            row_ids=row_ids
        )

    def register_dataset(
        self, table_name, columns, row_ids, column_counter=None,
        row_counter=None, annotations=None, update_rows=False
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
        column_counter: int
            Counter for unique column ids
        row_counter: int
            Counter for unique row ids
        annotations: vizier.datastore.metadata.DatasetMetadata
            Annotations for dataset components
        update_rows: bool, optional
            Flag indicating that the number of rows may have changed and the
            list of row identifier therefore needs to be checked.

        Returns
        -------
        vizier.datastore.mimir.MimirDatasetHandle
        """
        # Depending on whether we need to update row ids we either query the
        # database or just get the schema. In either case mimir_schema will
        # contain a the returned Mimir schema information.
        sql = get_select_query(table_name, columns=columns)
        mimir_schema = json.loads(mimir._mimir.getSchema(sql))
        if update_rows:
            sql = get_select_query(table_name)
            rs = json.loads(
                mimir._mimir.vistrailsQueryMimirJson(sql, False, False)
            )
            # Get list of row identifier in current dataset. Row ID's are
            # expected to be the only values in the returned result set.
            dataset_row_ids = set()
            for row in rs['data']:
                dataset_row_ids.add(int(row[0]))
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
            col_types[col['name']] = col['base_type']
        for col in columns:
            col.data_type = col_types[col.name_in_rdb]
        # Create column for row Identifier
        rowid_column = MimirDatasetColumn(
            name_in_dataset=ROW_ID,
            data_type=col_types[ROW_ID]
        )
        # Set column counter to max column id + 1 if None
        if column_counter is None:
            column_counter = max_column_id(columns) + 1
        # Set row counter to max. row id + 1 if None
        if row_counter is None:
            row_counter = -1
            for row_id in row_ids:
                if row_id > row_counter:
                    row_counter = row_id
            row_counter += 1
        dataset = MimirDatasetHandle(
            identifier=get_unique_identifier(),
            columns=columns,
            rowid_column=rowid_column,
            table_name=table_name,
            row_ids=row_ids,
            column_counter=column_counter,
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
        metadata_file = self.get_metadata_filename(identifier)
        if not os.path.isfile(metadata_file):
            return False
        # Read annotations from file, evaluate update statement and write result
        # back to file.
        annotations = DatasetMetadata.from_file(
            os.path.join(metadata_file)
        )
        # Get object annotations and update
        obj_annos = annotations.for_object(column_id=column_id, row_id=row_id)
        result = obj_annos.update(identifier=anno_id, key=key, value=value)
        # Write modified annotations to file
        annotations.to_file(os.path.join(metadata_file))
        return annotations



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
    sql += ' WHERE ' + ROW_ID + ' IS NULL'
    rs = json.loads(mimir._mimir.vistrailsQueryMimirJson(sql, False, False))
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
    sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + lens_name
    view_name = mimir._mimir.createView(dataset.table_name, sql)
    return view_name, dataset.row_counter + len(case_conditions)


def get_select_query(table_name, columns=None):
    """Get SQL query to select a full dataset with columns in order of their
    appearance as defined in the given column list. The first column will be
    the ROW ID.

    Parameters
    ----------
    table_name: string
        Name of the database table or view
    columns: list(vizier.datastore.mimir.MimirDatasetColumn), optional
        List of columns in the dataset

    Returns
    -------
    str
    """
    if not columns is None:
        col_list = ','.join([col.name_in_rdb for col in columns])
        return 'SELECT ' + ROW_ID + ',' + col_list + ' FROM ' + table_name
    else:
        return 'SELECT ' + ROW_ID + ' FROM ' + table_name


def get_tempfile():
    """Return the path to a temporary CSV file. Try to get a unique name to
    avoid problems with existing datasets.

    Returns
    -------
    string
    """
    tmp_prefix = 'DS_' + get_unique_identifier()
    return tempfile.mkstemp(suffix='.csv', prefix=tmp_prefix)[1]
