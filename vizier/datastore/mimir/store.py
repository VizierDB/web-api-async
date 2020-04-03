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

from io import StringIO

from vizier.core.util import dump_json, load_json
from vizier.core.util import get_unique_identifier, min_max
from vizier.core.timestamp import  get_current_time
from vizier.filestore.base import FileHandle
from vizier.datastore.annotation.dataset import DatasetMetadata
from vizier.datastore.object.dataobject import DataObjectMetadata
from vizier.datastore.object.base import DataObject
from vizier.datastore.base import DefaultDatastore
from vizier.datastore.mimir.dataset import MimirDatasetColumn, MimirDatasetHandle

import vizier.mimir as mimir
import vizier.datastore.base as helper
import vizier.datastore.mimir.base as base
from vizier.filestore.fs.base import DATA_FILENAME, write_metadata_file
import shutil
            
"""Name of file storing dataset (schema) information."""
DATASET_FILE = 'dataset.json'
DATA_OBJECT_FILE = 'dataobjects.json'

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

    def create_dataset(self, columns, rows, human_readable_name = None, annotations=None,backend_options = [], dependencies = []):
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
        colSql = ''
        for col in map(base.sanitize_column_name, columns):
            db_columns.append(
                MimirDatasetColumn(
                    identifier=col.identifier,
                    name_in_dataset=col.name,
                    name_in_rdb=col.name
                )
            )
            if colSql == '':
                colSql = col.name + ' AS ' + col.name
            else:
                colSql = colSql + ', ' + col.name + ' AS ' + col.name
        # Create CSV file for load
        with open(tmp_file, 'w') as f_out:
            writer = csv.writer(f_out, quoting=csv.QUOTE_MINIMAL)
            writer.writerow([col.name_in_rdb for col in db_columns])
            for row in rows:
                record = helper.encode_values(row.values)
                writer.writerow(record)
        # Load CSV file using Mimirs loadCSV method.
        table_name = mimir.loadDataSource(tmp_file, True, True, human_readable_name = human_readable_name, backend_options = backend_options, dependencies = dependencies)
        os.remove(tmp_file)
        sql = 'SELECT '+ colSql +' FROM {{input}};'
        view_name, dependencies = mimir.createView(table_name, sql)
        # Get number of rows in the view that was created in the backend
        row_count = mimir.countRows(view_name)
        
        # Insert the new dataset metadata information into the datastore
        return self.register_dataset(
            table_name=view_name,
            columns=db_columns,
            row_counter=row_count,
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
    
    def get_data_object_file(self, identifier):
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
        return os.path.join(self.get_dataobject_dir(identifier), DATA_OBJECT_FILE)

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
        vizier.datastore.object.dataset.DataObjectMetadata
        """
        if identifier is not None and obj_type is None and key is None:
            data_object_file = self.get_data_object_file(identifier)
            if not os.path.isfile(data_object_file):
                return DataObjectMetadata()
            else:
                return DataObjectMetadata.from_file(
                data_object_file
            )
        
        dsdirs = filter(lambda x: os.path.isdir(x) and 
                        os.path.exists(os.path.join(x,DATA_OBJECT_FILE)), 
                        [os.path.join(self.base_path, o) for o in os.listdir(self.base_path)])
        dsoids = [os.path.basename(os.path.normpath(o)) for o in dsdirs]
        dsofiles = [os.path.join(os.path.join(self.base_path,dsoid), 
                                 DATA_OBJECT_FILE) for dsoid in dsoids]
        if identifier is None and obj_type is None and key is None:
            return DataObjectMetadata(objects=[ object for objects in 
                                               [DataObjectMetadata.from_file( dsofile ).objects 
                                                for dsofile in dsofiles] for object in objects])
        elif identifier is None and obj_type is not None and key is None:
            return DataObjectMetadata(objects=[ object for objects in 
                                               [DataObjectMetadata.from_file( dsofile ).for_type(obj_type) 
                                                for dsofile in dsofiles] for object in objects])
        elif identifier is None and obj_type is None and key is not None:
            return DataObjectMetadata(objects=[ object for objects in 
                                               [DataObjectMetadata.from_file( dsofile ).for_key(key) 
                                                for dsofile in dsofiles] for object in objects])
        else:
            raise ValueError("specify only one of: identifier, obj_type or key")
    
    def update_annotation(
        self, identifier, key, old_value=None, new_value=None, column_id=None,
        row_id=None
    ):
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
        ds = self.get_dataset(identifier)
        ds_name = ds.name
        column = ds.column_by_id(column_id).name_in_rdb
        params = ['COMMENT('+column+', \''+new_value+'\', \''+str(row_id)+'\')','RESULT_COLUMNS('+str(column)+')']
        #url = 'http://localhost:5000/vizier-db/api/v1/projects/'+project_id+'/branches/'+branch_id+'/head'
        #data = {"packageId":"mimir","commandId":"comment","arguments":[{"id":"dataset","value":"mv"},{"id":"comments","value":[[{"id":"expression","value":column},{"id":"comment","value":new_value},{"id":"rowid","value":row_id}]]},{"id":"resultColumns","value":[[{"id":"column","value":column}]]},{"id":"materializeInput","value":False}]}
        #resp = requests.post(url,json=data)
       
    def update_object(
        self, identifier, key, old_value=None, new_value=None, obj_type=None
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
        # Raise ValueError if column id and row id are both None
        if identifier is None or key is None or obj_type is None:
            raise ValueError('invalid resource identifier')
        # Return None if the dataset is unknown
        
        dataobj_dir = self.get_dataobject_dir(identifier)
        data_object_filename = self.get_data_object_file(identifier)
        data_objects = None
        if not os.path.isdir(dataobj_dir):
            #it's a new object so create it
            os.makedirs(dataobj_dir)
            data_objects = DataObjectMetadata()
        else:
            # Read objects from file, Evaluate update statement and write result
            # back to file.
            data_objects = DataObjectMetadata.from_file(data_object_filename)
        # Get object annotations
        elements = data_objects.objects
        # Identify the type of operation: INSERT, DELETE or UPDATE
        if old_value is None and not new_value is None:
            elements.append(
                DataObject(
                    key=key,
                    value=new_value,
                    identifier=identifier,
                    obj_type=obj_type
                )
            )
        elif not old_value is None and new_value is None:
            del_index = None
            for i in range(len(elements)):
                a = elements[i]
                if a.identifier == identifier and a.value == old_value:
                    del_index = i
                    break
            if del_index is None:
                return False
            del elements[del_index]
        elif not old_value is None and not new_value is None:
            obj = None
            for a in elements:
                if a.identifier == identifier and a.value == old_value:
                    obj = a
                    break
            if obj is None:
                return False
            obj.value = new_value
        else:
            raise ValueError('invalid modification operation')
        # Write modified annotations to file
        data_objects.to_file(data_object_filename)
        return True   
    
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
        colSql = ''
        for col in mimirSchema:
            col_id = len(columns)
            name_in_dataset = base.sanitize_column_name(col['name'].upper())
            name_in_rdb = base.sanitize_column_name(col['name'].upper())
            col = MimirDatasetColumn(
                identifier=col_id,
                name_in_dataset=name_in_dataset,
                name_in_rdb=name_in_rdb
            )
            if colSql == '':
                colSql = name_in_dataset + ' AS ' + name_in_rdb
            else:
                colSql = colSql + ', ' + name_in_dataset + ' AS ' + name_in_rdb
            columns.append(col)
        # Create view for loaded dataset
        sql = 'SELECT '+ colSql +' FROM {{input}};'
        view_name, dependencies = mimir.createView(init_load_name, sql)
        # TODO: this is a hack to speed up this step a bit.
        #  we get the first row id and the count and take a range;
        #  this is fragile and should be made better
        #
        # NOTE: This does not work because ROW_ID appears to be a string.
        # Thus, sorting not necessarily returns the smallest integer value
        # first.
        #
        row_count = mimir.countRows(view_name)
        
        return self.register_dataset(
            table_name=view_name,
            columns=columns,
            row_counter=row_count
        )


    def unload_dataset(self, filepath, dataset_name, format='csv', options=[], filename=""):
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
        name = os.path.basename(filepath).lower()
        basepath = filepath.replace(name,"")
        
        # Create a new unique identifier for the file.
        
        abspath = os.path.abspath((r'%s' % filepath ) )
        exported_files = mimir.unloadDataSource(dataset_name, abspath, format, options)
        file_handles = []
        for output_file in exported_files:
            name = os.path.basename(output_file).lower()
            identifier = get_unique_identifier()
            file_dir = os.path.join(basepath,identifier )
            if not os.path.isdir(file_dir):
                os.makedirs(file_dir)
            fs_output_file = os.path.join(file_dir, DATA_FILENAME)
            shutil.move(os.path.join(filepath, output_file),fs_output_file)
            f_handle = FileHandle(
                    identifier,
                    output_file,
                    name
                )
            file_handles.append(f_handle )
            write_metadata_file(file_dir,f_handle)
        return file_handles
        

    def register_dataset(
        self, table_name, columns, row_counter=None, annotations=None
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
        
        # Create a mapping of column name (in database) to column type. This
        # mapping is then used to update the data type information for all
        # column descriptors.
        col_types = dict()
        for col in mimir_schema:
            col_types[base.sanitize_column_name(col['name'].upper())] = col['baseType']
        for col in columns:
            col.data_type = col_types[col.name_in_rdb]
        # Set row counter to max. row id + 1 if None
        if row_counter is None:
            row_counter = mimir.countRows(table_name)
        dataset = MimirDatasetHandle(
            identifier=get_unique_identifier(),
            columns=list(map(base.sanitize_column_name, columns)),
            table_name=table_name,
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
    view_name, dependencies = mimir.createView(dataset.table_name, sql)
    return view_name, dataset.row_counter + len(case_conditions)
