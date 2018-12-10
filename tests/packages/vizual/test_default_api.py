"""Test the default implementation for the vizual API."""

import os
import shutil
import unittest

from vizier.datastore.fs.base import FileSystemDatastore
from vizier.engine.packages.vizual.api.fs import DefaultVizualApi
from vizier.engine.packages.vizual.api.fs import RESOURCE_DATASET, RESOURCE_FILEID, RESOURCE_URI
from vizier.filestore.fs.base import DefaultFilestore


SERVER_DIR = './.tmp'
FILESTORE_DIR = './.tmp/fs'
DATASTORE_DIR = './.tmp/ds'
CSV_FILE = './.files/dataset.csv'
SORT_FILE = './.files/dataset_for_sort.csv'

# Note that some tests access an external resource to test download capabilities.
# The test will fail if the specified resource is not available. Set the URI
# to an available resource or to None to skip the download tests
URI = 'http://cds-swg1.cims.nyu.edu:8080/opendb-api/api/v1/datasets/w49k-mmkh/rows/download'



class TestDefaultVizualApi(unittest.TestCase):
    def setUp(self):
        """Create an instance of the default vizier API for an empty server
        directory.
        """
        # Drop directory if it exists
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.makedirs(SERVER_DIR)
        self.api = DefaultVizualApi()
        self.datastore=FileSystemDatastore(DATASTORE_DIR)
        self.filestore=DefaultFilestore(FILESTORE_DIR)


    def tearDown(self):
        """Clean-up by dropping the server directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_delete_column(self):
        """Test functionality to delete a column."""
        # Create a new dataset
        fh = self.filestore.upload_file(CSV_FILE)
        ds = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            file_id=fh.identifier
        ).dataset
        ds_rows = ds.fetch_rows()
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds_rows]
        # Delete Age column
        col_id = ds.column_by_name('AGE').identifier
        result = self.api.delete_column(ds.identifier, col_id, self.datastore)
        # Resulting dataset should differ from previous one
        self.assertNotEquals(result.dataset.identifier, ds.identifier)
        # Retrieve modified dataset and ensure that it cobtains the following
        #
        # Name, Salary
        # ------------
        # Alice, 35K
        # Bob, 30K
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        # Schema is Name, Salary
        self.assertEquals(len(ds.columns), 2)
        self.assertEquals(ds.columns[0].name.upper(), 'NAME')
        self.assertEquals(ds.columns[1].name.upper(), 'SALARY')
        # Make sure column identifier haven't changed
        del col_ids[1]
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # Make sure that all rows only have two columns
        row = ds_rows[0]
        self.assertEquals(len(row.values), 2)
        self.assertEquals(len(row.values), 2)
        self.assertEquals(row.values[0], 'Alice')
        self.assertEquals(row.values[1], '35K')
        row = ds_rows[1]
        self.assertEquals(len(row.values), 2)
        self.assertEquals(len(row.values), 2)
        self.assertEquals(row.values[0], 'Bob')
        self.assertEquals(row.values[1], '30K')
        # Ensure that row identifier haven't changed
        for i in range(len(ds_rows)):
            self.assertEquals(ds_rows[i].identifier, row_ids[i])
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.api.delete_column('unknown:uri', 0, self.datastore)
        # Ensure exception is thrown if column identifier is unknown
        with self.assertRaises(ValueError):
            self.api.delete_column(ds.identifier, col_id, self.datastore)

    def test_delete_row(self):
        """Test functionality to delete a row."""
        # Create a new dataset
        fh = self.filestore.upload_file(CSV_FILE)
        ds = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            file_id=fh.identifier
        ).dataset
        ds_rows = ds.fetch_rows()
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds_rows]
        # Delete second row
        result = self.api.delete_row(ds.identifier, 1, self.datastore)
        del row_ids[1]
        # Resulting dataset should differ from previous one
        self.assertNotEquals(result.dataset.identifier, ds.identifier)
        # Retrieve modified dataset and ensure that it contains the following
        # data:
        #
        # Name, Age, Salary
        # ------------
        # Alice, 23, 35K
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        # Schema is Name, Salary
        col_names = ['Name', 'Age', 'Salary']
        self.assertEquals(len(ds.columns), len(col_names))
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].name.upper(), col_names[i].upper())
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # There should only be one row
        self.assertEquals(len(ds_rows), 1)
        # Ensure that row identifier haven't changed
        for i in range(len(ds_rows)):
            self.assertEquals(ds_rows[i].identifier, row_ids[i])
        # Ensure exception is thrown if dataset is unknown
        with self.assertRaises(ValueError):
            self.api.delete_row('unknown:uri', 0, self.datastore)
        # Ensure exception is thrown if row index is out of bounds
        with self.assertRaises(ValueError):
            self.api.delete_row(ds.identifier, 100, self.datastore)

    def test_filter_columns(self):
        """Test projection of a dataset."""
        # Create a new dataset
        fh = self.filestore.upload_file(CSV_FILE)
        ds = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            file_id=fh.identifier
        ).dataset
        result = self.api.filter_columns(ds.identifier, [2, 0], ['BD', None], self.datastore)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        self.assertEquals(len(ds.columns), 2)
        self.assertEquals(ds.columns[0].identifier, 2)
        self.assertEquals(ds.columns[0].name.upper(), 'BD')
        self.assertEquals(ds.columns[1].identifier, 0)
        self.assertEquals(ds.columns[1].name.upper(), 'NAME')
        rows = ds.fetch_rows()
        self.assertEquals(rows[0].values, ['35K', 'Alice'])
        self.assertEquals(rows[1].values, ['30K', 'Bob'])
        with self.assertRaises(ValueError):
            self.api.filter_columns(ds.identifier, [0, 1], ['BD', None], self.datastore)

    def test_insert_column(self):
        """Test functionality to insert a columns."""
        # Create a new dataset
        fh = self.filestore.upload_file(CSV_FILE)
        ds = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            file_id=fh.identifier
        ).dataset
        ds_rows = ds.fetch_rows()
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds_rows]
        # Insert columns at position 1
        col_ids.insert(1, ds.column_counter)
        result = self.api.insert_column(ds.identifier, 1, 'Height', self.datastore)
        # Resulting dataset should differ from previous one
        self.assertNotEquals(result.dataset.identifier, ds.identifier)
        # Retrieve dataset and ensure that it has the following schema:
        # Name, Height, Age, Salary
        ds = self.datastore.get_dataset(result.dataset.identifier)
        col_names = ['Name' ,'Height', 'Age', 'Salary']
        # Ensure that there are four rows
        self.assertEquals(len(ds.columns), len(col_names))
        for i in range(len(col_names)):
            col = ds.columns[i]
            self.assertEquals(col.identifier, col_ids[i])
            self.assertEquals(col.name.upper(), col_names[i].upper())
        # Insert columns at last position
        col_ids.append(ds.column_counter)
        col_names.append('Weight')
        result = self.api.insert_column(ds.identifier, 4, 'Weight', self.datastore)
        # Resulting dataset should differ from previous one
        self.assertNotEquals(result.dataset.identifier, ds.identifier)
        # Retrieve dataset and ensure that it has the following schema:
        # Name, Height, Age, Salary, Weight
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        # Ensure that there are five rows
        self.assertEquals(len(ds.columns), len(col_names))
        for i in range(len(col_names)):
            col = ds.columns[i]
            self.assertEquals(col.identifier, col_ids[i])
            self.assertEquals(col.name.upper(), col_names[i].upper())
        # The cell values for new columns are None all other values are not None
        for row in ds_rows:
            for i in range(len(ds.columns)):
                if i == 1 or i == 4:
                    self.assertIsNone(row.values[i])
                else:
                    self.assertTrue(row.values[i])
        # Ensure that row identifier haven't changed
        for i in range(len(ds_rows)):
            self.assertEquals(ds_rows[i].identifier, row_ids[i])
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.api.insert_column('unknown:uri', 1, 'Height', self.datastore)
        # Ensure exception is thrown if column name is invalid
        self.api.insert_column(ds.identifier, 1, 'Height from ground', self.datastore)
        with self.assertRaises(ValueError):
            self.api.insert_column(ds.identifier, 1, 'Height from ground!@#', self.datastore)
        # Ensure exception is thrown if column position is out of bounds
        with self.assertRaises(ValueError):
            self.api.insert_column(ds.identifier, 100, 'Height', self.datastore)

    def test_insert_row(self):
        """Test functionality to insert a row."""
        # Create a new dataset
        fh = self.filestore.upload_file(CSV_FILE)
        ds = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            file_id=fh.identifier
        ).dataset
        # Keep track of column and row identifier
        ds_rows = ds.fetch_rows()
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds_rows]
        # Insert row at index position 1
        row_ids.insert(1, ds.row_counter)
        # Result should indicate that one row was inserted. The identifier of
        # the resulting dataset should differ from the identifier of the
        # original dataset
        result = self.api.insert_row(ds.identifier, 1, self.datastore)
        # Resulting dataset should differ from previous one
        self.assertNotEquals(result.dataset.identifier, ds.identifier)
        # Retrieve modified dataset
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        # Ensure that there are three rows
        self.assertEquals(len(ds_rows), 3)
        # The second row has empty values for each column
        row = ds_rows[1]
        self.assertEquals(len(row.values), len(ds.columns))
        for i in range(len(ds.columns)):
            self.assertIsNone(row.values[i])
        # Append row at end current dataset
        row_ids.append(ds.row_counter)
        result = self.api.insert_row(ds.identifier, 3, self.datastore)
        # Resulting dataset should differ from previous one
        self.assertNotEquals(result.dataset.identifier, ds.identifier)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        # Ensure that there are three rows
        self.assertEquals(len(ds_rows), 4)
        # The next to last row has non-empty values for each column
        row = ds_rows[2]
        self.assertEquals(len(row.values), len(ds.columns))
        for i in range(len(ds.columns)):
            self.assertIsNotNone(row.values[i])
        # The last row has empty values for each column
        row = ds_rows[3]
        self.assertEquals(len(row.values), len(ds.columns))
        for i in range(len(ds.columns)):
            self.assertIsNone(row.values[i])
        # Ensure that row ids haven't changed
        for i in range(len(ds_rows)):
            self.assertEquals(ds_rows[i].identifier, row_ids[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.api.insert_row('unknown:uri', 1, self.datastore)
        # Ensure exception is thrown if row index is out of bounds
        with self.assertRaises(ValueError):
            self.api.insert_row(ds.identifier, 5, self.datastore)
        # Ensure no exception is raised
        self.api.insert_row(ds.identifier, 4, self.datastore)

    def test_load_dataset(self):
        """Test functionality to load a dataset."""
        # Create a new dataset
        fh = self.filestore.upload_file(CSV_FILE)
        result = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            file_id=fh.identifier
        )
        ds = result.dataset
        resources = result.resources
        ds_rows = ds.fetch_rows()
        self.assertEquals(len(ds.columns), 3)
        self.assertEquals(len(ds_rows), 2)
        for row in ds_rows:
            self.assertTrue(isinstance(row.values[1], int))
        self.assertIsNotNone(resources)
        self.assertEquals(resources[RESOURCE_FILEID], fh.identifier)
        self.assertEquals(resources[RESOURCE_DATASET], ds.identifier)
        # Delete file handle and run load_dataset again with returned resource
        # information. This should not raise an error since the file is not
        # accessed but the previous dataset reused.
        self.filestore.delete_file(fh.identifier)
        result = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            file_id=fh.identifier,
            resources=resources
        )
        self.assertEquals(result.dataset.identifier, ds.identifier)
        # Doing the same without the resources should raise an exception
        with self.assertRaises(ValueError):
            self.api.load_dataset(
                datastore=self.datastore,
                filestore=self.filestore,
                file_id=fh.identifier
            )
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.api.load_dataset(
                datastore=self.datastore,
                filestore=self.filestore,
                file_id='unknown:uri'
            )
        # Test loading file from external resource. Skip if URI is None
        if URI is None:
            print 'Skipping download test'
            return
        result = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            uri=URI
        )
        ds = result.dataset
        resources = result.resources
        ds_rows = ds.fetch_rows()
        self.assertEquals(len(ds.columns), 4)
        self.assertEquals(len(ds_rows), 54)
        self.assertIsNotNone(resources)
        self.assertEquals(resources[RESOURCE_URI], URI)
        self.assertEquals(resources[RESOURCE_DATASET], ds.identifier)
        # Attempt to simulate re-running without downloading again. Set the
        # Uri to some fake Uri that would raise an exception if an attempt was
        # made to download
        uri = 'some fake uri'
        resources[RESOURCE_URI] = uri
        result = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            uri=uri,
            resources=resources
        )
        prev_id = result.dataset.identifier
        self.assertEquals(result.dataset.identifier, prev_id)
        # If we re-run with reload flag true a new dataset should be returned
        resources[RESOURCE_URI] = URI
        result = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            uri=URI,
            resources=resources,
            reload=True
        )
        self.assertNotEqual(result.dataset.identifier, prev_id)

    def test_move_column(self):
        """Test functionality to move a column."""
        # Create a new dataset
        fh = self.filestore.upload_file(CSV_FILE)
        ds = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            file_id=fh.identifier
        ).dataset
        ds_rows = ds.fetch_rows()
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds_rows]
        # Swap first two columns
        c = col_ids[0]
        del col_ids[0]
        col_ids.insert(1, c)
        result = self.api.move_column(
            ds.identifier,
            ds.column_by_name('Name').identifier,
            1,
            self.datastore
        )
        self.assertNotEquals(result.dataset.identifier, ds.identifier)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        self.assertEquals(ds.columns[0].name.upper(), 'Age'.upper())
        self.assertEquals(ds.columns[1].name.upper(), 'Name'.upper())
        self.assertEquals(ds.columns[2].name.upper(), 'Salary'.upper())
        row = ds_rows[0]
        self.assertEquals(row.values[0], 23)
        self.assertEquals(row.values[1], 'Alice')
        self.assertEquals(row.values[2], '35K')
        row = ds_rows[1]
        self.assertEquals(row.values[0], 32)
        self.assertEquals(row.values[1], 'Bob')
        self.assertEquals(row.values[2], '30K')
        # Ensure that row ids haven't changed
        for i in range(len(ds_rows)):
            self.assertEquals(ds_rows[i].identifier, row_ids[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # Swap last two columns
        c = col_ids[1]
        del col_ids[1]
        col_ids.append(c)
        result = self.api.move_column(
            ds.identifier,
            ds.column_by_name('Salary').identifier,
            1,
            self.datastore
        )
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        self.assertEquals(ds.columns[0].name.upper(), 'Age'.upper())
        self.assertEquals(ds.columns[1].name.upper(), 'Salary'.upper())
        self.assertEquals(ds.columns[2].name.upper(), 'Name'.upper())
        row = ds_rows[0]
        self.assertEquals(row.values[0], 23)
        self.assertEquals(row.values[1], '35K')
        self.assertEquals(row.values[2], 'Alice')
        row = ds_rows[1]
        self.assertEquals(row.values[0], 32)
        self.assertEquals(row.values[1], '30K')
        self.assertEquals(row.values[2], 'Bob')
        # Ensure that row ids haven't changed
        for i in range(len(ds_rows)):
            self.assertEquals(ds_rows[i].identifier, row_ids[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # No changes if source and target position are the same
        result = self.api.move_column(
            ds.identifier,
            ds.columns[1].identifier,
            1,
            self.datastore
        )
        self.assertEquals(ds.identifier, result.dataset.identifier)
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.api.move_column('unknown:uri', 0, 1, self.datastore)
        # Raise error if source column is out of bounds
        with self.assertRaises(ValueError):
            self.api.move_column(ds.identifier, 40, 1, self.datastore)
        # Raise error if target position is out of bounds
        with self.assertRaises(ValueError):
            self.api.move_column(
                ds.identifier,
                ds.column_by_name('Name').identifier,
                -1,
                self.datastore
            )
        with self.assertRaises(ValueError):
            self.api.move_column(
                ds.identifier,
                ds.column_by_name('Name').identifier,
                4,
                self.datastore
            )

    def test_move_row(self):
        """Test functionality to move a row."""
        # Create a new dataset
        fh = self.filestore.upload_file(CSV_FILE)
        ds = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            file_id=fh.identifier
        ).dataset
        ds_rows = ds.fetch_rows()
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds_rows]
        # Swap first two rows
        row_ids = [row for row in reversed(row_ids)]
        result = self.api.move_row(ds.identifier, 0, 1, self.datastore)
        self.assertNotEquals(result.dataset.identifier, ds.identifier)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        self.assertEquals(ds.columns[0].name.upper(), 'Name'.upper())
        self.assertEquals(ds.columns[1].name.upper(), 'Age'.upper())
        self.assertEquals(ds.columns[2].name.upper(), 'Salary'.upper())
        row = ds_rows[0]
        self.assertEquals(row.values[0], 'Bob')
        self.assertEquals(row.values[1], 32)
        self.assertEquals(row.values[2], '30K')
        row = ds_rows[1]
        self.assertEquals(row.values[0], 'Alice')
        self.assertEquals(row.values[1], 23)
        self.assertEquals(row.values[2], '35K')
        # Ensure that row ids haven't changed
        for i in range(len(ds_rows)):
            self.assertEquals(ds_rows[i].identifier, row_ids[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # Swap last two rows
        row_ids = [row for row in reversed(row_ids)]
        result = self.api.move_row(ds.identifier, 1, 0, self.datastore)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        self.assertEquals(ds.columns[0].name.upper(), 'Name'.upper())
        self.assertEquals(ds.columns[1].name.upper(), 'Age'.upper())
        self.assertEquals(ds.columns[2].name.upper(), 'Salary'.upper())
        row = ds_rows[0]
        self.assertEquals(row.values[0], 'Alice')
        self.assertEquals(row.values[1], 23)
        self.assertEquals(row.values[2], '35K')
        row = ds_rows[1]
        self.assertEquals(row.values[0], 'Bob')
        self.assertEquals(row.values[1], 32)
        self.assertEquals(row.values[2], '30K')
        # Ensure that row ids haven't changed
        for i in range(len(ds_rows)):
            self.assertEquals(ds_rows[i].identifier, row_ids[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # Move first row to the end
        result = self.api.move_row(ds.identifier, 0, 2, self.datastore)
        row_ids = [row for row in reversed(row_ids)]
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        row = ds_rows[0]
        self.assertEquals(row.values[0], 'Bob')
        self.assertEquals(row.values[1], 32)
        self.assertEquals(row.values[2], '30K')
        row = ds_rows[1]
        self.assertEquals(row.values[0], 'Alice')
        self.assertEquals(row.values[1], 23)
        self.assertEquals(row.values[2], '35K')
        # Ensure that row ids haven't changed
        for i in range(len(ds_rows)):
            self.assertEquals(ds_rows[i].identifier, row_ids[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # No changes if source and target position are the same
        result = self.api.move_row(ds.identifier, 1, 1, self.datastore)
        self.assertEquals(ds.identifier, result.dataset.identifier)
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.api.move_row('unknown:uri', 0, 1, self.datastore)
        # Raise error if source row is out of bounds
        with self.assertRaises(ValueError):
            self.api.move_row(ds.identifier, 3, 1, self.datastore)
        # Raise error if target position is out of bounds
        with self.assertRaises(ValueError):
            self.api.move_row(ds.identifier, 0, -1, self.datastore)
        with self.assertRaises(ValueError):
            self.api.move_row(ds.identifier, 1, 4, self.datastore)

    def test_rename_column(self):
        """Test functionality to rename a column."""
        # Create a new dataset
        fh = self.filestore.upload_file(CSV_FILE)
        ds = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            file_id=fh.identifier
        ).dataset
        ds_rows = ds.fetch_rows()
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds_rows]
        # Rename first column to Firstname
        result = self.api.rename_column(
            ds.identifier,
            ds.column_by_name('Name').identifier,
            'Firstname',
            self.datastore
        )
        self.assertNotEquals(result.dataset.identifier, ds.identifier)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        self.assertEquals(ds.columns[0].name.upper(), 'Firstname'.upper())
        self.assertEquals(ds.columns[1].name.upper(), 'Age'.upper())
        self.assertEquals(ds.columns[2].name.upper(), 'Salary'.upper())
        result = self.api.rename_column(
            ds.identifier,
            ds.column_by_name('Age').identifier,
            'BDate',
            self.datastore
        )
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        self.assertEquals(ds.columns[0].name.upper(), 'Firstname'.upper())
        self.assertEquals(ds.columns[1].name, 'BDate')
        self.assertEquals(ds.columns[2].name.upper(), 'Salary'.upper())
        # Ensure that row ids haven't changed
        for i in range(len(ds_rows)):
            self.assertEquals(ds_rows[i].identifier, row_ids[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # No changes if the old and new column name are the same (with exception
        # to upper and lower cases).
        result = self.api.rename_column(
            ds.identifier,
            ds.column_by_name('BDate').identifier,
            'BDate',
            self.datastore
        )
        self.assertEquals(ds.identifier, result.dataset.identifier)
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.api.rename_column('unknown:uri', 0, 'Firstname', self.datastore)
        # Ensure exception is thrown for invalid column id
        with self.assertRaises(ValueError):
            self.api.rename_column(ds.identifier, 500, 'BDate', self.datastore)

    def test_sequence_of_steps(self):
        """Test sequence of calls that modify a dataset."""
        # Create a new dataset
        fh = self.filestore.upload_file(CSV_FILE)
        ds = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            file_id=fh.identifier
        ).dataset
        ds = self.api.insert_row(ds.identifier, 1, self.datastore).dataset
        ds = self.api.insert_column(ds.identifier, 3, 'HDate', self.datastore).dataset
        ds = self.api.update_cell(ds.identifier, ds.column_by_name('HDate').identifier, 0, '180', self.datastore).dataset
        ds = self.api.update_cell(ds.identifier, ds.column_by_name('HDate').identifier, 1, '160', self.datastore).dataset
        ds = self.api.rename_column(ds.identifier, ds.column_by_name('HDate').identifier, 'Height', self.datastore).dataset
        ds = self.api.update_cell(ds.identifier, ds.column_by_name('Height').identifier, 2, '170', self.datastore).dataset
        ds = self.api.move_row(ds.identifier, 1, 2, self.datastore).dataset
        ds = self.api.update_cell(ds.identifier, ds.column_by_name('Name').identifier, 2, 'Carla', self.datastore).dataset
        ds = self.api.update_cell(ds.identifier, ds.column_by_name('Age').identifier, 2, '45', self.datastore).dataset
        ds = self.api.update_cell(ds.identifier, ds.column_by_name('Salary').identifier, 2, '56K', self.datastore).dataset
        ds = self.api.move_column(ds.identifier, ds.column_by_name('Salary').identifier, 4, self.datastore).dataset
        ds = self.api.delete_column(ds.identifier, ds.column_by_name('Age').identifier, self.datastore).dataset
        ds = self.api.delete_row(ds.identifier, 0, self.datastore).dataset
        ds = self.api.delete_row(ds.identifier, 0, self.datastore).dataset
        ds_rows = ds.fetch_rows()
        names = ['Name', 'Height', 'Salary']
        self.assertEquals(len(ds.columns), len(names))
        for i in range(len(names)):
            col = ds.columns[i]
            self.assertEquals(col.name.upper(), names[i].upper())
        self.assertEquals([col.identifier for col in ds.columns], [0, 3, 2])
        self.assertEquals(len(ds_rows), 1)
        self.assertEquals(ds_rows[0].values, ['Carla', '160', '56K'])
        self.assertEquals(ds_rows[0].identifier, 2)

    def test_sort_dataset(self):
        """Test sorting a dataset."""
        # Create a new dataset
        fh = self.filestore.upload_file(SORT_FILE)
        ds = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            file_id=fh.identifier
        ).dataset
        result = self.api.sort_dataset(ds.identifier, [1, 2, 0], [False, False, True], self.datastore)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        rows = ds.fetch_rows()
        names = ['Alice', 'Bob', 'Dave', 'Gertrud', 'Frank']
        result = list()
        for row in rows:
            name = row.values[0]
            if name in names:
                result.append(name)
        for i in range(len(names)):
            self.assertEquals(names[i], result[i])
        result = self.api.sort_dataset(ds.identifier, [2, 1, 0], [True, False, True], self.datastore)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        rows = ds.fetch_rows()
        names = ['Gertrud', 'Frank', 'Bob', 'Alice', 'Dave']
        result = list()
        for row in rows:
            name = row.values[0]
            if name in names:
                result.append(name)
        for i in range(len(names)):
            self.assertEquals(names[i], result[i])
        # Raises error for invalid column identifier
        with self.assertRaises(ValueError):
            self.api.sort_dataset(ds.identifier, [2, 10, 0], [True, False, True], self.datastore)

    def test_update_cell(self):
        """Test functionality to update a dataset cell."""
        # Create a new dataset
        fh = self.filestore.upload_file(CSV_FILE)
        ds = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            file_id=fh.identifier
        ).dataset
        ds_rows = ds.fetch_rows()
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds_rows]
        # Update cell [0, 0]. Ensure that one row was updated and a new
        # identifier is generated. Also ensure that the resulting datasets
        # has the new value in cell [0, 0]
        result = self.api.update_cell(ds.identifier, 0, 0, 'MyValue', self.datastore)
        self.assertNotEquals(ds.identifier, result.dataset.identifier)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        self.assertEquals(ds_rows[0].values[0], 'MyValue')
        result = self.api.update_cell(ds.identifier, ds.column_by_name('Name').identifier, 0, 'AValue', self.datastore)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        self.assertEquals(ds_rows[0].values[0], 'AValue')
        self.assertEquals(ds_rows[0].values[ds.column_index('Name')], 'AValue')
        # Ensure that row ids haven't changed
        for i in range(len(ds_rows)):
            self.assertEquals(ds_rows[i].identifier, row_ids[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # Set value to None
        result = self.api.update_cell(ds.identifier, ds.column_by_name('Name').identifier, 0, None, self.datastore)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        self.assertIsNone(ds_rows[0].values[0])
        self.assertIsNone(ds_rows[0].values[ds.column_index('Name')])
        # Ensure exception is thrown if dataset is unknown
        with self.assertRaises(ValueError):
            self.api.update_cell('unknown:uri', 0, 0, 'MyValue', self.datastore)
        # Ensure exception is thrown if column is unknown
        with self.assertRaises(ValueError):
            self.api.update_cell(ds.identifier, 100, 0, 'MyValue', self.datastore)
        # Ensure exception is thrown if row index is out ouf bounds
        with self.assertRaises(ValueError):
            self.api.update_cell(ds.identifier, 0, 100, 'MyValue', self.datastore)


if __name__ == '__main__':
    unittest.main()
