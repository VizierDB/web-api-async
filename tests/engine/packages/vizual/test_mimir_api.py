"""Test the default implementation for the vizual API."""

import os
import shutil
import unittest

from vizier.datastore.mimir.store import MimirDatastore
from vizier.engine.packages.vizual.api.base import RESOURCE_DATASET, RESOURCE_FILEID, RESOURCE_URL
from vizier.engine.packages.vizual.api.mimir import MimirVizualApi
from vizier.filestore.fs.base import FileSystemFilestore
from vizier.datastore.dataset import DatasetHandle

import vizier.mimir as mimir # noqa: F401
from vizier.mimir import MimirError

SERVER_DIR = './.tmp'
FILESTORE_DIR = './.tmp/fs'
DATASTORE_DIR = './.tmp/ds'
CSV_FILE = './tests/engine/packages/vizual/.files/dataset.csv'
SORT_FILE = './tests/engine/packages/vizual/.files/dataset_for_sort.csv'

# Note that some tests access an external resource to test download capabilities.
# The test will fail if the specified resource is not available. Set the
# DOWNLOAD_URL # to an available resource or to None to skip the download tests
DOWNLOAD_URL = None #'http://cds-swg1.cims.nyu.edu:8080/opendb-api/api/v1/datasets/w49k-mmkh/rows/download'



class TestDefaultVizualApi(unittest.TestCase):

    api: MimirVizualApi

    def setUp(self):
        """Create an instance of the default vizier API for an empty server
        directory.
        """
        # Drop directory if it exists
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.makedirs(SERVER_DIR)
        self.api = MimirVizualApi()
        self.datastore=MimirDatastore(DATASTORE_DIR)
        self.filestore=FileSystemFilestore(FILESTORE_DIR)


    def tearDown(self):
        """Clean-up by dropping the server directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_api(self):
        """Run all tests after we initialize mimir. Make sure to create a
        fresh environment after each test.
        """
        self.delete_column()
        self.setUp()
        self.delete_row()
        self.setUp()
        self.filter_columns()
        self.setUp()
        self.insert_column()
        self.setUp()
        self.insert_row()
        self.setUp()
        self.load_dataset()
        self.setUp()
        self.move_column()
        self.setUp()
        self.move_row()
        self.setUp()
        self.rename_column()
        self.setUp()
        self.sequence_of_steps()
        self.setUp()
        self.sort_dataset()
        self.setUp()
        self.update_cell()

    def delete_column(self):
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
        row_ids = [row.identifier for row in ds_rows]
        # Delete Age column
        col_id = ds.column_by_name('AGE').identifier
        result = self.api.delete_column(ds.identifier, col_id, self.datastore)
        # Resulting dataset should differ from previous one
        self.assertNotEqual(result.dataset.identifier, ds.identifier)
        # Retrieve modified dataset and ensure that it cobtains the following
        #
        # Name, Salary
        # ------------
        # Alice, 35K
        # Bob, 30K
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        # Schema is Name, Salary
        self.assertEqual(len(ds.columns), 2)
        self.assertEqual(ds.columns[0].name.upper(), 'NAME')
        self.assertEqual(ds.columns[1].name.upper(), 'SALARY')
        # Make sure that all rows only have two columns
        row = ds_rows[0]
        self.assertEqual(len(row.values), 2)
        self.assertEqual(len(row.values), 2)
        self.assertEqual(row.values[0], 'Alice')
        self.assertEqual(row.values[1], '35K')
        row = ds_rows[1]
        self.assertEqual(len(row.values), 2)
        self.assertEqual(len(row.values), 2)
        self.assertEqual(row.values[0], 'Bob')
        self.assertEqual(row.values[1], '30K')
        # Ensure that row identifier haven't changed
        for i in range(len(ds_rows)):
            self.assertEqual(ds_rows[i].identifier, row_ids[i])
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(MimirError):
            self.api.delete_column('unknown:uri', 0, self.datastore)
        # Ensure exception is thrown if column identifier is unknown
        with self.assertRaises(ValueError):
            self.api.delete_column(ds.identifier, 100, self.datastore)

    def delete_row(self):
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
        result = self.api.delete_row(ds.identifier, row_ids[1], self.datastore)
        del row_ids[1]
        # Resulting dataset should differ from previous one
        self.assertNotEqual(result.dataset.identifier, ds.identifier)
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
        self.assertEqual(len(ds.columns), len(col_names))
        for i in range(len(ds.columns)):
            self.assertEqual(ds.columns[i].name.upper(), col_names[i].upper())
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEqual(ds.columns[i].identifier, col_ids[i])
        # There should only be one row
        self.assertEqual(len(ds_rows), 1)
        # Ensure that row identifier haven't changed
        for i in range(len(ds_rows)):
            self.assertEqual(ds_rows[i].identifier, row_ids[i])
        # Ensure exception is thrown if dataset is unknown
        with self.assertRaises(MimirError):
            self.api.delete_row('unknown:uri', 0, self.datastore)

    def filter_columns(self):
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
        self.assertEqual(len(ds.columns), 2)
        self.assertEqual(ds.columns[0].name.upper(), 'BD')
        self.assertEqual(ds.columns[1].name.upper(), 'NAME')
        rows = ds.fetch_rows()
        self.assertEqual(rows[0].values, ['35K', 'Alice'])
        self.assertEqual(rows[1].values, ['30K', 'Bob'])

    def insert_column(self):
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
        col_ids.insert(1, ds.max_column_id() + 1)
        result = self.api.insert_column(ds.identifier, 1, 'Height', self.datastore)
        # Resulting dataset should differ from previous one
        self.assertNotEqual(result.dataset.identifier, ds.identifier)
        # Retrieve dataset and ensure that it has the following schema:
        # Name, Height, Age, Salary
        ds = self.datastore.get_dataset(result.dataset.identifier)
        col_names = ['Name' ,'Height', 'Age', 'Salary']
        # Ensure that there are four rows
        self.assertEqual(len(ds.columns), len(col_names))
        print(ds.columns)
        for i in range(len(col_names)):
            col = ds.columns[i]
            self.assertEqual(col.name.upper(), col_names[i].upper())
        # Insert columns at last position
        col_ids.append(ds.max_column_id() + 1)
        col_names.append('Weight')
        result = self.api.insert_column(ds.identifier, 4, 'Weight', self.datastore)
        # Resulting dataset should differ from previous one
        self.assertNotEqual(result.dataset.identifier, ds.identifier)
        # Retrieve dataset and ensure that it has the following schema:
        # Name, Height, Age, Salary, Weight
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        # Ensure that there are five rows
        self.assertEqual(len(ds.columns), len(col_names))
        for i in range(len(col_names)):
            col = ds.columns[i]
            self.assertEqual(col.name.upper(), col_names[i].upper())
        # The cell values for new columns are None all other values are not None
        for row in ds_rows:
            for i in range(len(ds.columns)):
                if i == 1 or i == 4:
                    self.assertIsNone(row.values[i])
                else:
                    self.assertTrue(row.values[i])
        # Ensure that row identifier haven't changed
        for i in range(len(ds_rows)):
            self.assertEqual(ds_rows[i].identifier, row_ids[i])
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(MimirError):
            self.api.insert_column('unknown:uri', 1, 'Height', self.datastore)
        # Ensure exception is thrown if column name is invalid
        self.api.insert_column(ds.identifier, 1, 'Height_from_ground', self.datastore)
        with self.assertRaises(ValueError):
            self.api.insert_column(ds.identifier, 1, 'Height from ground!@#', self.datastore)
        # Ensure exception is thrown if column position is out of bounds
        with self.assertRaises(ValueError):
            self.api.insert_column(ds.identifier, 100, 'Height', self.datastore)

    def insert_row(self):
        """Test functionality to insert a row."""
        fh = self.filestore.upload_file(CSV_FILE)
        ds = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            file_id=fh.identifier
        ).dataset
        # Keep track of column and row identifier
        ds_rows = ds.fetch_rows()
        row_ids = [row.identifier for row in ds_rows]
        # Insert row at index position 1
        row_ids.insert(1, None)
        # Result should indicate that one row was inserted. The identifier of
        # the resulting dataset should differ from the identifier of the
        # original dataset
        result = self.api.insert_row(ds.identifier, 1, self.datastore)
        # Resulting dataset should differ from previous one
        self.assertNotEqual(result.dataset.identifier, ds.identifier)
        # Retrieve modified dataset
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        # Ensure that there are three rows
        self.assertEqual(len(ds_rows), 3)
        # The second row has empty values for each column
        row = ds_rows[1]
        self.assertEqual(len(row.values), len(ds.columns))
        for i in range(len(ds.columns)):
            self.assertIsNone(row.values[i])
        # Append row at end current dataset
        row_ids.append(None)
        result = self.api.insert_row(ds.identifier, 3, self.datastore)
        # Resulting dataset should differ from previous one
        self.assertNotEqual(result.dataset.identifier, ds.identifier)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        # Ensure that there are three rows
        self.assertEqual(len(ds_rows), 4)
        # The next to last row has non-empty values for each column
        row = ds_rows[2]
        self.assertEqual(len(row.values), len(ds.columns))
        for i in range(len(ds.columns)):
            self.assertIsNotNone(row.values[i])
        # The last row has empty values for each column
        row = ds_rows[3]
        self.assertEqual(len(row.values), len(ds.columns))
        for i in range(len(ds.columns)):
            self.assertIsNone(row.values[i])
        # Ensure that row ids haven't changed
        # ## July 16, 2020 by OK: Bug in mimir that is going to take a bunch of
        # ## heavy lifting to fix: https://github.com/UBOdin/mimir-api/issues/11
        # for i in range(len(ds_rows)):
        #     if row_ids[i] is not None:
        #         self.assertEqual(int(ds_rows[i].identifier), int(row_ids[i]))
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(MimirError):
            self.api.insert_row('unknown:uri', 1, self.datastore)
        # Ensure no exception is raised
        self.api.insert_row(ds.identifier, 4, self.datastore)

    def load_dataset(self) -> None:
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
        assert(isinstance(ds, DatasetHandle))
        ds_rows = ds.fetch_rows()
        self.assertEqual(len(ds.columns), 3)
        self.assertEqual(len(ds_rows), 2)
        for row in ds_rows:
            self.assertTrue(isinstance(row.values[1], int))
        self.assertIsNotNone(resources)
        self.assertEqual(resources[RESOURCE_FILEID], fh.identifier)
        self.assertEqual(resources[RESOURCE_DATASET], ds.identifier)
        # Delete file handle and oing the same should raise an exception
        self.filestore.delete_file(fh.identifier)
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
        # Test loading file from external resource. Skip if DOWNLOAD_URL is None
        if DOWNLOAD_URL is None:
            print('Skipping download test')
            return
        result = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            url=DOWNLOAD_URL,
            options=[{'delimiter':'\t'}]
        )
        ds = result.dataset
        resources = result.resources
        ds_rows = ds.fetch_rows()
        self.assertEqual(len(ds.columns), 4)
        self.assertEqual(len(ds_rows), 54)
        self.assertIsNotNone(resources)
        self.assertEqual(resources[RESOURCE_URL], DOWNLOAD_URL)
        self.assertEqual(resources[RESOURCE_DATASET], ds.identifier)
        # Attempt to simulate re-running without downloading again. Set the
        # Uri to some fake Uri that would raise an exception if an attempt was
        # made to download
        url = 'some fake uri'
        resources[RESOURCE_URL] = url
        result = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            url=url,
            resources=resources
        )
        prev_id = result.dataset.identifier
        self.assertEqual(result.dataset.identifier, prev_id)
        # If we re-run with reload flag true a new dataset should be returned
        resources[RESOURCE_URL] = DOWNLOAD_URL
        result = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            url=DOWNLOAD_URL,
            resources=resources,
            reload=True,
            options=[{'delimiter':'\t'}]
        )
        self.assertNotEqual(result.dataset.identifier, prev_id)

    def move_column(self):
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
        self.assertNotEqual(result.dataset.identifier, ds.identifier)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        self.assertEqual(ds.columns[0].name.upper(), 'Age'.upper())
        self.assertEqual(ds.columns[1].name.upper(), 'Name'.upper())
        self.assertEqual(ds.columns[2].name.upper(), 'Salary'.upper())
        row = ds_rows[0]
        self.assertEqual(row.values[0], 23)
        self.assertEqual(row.values[1], 'Alice')
        self.assertEqual(row.values[2], '35K')
        row = ds_rows[1]
        self.assertEqual(row.values[0], 32)
        self.assertEqual(row.values[1], 'Bob')
        self.assertEqual(row.values[2], '30K')
        # Ensure that row ids haven't changed
        for i in range(len(ds_rows)):
            self.assertEqual(int(ds_rows[i].identifier), int(row_ids[i]))
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
        self.assertEqual(ds.columns[0].name.upper(), 'Age'.upper())
        self.assertEqual(ds.columns[1].name.upper(), 'Salary'.upper())
        self.assertEqual(ds.columns[2].name.upper(), 'Name'.upper())
        row = ds_rows[0]
        self.assertEqual(row.values[0], 23)
        self.assertEqual(row.values[1], '35K')
        self.assertEqual(row.values[2], 'Alice')
        row = ds_rows[1]
        self.assertEqual(row.values[0], 32)
        self.assertEqual(row.values[1], '30K')
        self.assertEqual(row.values[2], 'Bob')
        # Ensure that row ids haven't changed
        for i in range(len(ds_rows)):
            self.assertEqual(int(ds_rows[i].identifier), int(row_ids[i]))
        # No changes if source and target position are the same
        result = self.api.move_column(
            ds.identifier,
            ds.columns[1].identifier,
            1,
            self.datastore
        )
        self.assertEqual(ds.identifier, result.dataset.identifier)
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(MimirError):
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

    def move_row(self):
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
        row_ids = [row.identifier for row in ds_rows]
        # Swap first two rows
        result = self.api.move_row(ds.identifier, row_ids[0], 1, self.datastore)
        row_ids = [row for row in reversed(row_ids)]
        self.assertNotEqual(result.dataset.identifier, ds.identifier)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        self.assertEqual(ds.columns[0].name.upper(), 'Name'.upper())
        self.assertEqual(ds.columns[1].name.upper(), 'Age'.upper())
        self.assertEqual(ds.columns[2].name.upper(), 'Salary'.upper())
        row = ds_rows[0]
        self.assertEqual(row.values[0], 'Bob')
        self.assertEqual(row.values[1], 32)
        self.assertEqual(row.values[2], '30K')
        row = ds_rows[1]
        self.assertEqual(row.values[0], 'Alice')
        self.assertEqual(row.values[1], 23)
        self.assertEqual(row.values[2], '35K')
        # Ensure that row ids haven't changed
        for i in range(len(ds_rows)):
            self.assertEqual(ds_rows[i].identifier, row_ids[i])
        # Swap last two rows
        result = self.api.move_row(ds.identifier, row_ids[1], 0, self.datastore)
        row_ids = [row for row in reversed(row_ids)]
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        self.assertEqual(ds.columns[0].name.upper(), 'Name'.upper())
        self.assertEqual(ds.columns[1].name.upper(), 'Age'.upper())
        self.assertEqual(ds.columns[2].name.upper(), 'Salary'.upper())
        row = ds_rows[0]
        self.assertEqual(row.values[0], 'Alice')
        self.assertEqual(row.values[1], 23)
        self.assertEqual(row.values[2], '35K')
        row = ds_rows[1]
        self.assertEqual(row.values[0], 'Bob')
        self.assertEqual(row.values[1], 32)
        self.assertEqual(row.values[2], '30K')
        # Ensure that row ids haven't changed
        for i in range(len(ds_rows)):
            self.assertEqual(int(ds_rows[i].identifier), int(row_ids[i]))
        # Move first row to the end
        result = self.api.move_row(ds.identifier, row_ids[0], 2, self.datastore)
        row_ids = [row for row in reversed(row_ids)]
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        row = ds_rows[0]
        self.assertEqual(row.values[0], 'Bob')
        self.assertEqual(row.values[1], 32)
        self.assertEqual(row.values[2], '30K')
        row = ds_rows[1]
        self.assertEqual(row.values[0], 'Alice')
        self.assertEqual(row.values[1], 23)
        self.assertEqual(row.values[2], '35K')
        # Ensure that row ids haven't changed

        # ## July 16, 2020 by OK: Bug in mimir that is going to take a bunch of
        # ## heavy lifting to fix: https://github.com/UBOdin/mimir-api/issues/11
        # for i in range(len(ds_rows)):
        #     self.assertEqual(int(ds_rows[i].identifier), int(row_ids[i]))
        # No changes if source and target position are the same

        result = self.api.move_row(ds.identifier, row_ids[1], 1, self.datastore)

        # ## July 21, 2020 by OK: It would be fantastic if we could easily detect
        # no-op vizual, but for now skip this check
        #self.assertEqual(ds.identifier, result.dataset.identifier)

        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(MimirError):
            self.api.move_row('unknown:uri', 0, 1, self.datastore)
        # Raise error if target position is out of bounds
        # ## July 21, 2020 by OK: Skipping this check for now
        # with self.assertRaises(ValueError):
        #     self.api.move_row(ds.identifier, 0, -1, self.datastore)
        # with self.assertRaises(ValueError):
        #     self.api.move_row(ds.identifier, 1, 4, self.datastore)

    def rename_column(self):
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
        row_ids = [row.identifier for row in ds_rows]
        # Rename first column to Firstname
        result = self.api.rename_column(
            ds.identifier,
            ds.column_by_name('Name').identifier,
            'Firstname',
            self.datastore
        )
        self.assertNotEqual(result.dataset.identifier, ds.identifier)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        self.assertEqual(ds.columns[0].name.upper(), 'Firstname'.upper())
        self.assertEqual(ds.columns[1].name.upper(), 'Age'.upper())
        self.assertEqual(ds.columns[2].name.upper(), 'Salary'.upper())
        result = self.api.rename_column(
            ds.identifier,
            ds.column_by_name('Age').identifier,
            'BDate',
            self.datastore
        )
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        self.assertEqual(ds.columns[0].name.upper(), 'Firstname'.upper())
        self.assertEqual(ds.columns[1].name, 'BDate')
        self.assertEqual(ds.columns[2].name.upper(), 'Salary'.upper())
        # Ensure that row ids haven't changed
        for i in range(len(ds_rows)):
            self.assertEqual(int(ds_rows[i].identifier), int(row_ids[i]))
        # No changes if the old and new column name are the same (with exception
        # to upper and lower cases).
        result = self.api.rename_column(
            ds.identifier,
            ds.column_by_name('BDate').identifier,
            'BDate',
            self.datastore
        )
        # ## July 21, 2020 by OK: It would be fantastic if we could easily detect
        # no-op vizual, but for now skip this check
        # self.assertEqual(ds.identifier, result.dataset.identifier)
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(MimirError):
            self.api.rename_column('unknown:uri', 0, 'Firstname', self.datastore)
        # Ensure exception is thrown for invalid column id
        with self.assertRaises(ValueError):
            self.api.rename_column(ds.identifier, 500, 'BDate', self.datastore)

    def sequence_of_steps(self):
        """Test sequence of calls that modify a dataset."""
        # Create a new dataset
        fh = self.filestore.upload_file(CSV_FILE)
        ds = self.api.load_dataset(
            datastore=self.datastore,
            filestore=self.filestore,
            file_id=fh.identifier
        ).dataset
        ds = self.api.insert_row(ds.identifier, 1, self.datastore).dataset
        row_ids = [ row.identifier for row in ds.fetch_rows() ]
        row0 = row_ids[0]
        row1 = row_ids[1]
        row2 = row_ids[2]
        ds = self.api.insert_column(ds.identifier, 3, 'HDate', self.datastore).dataset
        ds = self.api.update_cell(ds.identifier, ds.column_by_name('HDate').identifier, row0, '180', self.datastore).dataset
        ds = self.api.update_cell(ds.identifier, ds.column_by_name('HDate').identifier, row2, '160', self.datastore).dataset
        ds = self.api.rename_column(ds.identifier, ds.column_by_name('HDate').identifier, 'Height', self.datastore).dataset
        ds = self.api.update_cell(ds.identifier, ds.column_by_name('Height').identifier, row1, '170', self.datastore).dataset
        ds = self.api.move_row(ds.identifier, row1, 2, self.datastore).dataset
        ds = self.api.update_cell(ds.identifier, ds.column_by_name('Name').identifier,row2, 'Carla', self.datastore).dataset
        ds = self.api.update_cell(ds.identifier, ds.column_by_name('Age').identifier, row2, '45', self.datastore).dataset
        ds = self.api.update_cell(ds.identifier, ds.column_by_name('Salary').identifier, row2, '56K', self.datastore).dataset
        ds = self.api.move_column(ds.identifier, ds.column_by_name('Salary').identifier, 4, self.datastore).dataset
        ds = self.api.delete_column(ds.identifier, ds.column_by_name('Age').identifier, self.datastore).dataset
        ds = self.api.delete_row(ds.identifier, row0, self.datastore).dataset
        ds = self.api.delete_row(ds.identifier, row1, self.datastore).dataset
        ds = self.datastore.get_dataset(ds.identifier)
        ds_rows = ds.fetch_rows()
        names = ['Name', 'Height', 'Salary']
        self.assertEqual(len(ds.columns), len(names))
        for i in range(len(names)):
            col = ds.columns[i]
            self.assertEqual(col.name.upper(), names[i].upper())
        self.assertEqual(len(ds_rows), 1)
        self.assertEqual(ds_rows[0].values, ['Carla', '160', '56K'])

    def sort_dataset(self):
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
            self.assertEqual(names[i], result[i])
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
            self.assertEqual(names[i], result[i])
        # Raises error for invalid column identifier
        with self.assertRaises(ValueError):
            self.api.sort_dataset(ds.identifier, [2, 10, 0], [True, False, True], self.datastore)

    def update_cell(self):
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
        row_ids = [row.identifier for row in ds_rows]
        # Update cell [0, 0]. Ensure that one row was updated and a new
        # identifier is generated. Also ensure that the resulting datasets
        # has the new value in cell [0, 0]
        row_id = row_ids[0]
        result = self.api.update_cell(ds.identifier, 0, row_id, 'MyValue', self.datastore)
        self.assertNotEqual(ds.identifier, result.dataset.identifier)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        row = None
        for r in ds.fetch_rows():
            if r.identifier == row_id:
                row = r
                break
        self.assertEqual(row.values[0], 'MyValue')
        result = self.api.update_cell(ds.identifier, ds.column_by_name('Name').identifier, row_id, 'AValue', self.datastore)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        row = None
        for r in ds.fetch_rows():
            if r.identifier == row_id:
                row = r
                break
        self.assertEqual(row.values[0], 'AValue')
        self.assertEqual(row.values[ds.column_index('Name')], 'AValue')
        # Ensure that row ids haven't changed
        for i in range(len(ds_rows)):
            self.assertEqual(int(ds_rows[i].identifier), int(row_ids[i]))
        # Set value to None
        result = self.api.update_cell(ds.identifier, ds.column_by_name('Name').identifier, row_id, None, self.datastore)
        ds = self.datastore.get_dataset(result.dataset.identifier)
        ds_rows = ds.fetch_rows()
        row = None
        for r in ds.fetch_rows():
            if r.identifier == row_id:
                row = r
                break
        self.assertIsNone(row.values[0])
        self.assertIsNone(row.values[ds.column_index('Name')])
        # Ensure exception is thrown if dataset is unknown
        with self.assertRaises(MimirError):
            self.api.update_cell('unknown:uri', 0, 0, 'MyValue', self.datastore)


if __name__ == '__main__':
    unittest.main()
