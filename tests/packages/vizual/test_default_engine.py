import os
import shutil
import unittest

from vizier.datastore.fs.base import FileSystemDatastore
from vizier.engine.packages.vizual.api.fs import DefaultVizualApi
from vizier.filestore.fs.base import DefaultFilestore


SERVER_DIR = './.tmp'
FILESTORE_DIR = './.tmp/fs'
DATASTORE_DIR = './.tmp/ds'
CSV_FILE = './.files/dataset.csv'
SORT_FILE = './.files/dataset_for_sort.csv'


class TestDefaultVizualEngine(unittest.TestCase):

    def setUp(self):
        """Create an instance of the default vizier API for an empty server
        directory.
        """
        # Drop directory if it exists
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.makedirs(SERVER_DIR)
        self.api = DefaultVizualApi(
            datastore=FileSystemDatastore(DATASTORE_DIR),
            filestore=DefaultFilestore(FILESTORE_DIR)
        )

    def tearDown(self):
        """Clean-up by dropping the server directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_delete_column(self):
        """Test functionality to delete a column."""
        # Create a new dataset
        ds = self.api.load_dataset(self.api.filestore.upload_file(CSV_FILE))
        ds_rows = ds.fetch_rows()
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds_rows]
        # Delete Age column
        col_id = ds.column_by_name('AGE').identifier
        col_count, id1 = self.vizual.delete_column(ds.identifier, col_id)
        del col_ids[1]
        # Result should indicate that one column was deleted. The identifier of
        # the resulting dataset should differ from the identifier of the
        # original dataset
        self.assertEquals(col_count, 1)
        self.assertNotEquals(id1, ds.identifier)
        # Retrieve modified dataset and ensure that it cobtains the following
        #
        # Name, Salary
        # ------------
        # Alice, 35K
        # Bob, 30K
        ds = self.datastore.get_dataset(id1)
        ds_rows = ds.fetch_rows()
        # Schema is Name, Salary
        self.assertEquals(len(ds.columns), 2)
        self.assertEquals(ds.columns[0].name.upper(), 'NAME')
        self.assertEquals(ds.columns[1].name.upper(), 'SALARY')
        # Make sure column identifier haven't changed
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
            self.vizual.delete_column('unknown:uri', 0)
        self.tear_down(engine)

    def delete_row(self, engine):
        """Test functionality to delete a row."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        ds_rows = ds.fetch_rows()
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds_rows]
        # Delete second row
        row_count, id1 = self.vizual.delete_row(ds.identifier, 1)
        del row_ids[1]
        # Result should indicate that one row was deleted. The identifier of the
        #  resulting dataset should differ from the identifier of the original
        # dataset
        self.assertEquals(row_count, 1)
        self.assertNotEquals(id1, ds.identifier)
        # Retrieve modified dataset and ensure that it contains the following
        # data:
        #
        # Name, Age, Salary
        # ------------
        # Alice, 23, 35K
        ds = self.datastore.get_dataset(id1)
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
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.vizual.delete_row('unknown:uri', 1)
        # Ensure exception is thrown if row index is out of bounds
        with self.assertRaises(ValueError):
            self.vizual.delete_row(ds.identifier, 100)
        self.tear_down(engine)

    def filter_columns(self, engine):
        """Test projection of a dataset."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        count, ds_id = self.vizual.filter_columns(ds.identifier, [2, 0], ['BD', None])
        ds = self.datastore.get_dataset(ds_id)
        self.assertEquals(len(ds.columns), 2)
        self.assertEquals(ds.columns[0].identifier, 2)
        self.assertEquals(ds.columns[0].name.upper(), 'BD')
        self.assertEquals(ds.columns[1].identifier, 0)
        self.assertEquals(ds.columns[1].name.upper(), 'NAME')
        rows = ds.fetch_rows()
        self.assertEquals(rows[0].values, ['35K', 'Alice'])
        self.assertEquals(rows[1].values, ['30K', 'Bob'])
        with self.assertRaises(ValueError):
            self.vizual.filter_columns(ds.identifier, [0, 1], ['BD', None])

        self.tear_down(engine)

    def insert_column(self, engine):
        """Test functionality to insert a columns."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        ds_rows = ds.fetch_rows()
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds_rows]
        # Insert columns at position 1
        col_ids.insert(1, ds.column_counter)
        col_count, id1 = self.vizual.insert_column(ds.identifier, 1, 'Height')
        # Result should indicate that one column was inserted. The identifier of
        # the resulting dataset should differ from the identifier of the
        # original dataset
        self.assertEquals(col_count, 1)
        self.assertNotEquals(id1, ds.identifier)
        # Retrieve dataset and ensure that it has the following schema:
        # Name, Height, Age, Salary
        ds = self.datastore.get_dataset(id1)
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
        col_count, id2 = self.vizual.insert_column(id1, 4, 'Weight')
        # Result should indicate that one column was deleted. The identifier of
        # the resulting dataset should differ from the identifier of the
        # previous dataset
        self.assertEquals(col_count, 1)
        self.assertNotEquals(id1, id2)
        # Retrieve dataset and ensure that it has the following schema:
        # Name, Height, Age, Salary, Weight
        ds = self.datastore.get_dataset(id2)
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
                    self.assertTrue(is_null(row.values[i]))
                else:
                    self.assertFalse(is_null(row.values[i]))
        # Ensure that row identifier haven't changed
        for i in range(len(ds_rows)):
            self.assertEquals(ds_rows[i].identifier, row_ids[i])
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.vizual.insert_column('unknown:uri', 1, 'Height')
        # Ensure exception is thrown if column name is invalid
        self.vizual.insert_column(ds.identifier, 1, 'Height from ground')
        with self.assertRaises(ValueError):
            self.vizual.insert_column(ds.identifier, 1, 'Height from ground!@#')
        # Ensure exception is thrown if column position is out of bounds
        with self.assertRaises(ValueError):
            self.vizual.insert_column(ds.identifier, 100, 'Height')
        self.tear_down(engine)

    def insert_row(self, engine):
        """Test functionality to insert a row."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        ds_rows = ds.fetch_rows()
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds_rows]
        # Insert row at index position 1
        row_ids.insert(1, ds.row_counter)
        # Result should indicate that one row was inserted. The identifier of
        # the resulting dataset should differ from the identifier of the
        # original dataset
        row_count, id1 = self.vizual.insert_row(ds.identifier, 1)
        self.assertEquals(row_count, 1)
        self.assertNotEquals(id1, ds.identifier)
        # Retrieve modified dataset
        ds = self.datastore.get_dataset(id1)
        ds_rows = ds.fetch_rows()
        # Ensure that there are three rows
        self.assertEquals(len(ds_rows), 3)
        # The second row has empty values for each column
        row = ds_rows[1]
        self.assertEquals(len(row.values), len(ds.columns))
        for i in range(len(ds.columns)):
            self.assertTrue(is_null(row.values[i]))
        # Append row at end current dataset
        row_ids.append(ds.row_counter)
        row_count, id2 = self.vizual.insert_row(id1, 3)
        self.assertEquals(row_count, 1)
        self.assertNotEquals(id1, id2)
        ds = self.datastore.get_dataset(id2)
        ds_rows = ds.fetch_rows()
        # Ensure that there are three rows
        self.assertEquals(len(ds_rows), 4)
        # The next to last row has non-empty values for each column
        row = ds_rows[2]
        self.assertEquals(len(row.values), len(ds.columns))
        for i in range(len(ds.columns)):
            self.assertFalse(is_null(row.values[i]))
        # The last row has empty values for each column
        row = ds_rows[3]
        self.assertEquals(len(row.values), len(ds.columns))
        for i in range(len(ds.columns)):
            self.assertTrue(is_null(row.values[i]))
        # Ensure that row ids haven't changed
        for i in range(len(ds_rows)):
            self.assertEquals(ds_rows[i].identifier, row_ids[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.vizual.insert_row('unknown:uri', 1)
        # Ensure exception is thrown if row index is out of bounds
        with self.assertRaises(ValueError):
            self.vizual.insert_row(ds.identifier, 5)
        # Ensure no exception is raised
        self.vizual.insert_row(ds.identifier, 4)
        self.tear_down(engine)

    def load_dataset(self, engine):
        """Test functionality to load a dataset."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        ds_rows = ds.fetch_rows()
        self.assertEquals(len(ds.columns), 3)
        self.assertEquals(len(ds_rows), 2)
        for row in ds_rows:
            self.assertTrue(isinstance(row.values[1], int))
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.vizual.load_dataset('unknown:uri')
        self.tear_down(engine)

    def move_column(self, engine):
        """Test functionality to move a column."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        ds_rows = ds.fetch_rows()
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds_rows]
        # Swap first two columns
        c = col_ids[0]
        del col_ids[0]
        col_ids.insert(1, c)
        col_count, id1 = self.vizual.move_column(ds.identifier, ds.column_by_name('Name').identifier, 1)
        self.assertEquals(col_count, 1)
        self.assertNotEquals(id1, ds.identifier)
        ds = self.datastore.get_dataset(id1)
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
        col_count, id2 = self.vizual.move_column(id1, ds.column_by_name('Salary').identifier, 1)
        ds = self.datastore.get_dataset(id2)
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
        # Raise error if source column is out of bounds
        with self.assertRaises(ValueError):
            self.vizual.move_column(id2, 40, 1)
        # Raise error if target position is out of bounds
        with self.assertRaises(ValueError):
            self.vizual.move_column(id2, ds.column_by_name('Name').identifier, -1)
        with self.assertRaises(ValueError):
            self.vizual.move_column(id2, ds.column_by_name('Name').identifier, 4)
        self.tear_down(engine)

    def move_row(self, engine):
        """Test functionality to move a row."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        ds_rows = ds.fetch_rows()
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds_rows]
        # Swap first two rows
        row_ids = [row for row in reversed(row_ids)]
        row_count, id1 = self.vizual.move_row(ds.identifier, 0, 1)
        self.assertEquals(row_count, 1)
        self.assertNotEquals(id1, ds.identifier)
        ds = self.datastore.get_dataset(id1)
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
        row_count, id2 = self.vizual.move_row(id1, 1, 0)
        ds = self.datastore.get_dataset(id2)
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
        row_count, id3 = self.vizual.move_row(id2, 0, 2)
        row_ids = [row for row in reversed(row_ids)]
        ds = self.datastore.get_dataset(id3)
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
        # Raise error if source row is out of bounds
        with self.assertRaises(ValueError):
            self.vizual.move_row(id2, 3, 1)
        # Raise error if target position is out of bounds
        with self.assertRaises(ValueError):
            self.vizual.move_row(id2, 0, -1)
        with self.assertRaises(ValueError):
            self.vizual.move_row(id2, 1, 4)
        self.tear_down(engine)

    def rename_column(self, engine):
        """Test functionality to rename a column."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        ds_rows = ds.fetch_rows()
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds_rows]
        # Rename first column to Firstname
        col_count, id1 = self.vizual.rename_column(ds.identifier, ds.column_by_name('Name').identifier, 'Firstname')
        self.assertEquals(col_count, 1)
        self.assertNotEquals(id1, ds.identifier)
        ds = self.datastore.get_dataset(id1)
        self.assertEquals(ds.columns[0].name.upper(), 'Firstname'.upper())
        self.assertEquals(ds.columns[1].name.upper(), 'Age'.upper())
        self.assertEquals(ds.columns[2].name.upper(), 'Salary'.upper())
        col_count, id2 = self.vizual.rename_column(id1, ds.column_by_name('Age').identifier, 'BDate')
        ds = self.datastore.get_dataset(id2)
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
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.vizual.rename_column('unknown:uri', 0, 'Firstname')
        # Ensure exception is thrown for invalid column id
        with self.assertRaises(ValueError):
            self.vizual.rename_column(id2, 500, 'BDate')
        self.tear_down(engine)

    def sequence_of_steps(self, engine):
        """Test sequence of calls that modify a dataset."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        count, ds_id = self.vizual.insert_row(ds.identifier, 1)
        ds = self.datastore.get_dataset(ds_id)
        count, ds_id = self.vizual.insert_column(ds_id, 3, 'HDate')
        ds = self.datastore.get_dataset(ds_id)
        count, ds_id = self.vizual.update_cell(ds_id, ds.column_by_name('HDate').identifier, 0, '180')
        ds = self.datastore.get_dataset(ds_id)
        count, ds_id = self.vizual.update_cell(ds_id, ds.column_by_name('HDate').identifier, 1, '160')
        ds = self.datastore.get_dataset(ds_id)
        count, ds_id = self.vizual.rename_column(ds_id, ds.column_by_name('HDate').identifier, 'Height')
        ds = self.datastore.get_dataset(ds_id)
        count, ds_id = self.vizual.update_cell(ds_id, ds.column_by_name('Height').identifier, 2, '170')
        ds = self.datastore.get_dataset(ds_id)
        count, ds_id = self.vizual.move_row(ds_id, 1, 2)
        ds = self.datastore.get_dataset(ds_id)
        count, ds_id = self.vizual.update_cell(ds_id, ds.column_by_name('Name').identifier, 2, 'Carla')
        ds = self.datastore.get_dataset(ds_id)
        count, ds_id = self.vizual.update_cell(ds_id, ds.column_by_name('Age').identifier, 2, '45')
        ds = self.datastore.get_dataset(ds_id)
        count, ds_id = self.vizual.update_cell(ds_id, ds.column_by_name('Salary').identifier, 2, '56K')
        ds = self.datastore.get_dataset(ds_id)
        count, ds_id = self.vizual.move_column(ds_id, ds.column_by_name('Salary').identifier, 4)
        ds = self.datastore.get_dataset(ds_id)
        count, ds_id = self.vizual.delete_column(ds_id, ds.column_by_name('Age').identifier)
        ds = self.datastore.get_dataset(ds_id)
        count, ds_id = self.vizual.delete_row(ds_id, 0)
        ds = self.datastore.get_dataset(ds_id)
        count, ds_id = self.vizual.delete_row(ds_id, 0)
        ds = self.datastore.get_dataset(ds_id)
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
        self.tear_down(engine)

    def sort_dataset(self, engine):
        """Test sorting a dataset."""
        self.set_up(engine)
        # Create a new dataset
        fh = self.fs.upload_file(SORT_FILE)
        ds = self.vizual.load_dataset(fh.identifier)
        count, ds_id = self.vizual.sort_dataset(ds.identifier, [1, 2, 0], [False, False, True])
        ds = self.datastore.get_dataset(ds_id)
        rows = ds.fetch_rows()
        names = ['Alice', 'Bob', 'Dave', 'Gertrud', 'Frank']
        result = list()
        for row in rows:
            name = row.values[0]
            if name in names:
                result.append(name)
        for i in range(len(names)):
            self.assertEquals(names[i], result[i])
        count, ds_id = self.vizual.sort_dataset(ds.identifier, [2, 1, 0], [True, False, True])
        ds = self.datastore.get_dataset(ds_id)
        rows = ds.fetch_rows()
        names = ['Gertrud', 'Frank', 'Bob', 'Alice', 'Dave']
        result = list()
        for row in rows:
            name = row.values[0]
            if name in names:
                result.append(name)
        for i in range(len(names)):
            self.assertEquals(names[i], result[i])
        self.tear_down(engine)

    def update_cell(self, engine):
        """Test functionality to update a dataset cell."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        ds_rows = ds.fetch_rows()
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds_rows]
        # Update cell [0, 0]. Ensure that one row was updated and a new
        # identifier is generated. Also ensure that the resulting datasets
        # has the new value in cell [0, 0]
        upd_rows, id1 = self.vizual.update_cell(ds.identifier, 0, 0, 'MyValue')
        self.assertEquals(upd_rows, 1)
        self.assertNotEquals(ds.identifier, id1)
        ds = self.datastore.get_dataset(id1)
        ds_rows = ds.fetch_rows()
        self.assertEquals(ds_rows[0].values[0], 'MyValue')
        upd_rows, id2 = self.vizual.update_cell(id1, ds.column_by_name('Name').identifier, 0, 'AValue')
        ds = self.datastore.get_dataset(id2)
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
        upd_rows, id3 = self.vizual.update_cell(id2, ds.column_by_name('Name').identifier, 0, None)
        ds = self.datastore.get_dataset(id3)
        ds_rows = ds.fetch_rows()
        self.assertIsNone(ds_rows[0].values[0])
        self.assertIsNone(ds_rows[0].values[ds.column_index('Name')])
        # Ensure exception is thrown if column is unknown
        with self.assertRaises(ValueError):
            self.vizual.update_cell(ds.identifier, 100, 0, 'MyValue')
        # Ensure exception is thrown if row index is out ouf bounds
        with self.assertRaises(ValueError):
            self.vizual.update_cell(ds.identifier, 0, 100, 'MyValue')
        self.tear_down(engine)

if __name__ == '__main__':
    unittest.main()
