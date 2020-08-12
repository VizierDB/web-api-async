"""Test functionality of the default datastore."""

import os
import shutil
import unittest

from vizier.datastore.base import METADATA_FILE
from vizier.datastore.dataset import DatasetColumn, DatasetRow
from vizier.datastore.fs.base import FileSystemDatastore
from vizier.datastore.fs.base import DATA_FILE, DESCRIPTOR_FILE
from vizier.datastore.fs.base import validate_dataset
from vizier.filestore.fs.base import FileSystemFilestore
from vizier.filestore.base import FileHandle, FORMAT_TSV

BASE_DIR = './.tmp'
STORE_DIR = './.tmp/ds'
FSSTORE_DIR = './.tmp/fs'

FILE = FileHandle(
    identifier='0000',
    filepath='./tests/test_data/r.csv',
    file_name='r.csv'
)

# Note that some tests access an external resource to test download capabilities.
# The test will fail if the specified resource is not available. Set the
# DOWNLOAD_URL to an available resource or to None to skip the download tests
DOWNLOAD_URL = 'https://github.com/UBOdin/mimir-api/raw/master/test_data/r.csv'


EXAMPLE_PROPERTIES = {
  'columns': [
      { 'name': 'A',
        'structural_type': 'http://schema.org/Integer',
        'semantic_types': [],
        'unclean_values_ratio': 0.0,
        'num_distinct_values': 25,
        'mean': 1.0,
        'stddev': 0.0,
        'plot': { 'type': 'histogram_numerical',
                  'data': [
                    {'count': 9, 'bin_start': 20.0, 'bin_end': 23.5},
                    {'count': 13, 'bin_start': 23.5, 'bin_end': 27.0},
                    {'count': 15, 'bin_start': 27.0, 'bin_end': 30.5},
                    {'count': 10, 'bin_start': 30.5, 'bin_end': 34.0},
                    {'count': 11, 'bin_start': 34.0, 'bin_end': 37.5},
                    {'count': 5, 'bin_start': 37.5, 'bin_end': 41.0},
                    {'count': 7, 'bin_start': 41.0, 'bin_end': 44.5},
                    {'count': 1, 'bin_start': 44.5, 'bin_end': 48.0},
                    {'count': 0, 'bin_start': 48.0, 'bin_end': 51.5},
                    {'count': 1, 'bin_start': 51.5, 'bin_end': 55.0}
                ]},
        'coverage': [
          {'range': {'gte': 21.0, 'lte': 29.0}},
          {'range': {'gte': 30.0, 'lte': 38.0}},
          {'range': {'gte': 40.0, 'lte': 55.0}}
        ]
      },
      { 'name' : 'B', 'structural_type' : 'http://schema.org/Integer' }
  ]
}


class TestFileSystemDatastore(unittest.TestCase):

    def setUp(self):
        """Create an empty datastore directory."""
        # Delete datastore directory if it exists
        if os.path.isdir(BASE_DIR):
            shutil.rmtree(BASE_DIR)
        # Create new datastore directory
        os.makedirs(BASE_DIR)
        os.makedirs(STORE_DIR)
        os.makedirs(FSSTORE_DIR)

    def tearDown(self):
        """Clean-up by deleting the datastore directory.
        """
        if os.path.isdir(BASE_DIR):
            shutil.rmtree(BASE_DIR)

    def test_properties(self):
        """Test loading a dataset from file."""
        store = FileSystemDatastore(STORE_DIR)
        ds = store.create_dataset(
            columns=[
                DatasetColumn(identifier=0, name='A'),
                DatasetColumn(identifier=1, name='B')
            ],
            rows=[DatasetRow(identifier=0, values=[1, 2])],
            properties=EXAMPLE_PROPERTIES
        )
        ds = store.get_dataset(ds.identifier)
        column_props = ds.properties['columns']
        self.assertEqual(len(column_props), 2)
        self.assertTrue('A' in [prop['name'] for prop in column_props])
        # Reload datastore
        store = FileSystemDatastore(STORE_DIR)
        ds = store.get_dataset(ds.identifier)
        column_props = ds.properties['columns']
        self.assertEqual(len(column_props), 2)

    def test_create_dataset(self):
        """Test loading a dataset from file."""
        store = FileSystemDatastore(STORE_DIR)
        ds = store.create_dataset(
            columns=[
                DatasetColumn(identifier=0, name='A'),
                DatasetColumn(identifier=1, name='B')
            ],
            rows=[DatasetRow(identifier=0, values=['a', 'b'])]
        )
        ds = store.get_dataset(ds.identifier)
        column_ids = [col.identifier for col in ds.columns]
        self.assertEqual(len(ds.columns), 2)
        for id in [0, 1]:
            self.assertTrue(id in column_ids)
        column_names = [col.name for col in ds.columns]
        for name in ['A', 'B']:
            self.assertTrue(name in column_names)
        rows = ds.fetch_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].values, ['a', 'b'])
        self.assertEqual(len(ds.properties), 0)
        # Reload the datastore
        store = FileSystemDatastore(STORE_DIR)
        ds = store.get_dataset(ds.identifier)
        column_ids = [col.identifier for col in ds.columns]
        self.assertEqual(len(ds.columns), 2)
        for id in [0, 1]:
            self.assertTrue(id in column_ids)
        column_names = [col.name for col in ds.columns]
        for name in ['A', 'B']:
            self.assertTrue(name in column_names)
        rows = ds.fetch_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].values, ['a', 'b'])
        self.assertEqual(len(ds.properties), 0)

    def test_create_base(self):
        """Test that the datastore base directory is created if it does not
        exist.
        """
        self.assertTrue(os.path.isdir(STORE_DIR))
        shutil.rmtree(STORE_DIR)
        self.assertFalse(os.path.isdir(STORE_DIR))
        store = FileSystemDatastore(STORE_DIR)
        self.assertTrue(os.path.isdir(STORE_DIR))

    def test_delete_dataset(self):
        """Test deleting datasets."""
        # None for non-existing dataset
        store = FileSystemDatastore(STORE_DIR)
        ds_id = store.load_dataset(f_handle=FILE).identifier
        ds_id_2 =  store.load_dataset(f_handle=FILE).identifier
        self.assertIsNotNone(store.get_dataset(ds_id))
        self.assertIsNotNone(store.get_dataset(ds_id_2))
        store.delete_dataset(ds_id)
        self.assertIsNone(store.get_dataset(ds_id))
        self.assertIsNotNone(store.get_dataset(ds_id_2))
        # Reload store to ensure only one dataset still exists
        store = FileSystemDatastore(STORE_DIR)
        self.assertIsNone(store.get_dataset(ds_id))
        self.assertIsNotNone(store.get_dataset(ds_id_2))
        # Delete the second dataset
        store.delete_dataset(ds_id_2)
        store = FileSystemDatastore(STORE_DIR)
        self.assertIsNone(store.get_dataset(ds_id))
        self.assertIsNone(store.get_dataset(ds_id_2))

    def test_download_dataset(self):
        """Test loading a dataset from Url. Note that this test depends on the
        accessed web service to be running. It will fail otherwise."""
        # Skip test if DOWNLOAD_URL is None
        if DOWNLOAD_URL is None:
            print('Skipping download test')
            return
        store = FileSystemDatastore(STORE_DIR)
        ds = store.download_dataset(url=DOWNLOAD_URL)
        dataset_dir = os.path.join(STORE_DIR, ds.identifier)
        self.assertTrue(os.path.isdir(dataset_dir))
        self.assertTrue(os.path.isfile(os.path.join(dataset_dir, DATA_FILE)))
        self.assertTrue(os.path.isfile(os.path.join(dataset_dir, DESCRIPTOR_FILE)))
        self.assertFalse(os.path.isfile(os.path.join(dataset_dir, METADATA_FILE)))
        self.validate_class_size_dataset(ds)
        # Download file into a given filestore
        fs = FileSystemFilestore(FSSTORE_DIR)
        ds, fh = store.download_dataset(
            url=DOWNLOAD_URL,
            filestore=fs
        )
        self.validate_class_size_dataset(ds)
        self.assertEqual(len(fs.list_files()), 1)
        self.assertIsNotNone(fh)
        self.assertIsNotNone(fs.get_file(fh.identifier))

    def test_get_dataset(self):
        """Test accessing dataset handle and descriptor."""
        # None for non-existing dataset
        store = FileSystemDatastore(STORE_DIR)
        self.assertIsNone(store.get_dataset('0000'))
        ds_id = store.load_dataset(f_handle=FILE).identifier
        self.assertIsNotNone(store.get_dataset(ds_id))
        self.assertIsNone(store.get_dataset('0000'))
        # Reload store to ensure the dataset still exists
        store = FileSystemDatastore(STORE_DIR)
        self.assertIsNotNone(store.get_dataset(ds_id))
        self.assertIsNone(store.get_dataset('0000'))
        self.validate_class_size_dataset(store.get_dataset(ds_id))
        # Load a second dataset
        ds_id_2 =  store.load_dataset(f_handle=FILE).identifier
        self.assertIsNotNone(store.get_dataset(ds_id))
        self.assertIsNotNone(store.get_dataset(ds_id_2))
        # Reload store to ensure the dataset still exists
        store = FileSystemDatastore(STORE_DIR)
        self.assertIsNotNone(store.get_dataset(ds_id))
        self.assertIsNotNone(store.get_dataset(ds_id_2))

    def test_load_dataset(self):
        """Test loading a dataset from file."""
        store = FileSystemDatastore(STORE_DIR)
        ds = store.load_dataset(f_handle=FILE)
        dataset_dir = os.path.join(STORE_DIR, ds.identifier)
        self.assertTrue(os.path.isdir(dataset_dir))
        self.assertTrue(os.path.isfile(os.path.join(dataset_dir, DATA_FILE)))
        self.assertTrue(os.path.isfile(os.path.join(dataset_dir, DESCRIPTOR_FILE)))
        self.assertFalse(os.path.isfile(os.path.join(dataset_dir, METADATA_FILE)))
        self.validate_class_size_dataset(ds)
        with self.assertRaises(ValueError):
            store.load_dataset(f_handle=None)

    def test_query_annotations(self):
        """Test retrieving annotations via the datastore."""
        store = FileSystemDatastore(STORE_DIR)
        ds = store.create_dataset(
            columns=[
                DatasetColumn(identifier=0, name='A'),
                DatasetColumn(identifier=1, name='B')
            ],
            rows=[DatasetRow(identifier=0, values=['a', 'b'])],
            properties=EXAMPLE_PROPERTIES
        )
        properties = store.get_properties(ds.identifier)
        self.assertEqual(len(properties["columns"]), 2)

    def test_validate_dataset(self):
        """Test the validate dataset function."""
        columns = []
        rows = []
        # Empty dataset
        max_col_id, max_row_id = validate_dataset(columns, rows)
        self.assertEqual(max_col_id, -1)
        self.assertEqual(max_row_id, -1)
        max_col_id, max_row_id = validate_dataset(
            columns=columns,
            rows=rows
        )
        self.assertEqual(max_col_id, -1)
        self.assertEqual(max_row_id, -1)
        # Valid set of columns and rows
        columns = [DatasetColumn(0, 'A'), DatasetColumn(10, 'B')]
        rows = [DatasetRow(0, [1, 2]), DatasetRow(4, [None, 2]), DatasetRow(2, [0, 0])]
        max_col_id, max_row_id = validate_dataset(columns, rows)
        self.assertEqual(max_col_id, 10)
        self.assertEqual(max_row_id, 4)
        max_col_id, max_row_id = validate_dataset(
            columns=columns,
            rows=rows
        )
        self.assertEqual(max_col_id, 10)
        self.assertEqual(max_row_id, 4)
        # Column errors
        with self.assertRaises(ValueError):
            validate_dataset(columns + [DatasetColumn()], [])
        with self.assertRaises(ValueError):
            validate_dataset(columns + [DatasetColumn(10, 'C')], [])
        # Row errors
        with self.assertRaises(ValueError):
            validate_dataset(columns, rows + [DatasetRow(1000, [0, 1, 3])])
        with self.assertRaises(ValueError):
            validate_dataset(columns, rows + [DatasetRow(-1, [1, 3])])
        with self.assertRaises(ValueError):
            validate_dataset(columns, rows + [DatasetRow(0, [1, 3])])

    def validate_class_size_dataset(self, ds):
        """Validate some features of the loaded class size dataset."""
        self.assertEqual(len(ds.columns), 3)
        self.assertEqual(ds.column_by_name('A').identifier, 0)
        self.assertEqual(ds.column_by_name('B').identifier, 1)
        self.assertEqual(ds.row_count, 7)
        # Get the first row
        row = ds.fetch_rows(offset=0, limit=1)[0]
        self.assertTrue(isinstance(row.values[0], int))


if __name__ == '__main__':
    unittest.main()
