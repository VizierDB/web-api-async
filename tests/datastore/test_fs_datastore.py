"""Test functionality of the default datastore."""

import os
import shutil
import unittest

from vizier.datastore.dataset import DatasetColumn, DatasetRow
from vizier.datastore.fs.base import FileSystemDatastore
from vizier.datastore.fs.base import DATA_FILE, DESCRIPTOR_FILE, METADATA_FILE
from vizier.datastore.fs.base import validate_dataset
from vizier.filestore.fs.base import DefaultFilestore
from vizier.filestore.base import FileHandle, FORMAT_TSV

BASE_DIR = './.tmp'
STORE_DIR = './.tmp/ds'
FSSTORE_DIR = './.tmp/fs'

FILE = FileHandle(
    identifier='0000',
    filepath='./.files/w49k-mmkh.tsv',
    file_name='w49k-mmkh.tsv'
)

# Note that some tests access an external resource to test download capabilities.
# The test will fail if the specified resource is not available. Set the URI
# to an available resource or to None to skip the download tests
URI = 'http://cds-swg1.cims.nyu.edu:8080/opendb-api/api/v1/datasets/w49k-mmkh/rows/download'

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
        # Skip test if URI is None
        if URI is None:
            print 'Skipping download test'
            return
        store = FileSystemDatastore(STORE_DIR)
        ds = store.download_dataset(uri=URI)
        dataset_dir = os.path.join(STORE_DIR, ds.identifier)
        self.assertTrue(os.path.isdir(dataset_dir))
        self.assertTrue(os.path.isfile(os.path.join(dataset_dir, DATA_FILE)))
        self.assertTrue(os.path.isfile(os.path.join(dataset_dir, DESCRIPTOR_FILE)))
        self.assertFalse(os.path.isfile(os.path.join(dataset_dir, METADATA_FILE)))
        self.validate_class_size_dataset(ds)
        # Download file into a given filestore
        fs = DefaultFilestore(FSSTORE_DIR)
        ds, fh = store.download_dataset(
            uri=URI,
            filestore=fs
        )
        self.validate_class_size_dataset(ds)
        self.assertEquals(len(fs.list_files()), 1)
        self.assertIsNotNone(fh)
        self.assertIsNotNone(fs.get_file(fh.identifier))

    def test_get_dataset(self):
        """Test accessing dataset handle and descriptor."""
        # None for non-existing dataset
        store = FileSystemDatastore(STORE_DIR)
        self.assertIsNone(store.get_dataset('0000'))
        self.assertIsNone(store.get_descriptor('0000'))
        ds_id = store.load_dataset(f_handle=FILE).identifier
        self.assertIsNotNone(store.get_dataset(ds_id))
        self.assertIsNotNone(store.get_descriptor(ds_id))
        self.assertIsNone(store.get_dataset('0000'))
        self.assertIsNone(store.get_descriptor('0000'))
        # Reload store to ensure the dataset still exists
        store = FileSystemDatastore(STORE_DIR)
        self.assertIsNotNone(store.get_dataset(ds_id))
        self.assertIsNotNone(store.get_descriptor(ds_id))
        self.assertIsNone(store.get_dataset('0000'))
        self.assertIsNone(store.get_descriptor('0000'))
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

    def validate_class_size_dataset(self, ds):
        """Validate some features of the loaded class size dataset."""
        self.assertEquals(len(ds.columns), 4)
        self.assertEquals(ds.column_by_name('average_class_size').identifier, 0)
        self.assertEquals(ds.column_by_name('grade_or_service_category_').identifier, 1)
        self.assertEquals(ds.column_by_name('program').identifier, 2)
        self.assertEquals(ds.column_by_name('core_course_high_schools_only_').identifier, 3)
        self.assertEquals(ds.row_count, 54)
        # Get the first row
        row = ds.fetch_rows(offset=0, limit=1)[0]
        self.assertEquals(row.identifier, 0)
        self.assertTrue(isinstance(row.values[0], float))

    def test_validate_dataset(self):
        """Test the validate dataset function."""
        columns = []
        rows = []
        # Empty dataset
        max_col_id, max_row_id = validate_dataset(columns, rows)
        self.assertEquals(max_col_id, -1)
        self.assertEquals(max_row_id, -1)
        max_col_id, max_row_id = validate_dataset(
            columns,
            rows,
            column_counter=10,
            row_counter=1000
        )
        self.assertEquals(max_col_id, -1)
        self.assertEquals(max_row_id, -1)
        # Valid set of columns and rows
        columns = [DatasetColumn(0, 'A'), DatasetColumn(10, 'B')]
        rows = [DatasetRow(0, [1, 2]), DatasetRow(4, [None, 2]), DatasetRow(2, [0, 0])]
        max_col_id, max_row_id = validate_dataset(columns, rows)
        self.assertEquals(max_col_id, 10)
        self.assertEquals(max_row_id, 4)
        max_col_id, max_row_id = validate_dataset(
            columns,
            rows,
            column_counter=11,
            row_counter=5
        )
        self.assertEquals(max_col_id, 10)
        self.assertEquals(max_row_id, 4)
        # Column errors
        with self.assertRaises(ValueError):
            validate_dataset(columns + [DatasetColumn()], [])
        with self.assertRaises(ValueError):
            validate_dataset(columns + [DatasetColumn(10, 'C')], [])
        with self.assertRaises(ValueError):
            validate_dataset(columns, [], column_counter=10)
        with self.assertRaises(ValueError):
            validate_dataset(columns, [], column_counter=5)
        # Row errors
        with self.assertRaises(ValueError):
            validate_dataset(columns, rows + [DatasetRow(1000, [0, 1, 3])])
        with self.assertRaises(ValueError):
            validate_dataset(columns, rows + [DatasetRow(-1, [1, 3])])
        with self.assertRaises(ValueError):
            validate_dataset(columns, rows + [DatasetRow(0, [1, 3])])
        with self.assertRaises(ValueError):
            validate_dataset(columns, rows + [DatasetRow(3, [1, 3])], row_counter=3)


if __name__ == '__main__':
    unittest.main()
