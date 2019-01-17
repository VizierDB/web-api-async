"""Test functionality of the default datastore."""

import os
import shutil
import unittest

from vizier.datastore.annotation.base import DatasetAnnotation
from vizier.datastore.annotation.dataset import DatasetMetadata
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

    def test_annotations(self):
        """Test loading a dataset from file."""
        store = FileSystemDatastore(STORE_DIR)
        ds = store.create_dataset(
            columns=[
                DatasetColumn(identifier=0, name='A'),
                DatasetColumn(identifier=1, name='B')
            ],
            rows=[DatasetRow(identifier=0, values=['a', 'b'])],
            annotations=DatasetMetadata(
                cells=[
                    DatasetAnnotation(column_id=0, row_id=0, key='X', value=1),
                    DatasetAnnotation(column_id=0, row_id=0, key='X', value=2),
                    DatasetAnnotation(column_id=1, row_id=0, key='X', value=3),
                    DatasetAnnotation(column_id=1, row_id=1, key='X', value=3),
                    DatasetAnnotation(column_id=0, row_id=0, key='Y', value=1)
                ],
                columns=[
                    DatasetAnnotation(column_id=0, key='A', value='x'),
                    DatasetAnnotation(column_id=2, key='A', value='x')
                    ],
                rows=[
                    DatasetAnnotation(row_id=0, key='E', value=100)
                ]
            )
        )
        ds = store.get_dataset(ds.identifier)
        self.assertEquals(len(ds.annotations.cells), 4)
        self.assertEquals(len(ds.annotations.columns), 1)
        self.assertEquals(len(ds.annotations.rows), 1)
        annos = ds.annotations.for_cell(column_id=0, row_id=0)
        self.assertEquals(len(annos), 3)
        self.assertTrue(1 in [a.value for a in annos])
        self.assertTrue(2 in [a.value for a in annos])
        self.assertFalse(3 in [a.value for a in annos])
        self.assertEquals(len(ds.annotations.find_all(values=annos, key='X')), 2)
        with self.assertRaises(ValueError):
            ds.annotations.find_one(values=annos, key='X')
        self.assertEquals(len(ds.annotations.for_column(column_id=0)), 1)
        self.assertEquals(len(ds.annotations.for_row(row_id=0)), 1)
        annotations = ds.annotations.filter(columns=[1])
        self.assertEquals(len(annotations.cells), 1)
        self.assertEquals(len(annotations.columns), 0)
        self.assertEquals(len(annotations.rows), 1)
        # Reload datastore
        store = FileSystemDatastore(STORE_DIR)
        ds = store.get_dataset(ds.identifier)
        self.assertEquals(len(ds.annotations.cells), 4)
        self.assertEquals(len(ds.annotations.columns), 1)
        self.assertEquals(len(ds.annotations.rows), 1)

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
        self.assertEquals(len(ds.columns), 2)
        for id in [0, 1]:
            self.assertTrue(id in column_ids)
        column_names = [col.name for col in ds.columns]
        for name in ['A', 'B']:
            self.assertTrue(name in column_names)
        rows = ds.fetch_rows()
        self.assertEquals(len(rows), 1)
        self.assertEquals(rows[0].values, ['a', 'b'])
        self.assertEquals(len(ds.annotations.cells), 0)
        self.assertEquals(len(ds.annotations.columns), 0)
        self.assertEquals(len(ds.annotations.rows), 0)
        # Reload the datastore
        store = FileSystemDatastore(STORE_DIR)
        ds = store.get_dataset(ds.identifier)
        column_ids = [col.identifier for col in ds.columns]
        self.assertEquals(len(ds.columns), 2)
        for id in [0, 1]:
            self.assertTrue(id in column_ids)
        column_names = [col.name for col in ds.columns]
        for name in ['A', 'B']:
            self.assertTrue(name in column_names)
        rows = ds.fetch_rows()
        self.assertEquals(len(rows), 1)
        self.assertEquals(rows[0].values, ['a', 'b'])
        self.assertEquals(len(ds.annotations.cells), 0)
        self.assertEquals(len(ds.annotations.columns), 0)
        self.assertEquals(len(ds.annotations.rows), 0)

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

    def test_deduplicate_annotations(self):
        """Test removing duplicated annotations."""
        store = FileSystemDatastore(STORE_DIR)
        ds = store.create_dataset(
            columns=[
                DatasetColumn(identifier=0, name='A'),
                DatasetColumn(identifier=1, name='B')
            ],
            rows=[DatasetRow(identifier=0, values=['a', 'b'])],
            annotations=DatasetMetadata(
                cells=[
                    DatasetAnnotation(column_id=0, row_id=0, key='X', value=1),
                    DatasetAnnotation(column_id=0, row_id=0, key='X', value=2),
                    DatasetAnnotation(column_id=1, row_id=0, key='X', value=3),
                    DatasetAnnotation(column_id=1, row_id=1, key='X', value=3),
                    DatasetAnnotation(column_id=0, row_id=0, key='Y', value=1),
                    DatasetAnnotation(column_id=0, row_id=0, key='X', value=1),
                    DatasetAnnotation(column_id=0, row_id=0, key='X', value=2),
                    DatasetAnnotation(column_id=1, row_id=0, key='X', value=3),
                    DatasetAnnotation(column_id=1, row_id=1, key='X', value=3),
                ],
                columns=[
                    DatasetAnnotation(column_id=0, key='A', value='x'),
                    DatasetAnnotation(column_id=1, key='A', value='x'),
                    DatasetAnnotation(column_id=0, key='A', value='x'),
                    DatasetAnnotation(column_id=1, key='A', value='x'),
                    DatasetAnnotation(column_id=0, key='A', value='x'),
                    DatasetAnnotation(column_id=1, key='A', value='x'),
                    DatasetAnnotation(column_id=0, key='A', value='x'),
                    DatasetAnnotation(column_id=1, key='A', value='x')
                    ],
                rows=[
                    DatasetAnnotation(row_id=0, key='E', value=100),
                    DatasetAnnotation(row_id=0, key='E', value=100)
                ]
            )
        )
        ds = store.get_dataset(ds.identifier)
        self.assertEquals(len(ds.annotations.cells), 4)
        self.assertEquals(len(ds.annotations.columns), 2)
        self.assertEquals(len(ds.annotations.rows), 1)
        annos = ds.annotations.for_cell(column_id=0, row_id=0)
        self.assertEquals(len(annos), 3)
        self.assertTrue(1 in [a.value for a in annos])
        self.assertTrue(2 in [a.value for a in annos])
        self.assertFalse(3 in [a.value for a in annos])
        self.assertEquals(len(ds.annotations.find_all(values=annos, key='X')), 2)
        with self.assertRaises(ValueError):
            ds.annotations.find_one(values=annos, key='X')
        self.assertEquals(len(ds.annotations.for_column(column_id=0)), 1)
        self.assertEquals(len(ds.annotations.for_row(row_id=0)), 1)
        annotations = ds.annotations.filter(columns=[1])
        self.assertEquals(len(annotations.cells), 1)
        self.assertEquals(len(annotations.columns), 1)
        self.assertEquals(len(annotations.rows), 1)

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
        fs = FileSystemFilestore(FSSTORE_DIR)
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
            annotations=DatasetMetadata(
                cells=[
                    DatasetAnnotation(column_id=0, row_id=0, key='X', value=1),
                    DatasetAnnotation(column_id=0, row_id=0, key='X', value=2),
                    DatasetAnnotation(column_id=1, row_id=0, key='X', value=3),
                    DatasetAnnotation(column_id=0, row_id=0, key='Y', value=1)
                ],
                columns=[
                    DatasetAnnotation(column_id=0, key='A', value='x'),
                    DatasetAnnotation(column_id=1, key='A', value='x')
                    ],
                rows=[
                    DatasetAnnotation(row_id=0, key='E', value=100)
                ]
            )
        )
        annos = store.get_annotations(ds.identifier, column_id=1)
        self.assertEquals(len(annos.columns), 1)
        self.assertEquals(len(annos.rows), 0)
        self.assertEquals(len(annos.cells), 0)
        annos = store.get_annotations(ds.identifier, column_id=0)
        self.assertEquals(len(annos.columns), 1)
        self.assertEquals(len(annos.rows), 0)
        self.assertEquals(len(annos.cells), 0)
        annos = store.get_annotations(ds.identifier, row_id=0)
        self.assertEquals(len(annos.columns), 0)
        self.assertEquals(len(annos.rows), 1)
        self.assertEquals(len(annos.cells), 0)
        annos = store.get_annotations(ds.identifier, column_id=1, row_id=0)
        self.assertEquals(len(annos.columns), 0)
        self.assertEquals(len(annos.rows), 0)
        self.assertEquals(len(annos.cells), 1)
        annos = store.get_annotations(ds.identifier, column_id=0, row_id=0)
        self.assertEquals(len(annos.columns), 0)
        self.assertEquals(len(annos.rows), 0)
        self.assertEquals(len(annos.cells), 3)

    def test_update_annotations(self):
        """Test updating annotations via the datastore."""
        store = FileSystemDatastore(STORE_DIR)
        ds = store.create_dataset(
            columns=[
                DatasetColumn(identifier=0, name='A'),
                DatasetColumn(identifier=1, name='B')
            ],
            rows=[DatasetRow(identifier=0, values=['a', 'b'])],
            annotations=DatasetMetadata(
                cells=[
                    DatasetAnnotation(column_id=0, row_id=0, key='X', value=1),
                    DatasetAnnotation(column_id=0, row_id=0, key='X', value=2),
                    DatasetAnnotation(column_id=1, row_id=0, key='X', value=3),
                    DatasetAnnotation(column_id=0, row_id=0, key='Y', value=1)
                ],
                columns=[
                    DatasetAnnotation(column_id=0, key='A', value='x'),
                    DatasetAnnotation(column_id=1, key='A', value='x')
                    ],
                rows=[
                    DatasetAnnotation(row_id=0, key='E', value=100)
                ]
            )
        )
        # INSERT row annotatins
        store.update_annotation(
            ds.identifier,
            key='D',
            row_id=0,
            new_value=200
        )
        annos = store.get_annotations(ds.identifier, row_id=0)
        self.assertEquals(len(annos.rows), 2)
        for key in ['D', 'E']:
            self.assertTrue(key in [a.key for a in annos.rows])
        for val in [100, 200]:
            self.assertTrue(val in [a.value for a in annos.rows])
        # UPDATE column annotation
        store.update_annotation(
            ds.identifier,
            key='A',
            column_id=1,
            old_value='x',
            new_value='y'
        )
        annos = store.get_annotations(ds.identifier, column_id=1)
        self.assertEquals(annos.columns[0].key, 'A')
        self.assertEquals(annos.columns[0].value, 'y')
        # DELETE cell annotation
        store.update_annotation(
            ds.identifier,
            key='X',
            column_id=0,
            row_id=0,
            old_value=2,
        )
        annos = store.get_annotations(ds.identifier, column_id=0, row_id=0)
        self.assertEquals(len(annos.cells), 2)
        for a in annos.cells:
            self.assertNotEqual(a.value, 2)
        result = store.update_annotation(
            ds.identifier,
            key='X',
            column_id=1,
            row_id=0,
            old_value=3,
        )
        self.assertTrue(result)
        annos = store.get_annotations(ds.identifier, column_id=1, row_id=0)
        self.assertEquals(len(annos.cells), 0)

    def test_validate_dataset(self):
        """Test the validate dataset function."""
        columns = []
        rows = []
        # Empty dataset
        max_col_id, max_row_id = validate_dataset(columns, rows)
        self.assertEquals(max_col_id, -1)
        self.assertEquals(max_row_id, -1)
        max_col_id, max_row_id = validate_dataset(
            columns=columns,
            rows=rows
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
            columns=columns,
            rows=rows
        )
        self.assertEquals(max_col_id, 10)
        self.assertEquals(max_row_id, 4)
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
        self.assertEquals(len(ds.columns), 4)
        self.assertEquals(ds.max_column_id(), 3)
        self.assertEquals(ds.column_by_name('average_class_size').identifier, 0)
        self.assertEquals(ds.column_by_name('grade_or_service_category_').identifier, 1)
        self.assertEquals(ds.column_by_name('program').identifier, 2)
        self.assertEquals(ds.column_by_name('core_course_high_schools_only_').identifier, 3)
        self.assertEquals(ds.row_count, 54)
        self.assertEquals(ds.max_row_id(), 53)
        # Get the first row
        row = ds.fetch_rows(offset=0, limit=1)[0]
        self.assertEquals(row.identifier, 0)
        self.assertTrue(isinstance(row.values[0], float))


if __name__ == '__main__':
    unittest.main()
