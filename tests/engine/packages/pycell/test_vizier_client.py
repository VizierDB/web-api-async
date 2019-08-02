"""Test vizier databse client using the default datastore implementation."""

import os
import shutil
import unittest

from vizier.datastore.fs.base import FileSystemDatastore
from vizier.engine.packages.pycell.client.base import VizierDBClient
from vizier.engine.packages.pycell.client.dataset import DatasetClient
from vizier.filestore.fs.base import FileSystemFilestore


SERVER_DIR = './.tmp'
FILESTORE_DIR = './.tmp/fs'
DATASTORE_DIR = './.tmp/ds'
CSV_FILE = './.files/dataset.csv'

DATASET_NAME = 'people'


class TestVizierClient(unittest.TestCase):

    def setUp(self):
        """Create instances of the default datastore and filestore."""
        # Drop directory if it exists
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.makedirs(SERVER_DIR)
        self.datastore=FileSystemDatastore(DATASTORE_DIR)
        self.filestore=FileSystemFilestore(FILESTORE_DIR)

    def tearDown(self):
        """Clean-up by dropping the server directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_create_new_dataset(self):
        """Test creating and updating a new dataset via the client."""
        client = VizierDBClient(datastore=self.datastore, datasets=dict())
        ds = DatasetClient()
        ds.insert_column('Name')
        ds.insert_column('Age')
        ds.insert_row(['Alice', '23'])
        ds.insert_row(['Bob', '25'])
        rows = ds.rows
        ds = client.create_dataset('MyDataset', ds)
        # Ensure the returned dataset contains the input data
        self.assertEqual([c.name for c in ds.columns], ['Name', 'Age'])
        self.assertEqual([str(v) for v in ds.rows[0].values], ['Alice', '23'])
        self.assertEqual([str(v) for v in ds.rows[1].values], ['Bob', '25'])
        # Modify the reference to the original rows to ensure that the rows in
        # the loaded datasets are not affected
        self.assertEqual([str(v) for v in rows[0].values], ['Alice', '23'])
        rows[0].set_value(0, 'Jane')
        self.assertEqual([str(v) for v in rows[0].values], ['Jane', '23'])
        self.assertEqual([str(v) for v in ds.rows[0].values], ['Alice', '23'])
        # Update dataset
        ds.rows[1].set_value('Age', '26')
        client.update_dataset('MyDataset', ds)
        ds = client.get_dataset('MyDataset')
        self.assertEqual([str(v) for v in ds.rows[1].values], ['Bob', '26'])
        # Value error when creating dataset with existing name
        with self.assertRaises(ValueError):
            client.create_dataset('MyDataset', ds)
        # Value error when retrieving unknown dataset
        with self.assertRaises(ValueError):
            client.get_dataset('SomeDataset')
        # Ensure the returned dataset contains the modified data
        client.rename_dataset('MyDataset', 'SomeDataset')
        ds = client.get_dataset('SomeDataset')
        # Ensure that access to unknown datasets is recorded
        with self.assertRaises(ValueError):
            client.get_dataset('ThisIsNotADataset')
        for name in ['somedataset', 'mydataset']:
            self.assertTrue(name in client.read)
            self.assertTrue(name in client.write)
        self.assertTrue('thisisnotadataset' in client.read)
        self.assertFalse('thisisnotadataset' in client.write)

    def test_dataset_annotations(self):
        """Test creating and updating an existing dataset via the client."""
        # Move columns around
        ds = self.datastore.load_dataset(self.filestore.upload_file(CSV_FILE))
        client = VizierDBClient(
            datastore=self.datastore,
            datasets={DATASET_NAME: ds.identifier}
        )
        ds = client.get_dataset(DATASET_NAME)
        annotations = ds.annotations
        annotations.add(key='comment', value='Good', column_id=0, row_id=1)
        annotations.add(key='comment', value='Good', column_id=1, row_id=1)
        annotations.add(key='quality', value='Nice', column_id=0, row_id=1)
        ds = client.update_dataset(name=DATASET_NAME, dataset=ds)
        self.assertEqual(len(ds.annotations.cells), 3)
        ds = client.get_dataset(DATASET_NAME)
        self.assertEqual(len(ds.annotations.cells), 3)
        row = ds.rows[1]
        annotations = row.annotations(0)
        for key in ['comment', 'quality']:
            self.assertTrue(key in list(annotations.keys()))
        annotations = row.annotations(1)
        self.assertTrue('comment' in list(annotations.keys()))
        self.assertFalse('quality' in list(annotations.keys()))
        row.set_value(0, 'New Value', clear_annotations=True)
        self.assertEqual(len(ds.annotations.cells), 1)
        ds = client.update_dataset(name=DATASET_NAME, dataset=ds)
        self.assertEqual(len(ds.annotations.cells), 1)
        ds = client.get_dataset(DATASET_NAME)
        self.assertEqual(len(ds.annotations.cells), 1)

    def test_update_existing_dataset(self):
        """Test creating and updating an existing dataset via the client."""
        # Move columns around
        ds = self.datastore.load_dataset(self.filestore.upload_file(CSV_FILE))
        client = VizierDBClient(
            datastore=self.datastore,
            datasets={DATASET_NAME:ds.identifier}
        )
        ds = client.get_dataset(DATASET_NAME)
        col_1 = [row.get_value(1) for row in ds.rows]
        ds.insert_column('empty', 2)
        ds = client.update_dataset(DATASET_NAME, ds)
        col_2 = [row.get_value(2) for row in ds.rows]
        ds.move_column('empty', 1)
        ds = client.update_dataset(DATASET_NAME, ds)
        for i in range(len(ds.rows)):
            row = ds.rows[i]
            self.assertEqual(row.values[1], col_2[i])
            self.assertEqual(row.values[2], col_1[i])
        # Rename
        ds.columns[1].name = 'allnone'
        ds = client.update_dataset(DATASET_NAME, ds)
        for i in range(len(ds.rows)):
            row = ds.rows[i]
            self.assertEqual(row.get_value('allnone'), col_2[i])
            self.assertEqual(row.values[2], col_1[i])
        # Insert row
        row = ds.insert_row()
        row.set_value('Name', 'Zoe')
        ds = client.create_dataset('upd', ds)
        self.assertEqual(len(ds.rows), 3)
        r2 = ds.rows[2]
        self.assertEqual(r2.identifier, 2)
        self.assertEqual(r2.values, ['Zoe', None, None, None])
        # Annotations
        ds = client.get_dataset(DATASET_NAME)
        col = ds.get_column('Age')
        row = ds.rows[0]
        ds.annotations.add(
            column_id=col.identifier,
            row_id=row.identifier,
            key='user:comment',
            value='My Comment'
        )
        ds = client.update_dataset(DATASET_NAME, ds)
        annotations = ds.rows[0].annotations('Age').find_all('user:comment')
        self.assertEqual(len(annotations), 1)
        anno = annotations[0]
        self.assertEqual(anno.key, 'user:comment')
        self.assertEqual(anno.value, 'My Comment')
        ds.annotations.add(
            column_id=col.identifier,
            row_id=row.identifier,
            key='user:comment',
            value='Another Comment'
        )
        ds = client.update_dataset(DATASET_NAME, ds)
        annotations = ds.rows[0].annotations('Age').find_all('user:comment')
        self.assertEqual(len(annotations), 2)
        self.assertEqual(list(ds.rows[0].annotations('Age').keys()), ['user:comment'])
        values = [a.value for a in annotations]
        for val in ['My Comment', 'Another Comment']:
            self.assertTrue(val in values)
        anno = ds.rows[0].annotations('Age').find_one('user:comment')
        anno.key = 'user:issue'
        anno.value = 'Some Issue'
        ds = client.update_dataset(DATASET_NAME, ds)
        annotations = ds.rows[0].annotations('Age').find_all('user:comment')
        self.assertEqual(len(annotations), 1)
        keys = list(ds.rows[0].annotations('Age').keys())
        for key in ['user:comment', 'user:issue']:
            self.assertTrue(key in keys)
        values = [a.value for a in ds.rows[0].annotations('Age').find_all('user:issue')]
        for val in ['Some Issue']:
            self.assertTrue(val in values)
        ds.annotations.remove(
            column_id=col.identifier,
            row_id=row.identifier,
            key='user:issue',
        )
        ds = client.update_dataset(DATASET_NAME, ds)
        annotations = ds.rows[0].annotations('Age').find_all('user:issue')
        self.assertEqual(len(annotations), 0)
        annotations = ds.rows[0].annotations('Age').find_all('user:comment')
        self.assertEqual(len(annotations), 1)
        # Delete column
        ds = client.get_dataset(DATASET_NAME)
        ds.delete_column('Age')
        client.update_dataset(DATASET_NAME, ds)
        ds = client.get_dataset(DATASET_NAME)
        names = [col.name.upper() for col in ds.columns]
        self.assertTrue('NAME' in names)
        self.assertFalse('AGE' in names)
        self.assertTrue(DATASET_NAME in client.read)
        self.assertTrue(DATASET_NAME in client.write)
        self.assertFalse('upd' in client.read)
        self.assertTrue('upd' in client.write)


if __name__ == '__main__':
    unittest.main()
