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
CSV_FILE = './tests/engine/packages/pycell/.files/dataset.csv'

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
        client = VizierDBClient(
                    datastore=self.datastore, 
                    datasets=dict(),
                    dataobjects=dict(),
                    source="",
                    project_id=7
                )
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
        ds.save()
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
        self.assertTrue('mydataset' in client.write)
        self.assertTrue('somedataset' in client.write)
        self.assertTrue('thisisnotadataset' in client.read)
        self.assertFalse('thisisnotadataset' in client.write)

    def test_update_existing_dataset(self):
        """Test creating and updating an existing dataset via the client."""
        # Move columns around
        ds = self.datastore.load_dataset(self.filestore.upload_file(CSV_FILE))
        client = VizierDBClient(
            datastore=self.datastore,
            datasets={DATASET_NAME:ds},
            dataobjects=dict(),
            source="",
            project_id=7
        )
        ds = client.get_dataset(DATASET_NAME)
        col_1 = [row.get_value(1) for row in ds.rows]
        ds.insert_column('empty', 3)
        ds = client.update_dataset(DATASET_NAME, ds)
        col_2 = [row.get_value(2) for row in ds.rows]
        ds.move_column('empty', 1)
        ds = client.update_dataset(DATASET_NAME, ds)
        for i in range(len(ds.rows)):
            row = ds.rows[i]
            self.assertEqual(row.values[3], col_2[i])
            self.assertEqual(row.values[2], col_1[i])
        # Rename
        ds.columns[1].name = 'allnone'
        ds = client.update_dataset(DATASET_NAME, ds)
        for i in range(len(ds.rows)):
            row = ds.rows[i]
            self.assertEqual(row.get_value('allnone'), None)
            self.assertEqual(row.values[2], col_1[i])
        # Insert row
        row = ds.insert_row()
        row.set_value('Name', 'Zoe')
        ds = client.create_dataset('upd', ds)
        self.assertEqual(len(ds.rows), 3)
        r2 = ds.rows[2]
        self.assertEqual(r2.values, ['Zoe', None, None, None])
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
