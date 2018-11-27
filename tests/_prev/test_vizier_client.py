import os
import shutil
import unittest

import vistrails.packages.mimir.init as mimir

from vizier.datastore.client import DatasetClient
from vizier.datastore.fs import FileSystemDataStore
from vizier.datastore.mem import InMemDataStore
from vizier.datastore.mimir import MimirDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.vizual.base import DefaultVizualEngine
from vizier.workflow.context import VizierDBClient

DATASTORE_DIR = './env/ds'
SERVER_DIR = './data/fs'

CSV_FILE = './data/dataset.csv'


class TestVizierClient(unittest.TestCase):

    def setUp(self):
        """Delete metadata file if it exists."""
        # Drop directorie
        self.tearDown()

    def tearDown(self):
        """Clean-up by dropping file server directory.
        """
        if os.path.isdir(DATASTORE_DIR):
            shutil.rmtree(DATASTORE_DIR)
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_fs_client(self):
        """Run tests for default engine and file server data store."""
        self.fs = DefaultFileServer(SERVER_DIR)
        self.ds = FileSystemDataStore(DATASTORE_DIR)
        self.run_client_tests(
            VizierDBClient(self.ds, dict(), DefaultVizualEngine(self.ds, self.fs))
        )

    def test_mem_client(self):
        """Run tests for default engine and in-memory data store."""
        self.fs = DefaultFileServer(SERVER_DIR)
        self.ds = InMemDataStore()
        self.run_client_tests(
            VizierDBClient(self.ds, dict(), DefaultVizualEngine(self.ds, self.fs))
        )

    def test_mimir_client(self):
        """Run tests for default engine and Mimir data store."""
        mimir.initialize()
        self.fs = DefaultFileServer(SERVER_DIR)
        self.ds = MimirDataStore(DATASTORE_DIR)
        self.run_client_tests(
            VizierDBClient(self.ds, dict(), DefaultVizualEngine(self.ds, self.fs))
        )
        mimir.finalize()

    def run_client_tests(self, client):
        """Test creating and updating a dataset via the client."""
        ds = DatasetClient()
        ds.insert_column('Name')
        ds.insert_column('Age')
        ds.insert_row(['Alice', '23'])
        ds.insert_row(['Bob', '25'])
        client.create_dataset('MyDataset', ds)
        # Ensure the returned dataset contains the input data
        ds = client.get_dataset('MyDataset')
        self.assertEquals([c.name for c in ds.columns], ['Name', 'Age'])
        self.assertEquals([str(v) for v in ds.rows[0].values], ['Alice', '23'])
        self.assertEquals([str(v) for v in ds.rows[1].values], ['Bob', '25'])
        # Update dataset
        ds.rows[1].set_value('Age', '26')
        client.update_dataset('MyDataset', ds)
        ds = client.get_dataset('MyDataset')
        self.assertEquals([str(v) for v in ds.rows[1].values], ['Bob', '26'])
        # Value error when creating dataset with existing name
        with self.assertRaises(ValueError):
            client.create_dataset('MyDataset', ds)
        # Value error when retrieving unknown dataset
        with self.assertRaises(ValueError):
            client.get_dataset('SomeDataset')
        # Ensure the returned dataset contains the modified data
        client.rename_dataset('MyDataset', 'SomeDataset')
        ds = client.get_dataset('SomeDataset')
        client.update_dataset('SomeDataset', ds)
        # Move columns around
        ds = self.ds.load_dataset(self.fs.upload_file(CSV_FILE))
        ds = client.create_dataset('people', DatasetClient(ds))
        col_1 = [row.get_value(1) for row in ds.rows]
        ds.insert_column('empty', 2)
        ds = client.update_dataset('people', ds)
        col_2 = [row.get_value(2) for row in ds.rows]
        ds.move_column('empty', 1)
        ds = client.update_dataset('people', ds)
        for i in range(len(ds.rows)):
            row = ds.rows[i]
            self.assertEquals(row.values[1], col_2[i])
            self.assertEquals(row.values[2], col_1[i])
        # Rename
        ds.columns[1].name = 'allnone'
        ds = client.update_dataset('people', ds)
        for i in range(len(ds.rows)):
            row = ds.rows[i]
            self.assertEquals(row.get_value('allnone'), col_2[i])
            self.assertEquals(row.values[2], col_1[i])
        # Insert row
        row = ds.insert_row()
        row.set_value('Name', 'Zoe')
        ds = client.create_dataset('upd', ds)
        self.assertEquals(len(ds.rows), 3)
        r2 = ds.rows[2]
        self.assertEquals(r2.identifier, 2)
        self.assertEquals(r2.values, ['Zoe', None, None, None])
        # Annotations
        ds = client.get_dataset('people')
        annotations = ds.rows[0].annotations('Age')
        annotations.add('user:comment', 'My Comment')
        ds = client.update_dataset('people', ds)
        annotations = ds.rows[0].annotations('Age').find_all('user:comment')
        self.assertEquals(len(annotations), 1)
        anno = annotations[0]
        self.assertEquals(anno.key, 'user:comment')
        self.assertEquals(anno.value, 'My Comment')
        ds.rows[0].annotations('Age').add('user:comment', 'Another Comment')
        ds = client.update_dataset('people', ds)
        annotations = ds.rows[0].annotations('Age').find_all('user:comment')
        self.assertEquals(len(annotations), 2)
        self.assertEquals(ds.rows[0].annotations('Age').keys(), ['user:comment'])
        values = [a.value for a in annotations]
        for val in ['My Comment', 'Another Comment']:
            self.assertTrue(val in values)
        ds.rows[0].annotations('Age').update(identifier=anno.identifier, key='user:issue', value='Some Issue')
        ds = client.update_dataset('people', ds)
        annotations = ds.rows[0].annotations('Age').find_all('user:comment')
        self.assertEquals(len(annotations), 1)
        keys = ds.rows[0].annotations('Age').keys()
        for key in ['user:comment', 'user:issue']:
            self.assertTrue(key in keys)
        values = [a.value for a in ds.rows[0].annotations('Age').find_all('user:comment')]
        for val in ['Another Comment']:
            self.assertTrue(val in values)
        values = [a.value for a in ds.rows[0].annotations('Age').find_all('user:issue')]
        for val in ['Some Issue']:
            self.assertTrue(val in values)
        ds.rows[0].annotations('Age').update(identifier=anno.identifier)
        ds = client.update_dataset('people', ds)
        annotations = ds.rows[0].annotations('Age').find_all('user:issue')
        self.assertEquals(len(annotations), 0)
        annotations = ds.rows[0].annotations('Age').find_all('user:comment')
        self.assertEquals(len(annotations), 1)
        # Delete column
        ds = client.get_dataset('people')
        ds.delete_column('Age')
        client.update_dataset('people', ds)
        ds = client.get_dataset('people')
        names = [col.name.upper() for col in ds.columns]
        self.assertTrue('NAME' in names)
        self.assertFalse('AGE' in names)


if __name__ == '__main__':
    unittest.main()
