import csv
import os
import shutil
import unittest

import vistrails.packages.mimir.init as mimir

from vizier.datastore.base import DatasetColumn
from vizier.datastore.client import DatasetClient
from vizier.datastore.federated import FederatedDataStore
from vizier.datastore.fs import FileSystemDataStore
from vizier.datastore.fs import DATA_FILE, METADATA_FILE
from vizier.datastore.mem import InMemDataStore, InMemDatasetHandle
from vizier.datastore.mem import VolatileDataStore
from vizier.datastore.mimir import MimirDataStore
from vizier.filestore.base import DefaultFileServer


CSV_FILE = './data/dataset.csv'
TSV_FILE = './data/dataset.tsv.gz'

DATASTORE_DIRECTORY = './env/ds'
FILESERVER_DIR = './env/fs'

FS_DATASTORE = 'FS'
MEM_DATASTORE = 'MEM'
MIMIR_DATASTORE = 'MIMIR'

class TestDataStore(unittest.TestCase):

    def setup_fileserver(self):
        """Create a fresh file server."""
        if os.path.isdir(FILESERVER_DIR):
            shutil.rmtree(FILESERVER_DIR)
        os.mkdir(FILESERVER_DIR)
        self.fileserver = DefaultFileServer(FILESERVER_DIR)

    def set_up(self, store_type):
        """Create empty data store directory."""
        if store_type == MEM_DATASTORE:
            self.db = InMemDataStore()
        else:
            # Remove directory if it exists
            if os.path.isdir(DATASTORE_DIRECTORY):
                shutil.rmtree(DATASTORE_DIRECTORY)
            os.mkdir(DATASTORE_DIRECTORY)
            if store_type == FS_DATASTORE:
                self.db = FileSystemDataStore(DATASTORE_DIRECTORY)
            elif store_type == MIMIR_DATASTORE:
                self.db = MimirDataStore(DATASTORE_DIRECTORY)

    def tear_down(self, store_type):
        """Delete data store directory.
        """
        for d in [DATASTORE_DIRECTORY, FILESERVER_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)

    def test_federated_datastore(self):
        """Test functionality of the federated data store."""
        self.setup_fileserver()
        store1 = InMemDataStore()
        store2 = InMemDataStore()
        fh = self.fileserver.upload_file(CSV_FILE)
        ds1 = store1.load_dataset(fh)
        ds2 = store2.load_dataset(fh)
        fed_store = FederatedDataStore([store1, store2])
        self.assertIsNotNone(fed_store.get_dataset(ds1.identifier))
        self.assertIsNotNone(fed_store.get_dataset(ds2.identifier))
        self.assertIsNone(fed_store.get_dataset('UNDEFINED'))
        with self.assertRaises(NotImplementedError):
            fed_store.load_dataset(fh)
        self.assertIsNotNone(fed_store.update_annotation(ds1.identifier, column_id=0, key='name', value='My Name'))
        self.assertIsNotNone(fed_store.update_annotation(ds2.identifier, column_id=0, key='name', value='My Name'))
        self.assertIsNone(fed_store.update_annotation('UNDEFINED', column_id=0, key='name', value='My Name'))

    def test_fs_datastore(self):
        """Run test for file system datastore."""
        self.run_tests(FS_DATASTORE)

    def test_mem_datastore(self):
        """Run test for in-memory datastore."""
        self.run_tests(MEM_DATASTORE)

    def test_mimir_datastore(self):
        """Run test for Mimir datastore."""
        mimir.initialize()
        self.run_tests(MIMIR_DATASTORE)
        self.set_up(MIMIR_DATASTORE)
        self.load_tsv()
        self.tear_down(MIMIR_DATASTORE)
        mimir.finalize()

    def test_volatile_datastore(self):
        """Test volatile data store on top of a file system data store."""
        self.set_up(FS_DATASTORE)
        self.setup_fileserver()
        ds = self.db.load_dataset(self.fileserver.upload_file(CSV_FILE))
        ds_rows = ds.fetch_rows()
        self.assertEquals(len(ds_rows), ds.row_count)
        v_store = VolatileDataStore(self.db)
        # Make sure the existing dataset is accessible via the volatile store
        v_ds = DatasetClient(dataset=v_store.get_dataset(ds.identifier))
        self.assertIsNotNone(v_ds)
        self.assertEquals(v_ds.get_cell('Salary', 1), '30K')
        # Create an updated dataset. The original should be the same in both
        # stores
        v_ds.rows[1].set_value('Salary', '40K')
        v_ds = v_store.create_dataset(columns=v_ds.columns, rows=v_ds.rows)
        self.assertEquals(DatasetClient(dataset=self.db.get_dataset(ds.identifier)).get_cell('Salary', 1), '30K')
        self.assertEquals(DatasetClient(dataset=v_store.get_dataset(ds.identifier)).get_cell('Salary', 1), '30K')
        self.assertEquals(DatasetClient(dataset=v_store.get_dataset(v_ds.identifier)).get_cell('Salary', 1), '40K')
        self.assertIsNone(self.db.get_dataset(v_ds.identifier))
        # Delete both datasets. The volatile store is empty. The original should
        # be unchanged.
        self.assertTrue(v_store.delete_dataset(ds.identifier))
        self.assertTrue(v_store.delete_dataset(v_ds.identifier))
        self.assertFalse(v_store.delete_dataset(ds.identifier))
        self.assertFalse(v_store.delete_dataset(v_ds.identifier))
        self.assertIsNone(v_store.get_dataset(ds.identifier))
        self.assertIsNone(v_store.get_dataset(v_ds.identifier))
        self.assertEquals(DatasetClient(dataset=self.db.get_dataset(ds.identifier)).get_cell('Salary', 1), '30K')
        self.tear_down(FS_DATASTORE)

    def run_tests(self, store_type):
        """Run sequence of test for given data store type."""
        self.set_up(store_type)
        self.dataset_life_cycle()
        self.tear_down(store_type)
        self.set_up(store_type)
        self.datastore_init(store_type)
        self.tear_down(store_type)
        self.set_up(store_type)
        self.dataset_read()
        self.tear_down(store_type)
        self.set_up(store_type)
        self.dataset_column_index()
        self.tear_down(store_type)

    def datastore_init(self, store_type):
        """Test initalizing a datastore with existing datasets."""
        self.setup_fileserver()
        ds = self.db.load_dataset(self.fileserver.upload_file(CSV_FILE))
        if store_type == MEM_DATASTORE:
            self.db = InMemDataStore()
        elif store_type == FS_DATASTORE:
            self.db = FileSystemDataStore(DATASTORE_DIRECTORY)
        elif store_type == MIMIR_DATASTORE:
            self.db = MimirDataStore(DATASTORE_DIRECTORY)

    def dataset_column_index(self):
        """Test the column by id index of the dataset handle."""
        self.setup_fileserver()
        ds = self.db.load_dataset(self.fileserver.upload_file(CSV_FILE))
        # Ensure that the project data has three columns and two rows
        self.assertEquals(ds.column_by_id(0).name.upper(), 'NAME')
        self.assertEquals(ds.column_by_id(1).name.upper(), 'AGE')
        self.assertEquals(ds.column_by_id(2).name.upper(), 'SALARY')
        with self.assertRaises(ValueError):
            ds.column_by_id(5)
        ds.columns.append(DatasetColumn(identifier=5, name='NEWNAME'))
        self.assertEquals(ds.column_by_id(5).name.upper(), 'NEWNAME')
        with self.assertRaises(ValueError):
            ds.column_by_id(4)

    def dataset_life_cycle(self):
        """Test create and delete dataset."""
        self.setup_fileserver()
        ds = self.db.load_dataset(self.fileserver.upload_file(CSV_FILE))
        # Ensure that the project data has three columns and two rows
        self.assertEquals(len(ds.columns), 3)
        self.assertEquals(len(ds.fetch_rows()), 2)
        self.assertEquals(ds.row_count, 2)
        # Delete dataset and ensure that the dataset directory no longer exists
        self.assertTrue(self.db.delete_dataset(ds.identifier))
        self.assertFalse(self.db.delete_dataset(ds.identifier))

    def dataset_read(self):
        """Test reading a dataset."""
        self.setup_fileserver()
        dh = self.db.load_dataset(self.fileserver.upload_file(CSV_FILE))
        ds = self.db.get_dataset(dh.identifier)
        ds_rows = ds.fetch_rows()
        self.assertEquals(dh.identifier, ds.identifier)
        self.assertEquals(len(dh.columns), len(ds.columns))
        self.assertEquals(len(dh.fetch_rows()), len(ds_rows))
        self.assertEquals(len(dh.fetch_rows()), len(ds_rows))
        self.assertEquals(dh.row_count, len(ds_rows))
        # Name,Age,Salary
        # Alice,23,35K
        # Bob,32,30K
        self.assertEquals(ds.column_index('Name'), 0)
        self.assertEquals(ds.column_index('Age'), 1)
        self.assertEquals(ds.column_index('Salary'), 2)
        row = ds_rows[0]
        self.assertEquals(row.values[0], 'Alice')
        self.assertEquals(int(row.values[1]), 23)
        self.assertEquals(row.values[2], '35K')
        row = ds_rows[1]
        self.assertEquals(row.values[0], 'Bob')
        self.assertEquals(int(row.values[1]), 32)
        self.assertEquals(row.values[2], '30K')

    def load_tsv(self):
        """Test writing a dataset with duplicate name twice."""
        self.setup_fileserver()
        fh = self.fileserver.upload_file(TSV_FILE)
        ds = self.db.load_dataset(fh)
        self.assertEquals(len(ds.columns), 3)
        self.assertEquals(ds.row_count, 2)


if __name__ == '__main__':
    unittest.main()
