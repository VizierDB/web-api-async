import csv
import os
import shutil
import unittest

import vizier.mimir as mimir

from vizier.datastore.dataset import DatasetColumn
from vizier.datastore.mimir.store import MimirDatastore
from vizier.filestore.fs.base import FileSystemFilestore


CSV_FILE = './tests/datastore/.files/dataset.csv'

SERVER_DIR = '.tmp'
DATASTORE_DIR = os.path.join(SERVER_DIR, 'ds')
FILESERVER_DIR = os.path.join(SERVER_DIR, 'fs')


class TestMimirDatastore(unittest.TestCase):

    def setup_fileserver(self):
        """Create a fresh file server."""
        if os.path.isdir(FILESERVER_DIR):
            shutil.rmtree(FILESERVER_DIR)
        os.mkdir(FILESERVER_DIR)
        self.fileserver = FileSystemFilestore(FILESERVER_DIR)

    def set_up(self):
        """Create empty data store directory."""
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.mkdir(SERVER_DIR)
        self.db = MimirDatastore(DATASTORE_DIR)

    def tear_down(self):
        """Delete data store directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_mimir_datastore(self):
        """Run test for Mimir datastore."""
        self.set_up()
        self.dataset_load()
        self.tear_down()
        self.set_up()
        self.datastore_init()
        self.tear_down()
        self.set_up()
        self.dataset_read()
        self.tear_down()
        self.set_up()
        self.dataset_column_index()
        self.tear_down()
        self.tear_down()

    def datastore_init(self):
        """Test initalizing a datastore with existing datasets."""
        self.setup_fileserver()
        ds = self.db.load_dataset(self.fileserver.upload_file(CSV_FILE))
        self.db = MimirDatastore(DATASTORE_DIR)

    def dataset_column_index(self):
        """Test the column by id index of the dataset handle."""
        self.setup_fileserver()
        ds = self.db.load_dataset(self.fileserver.upload_file(CSV_FILE))
        # Ensure that the project data has three columns and two rows
        self.assertEqual(ds.column_by_id(0).name.upper(), 'NAME')
        self.assertEqual(ds.column_by_id(1).name.upper(), 'AGE')
        self.assertEqual(ds.column_by_id(2).name.upper(), 'SALARY')
        with self.assertRaises(ValueError):
            ds.column_by_id(5)
        ds.columns.append(DatasetColumn(identifier=5, name='NEWNAME'))
        self.assertEqual(ds.column_by_id(5).name.upper(), 'NEWNAME')
        with self.assertRaises(ValueError):
            ds.column_by_id(4)

    def dataset_load(self):
        """Test create and delete dataset."""
        self.setup_fileserver()
        ds = self.db.load_dataset(self.fileserver.upload_file(CSV_FILE))
        # Ensure that the project data has three columns and two rows
        self.assertEqual(len(ds.columns), 3)
        self.assertEqual(len(ds.fetch_rows()), 2)
        self.assertEqual(ds.row_count, 2)

    def dataset_read(self):
        """Test reading a dataset."""
        self.setup_fileserver()
        dh = self.db.load_dataset(self.fileserver.upload_file(CSV_FILE))
        ds = self.db.get_dataset(dh.identifier)
        ds_rows = ds.fetch_rows()
        self.assertEqual(dh.identifier, ds.identifier)
        self.assertEqual(len(dh.columns), len(ds.columns))
        self.assertEqual(len(dh.fetch_rows()), len(ds_rows))
        self.assertEqual(len(dh.fetch_rows()), len(ds_rows))
        self.assertEqual(dh.row_count, len(ds_rows))
        # Name,Age,Salary
        # Alice,23,35K
        # Bob,32,30K
        self.assertEqual(ds.column_index('Name'), 0)
        self.assertEqual(ds.column_index('Age'), 1)
        self.assertEqual(ds.column_index('Salary'), 2)
        row = ds_rows[0]
        self.assertEqual(row.values[0], 'Alice')
        self.assertEqual(int(row.values[1]), 23)
        self.assertEqual(row.values[2], '35K')
        row = ds_rows[1]
        self.assertEqual(row.values[0], 'Bob')
        self.assertEqual(int(row.values[1]), 32)
        self.assertEqual(row.values[2], '30K')


if __name__ == '__main__':
    unittest.main()
