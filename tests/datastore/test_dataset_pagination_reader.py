import os
import shutil
import unittest

from vizier.datastore.fs.base import FileSystemDatastore
from vizier.datastore.mimir.store import MimirDatastore
from vizier.filestore.fs.base import FileSystemFilestore


CSV_FILE_1 = './tests/datastore/.files/dataset_7rows.csv'
CSV_FILE_2 = './tests/datastore/.files/pd5h-92mc.csv'

SERVER_DIR = '.tmp'
DATASTORE_DIR = os.path.join(SERVER_DIR, 'ds')
FILESERVER_DIR = os.path.join(SERVER_DIR, 'fs')

ENGINEENV_DEFAULT = 'default'
ENGINEENV_MIMIR = 'mimir'

class TestDatasetPaginationReader(unittest.TestCase):

    def set_up(self, engine):
        """Create an empty file server repository."""
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.mkdir(SERVER_DIR)
        # Setup file server
        self.fs = FileSystemFilestore(FILESERVER_DIR)
        # Setup the respective datastore and Vizual engine
        if engine == ENGINEENV_DEFAULT:
            self.datastore = FileSystemDatastore(DATASTORE_DIR)
        elif engine == ENGINEENV_MIMIR:
            self.datastore = MimirDatastore(DATASTORE_DIR)

    def tear_down(self, engine):
        """Clean-up by dropping file server directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_default_engine(self):
        """Test functionality for the default setup."""
        self.run_tests(ENGINEENV_DEFAULT)

    def test_mimir_engine(self):
        """Test functionality for the Mimir setup."""
        import vizier.mimir as mimir
        self.run_tests(ENGINEENV_MIMIR)

    def run_tests(self, engine):
        """Run sequence of tests for given configuration."""
        self.set_up(engine)
        ds = self.datastore.load_dataset(self.fs.upload_file(CSV_FILE_1))
        rows = ds.fetch_rows()
        self.assertEqual(len(rows), 7)
        rows = ds.fetch_rows(offset=1)
        self.assertEqual(len(rows), 6)
        self.assertEqual(rows[0].values[0], 'Bob')
        self.assertEqual(rows[5].values[0], 'Gertrud')
        rows = ds.fetch_rows(limit=2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].values[0], 'Alice')
        self.assertEqual(rows[1].values[0], 'Bob')
        rows = ds.fetch_rows(offset=4, limit=3)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].values[0], 'Eileen')
        self.assertEqual(rows[2].values[0], 'Gertrud')
        rows = ds.fetch_rows(offset=5, limit=3)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].values[0], 'Frank')
        self.assertEqual(rows[1].values[0], 'Gertrud')
        rows = ds.fetch_rows(offset=6, limit=3)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].values[0], 'Gertrud')
        # Test larger dataset with deletes
        ds = self.datastore.load_dataset(self.fs.upload_file(CSV_FILE_2))
        rows = ds.fetch_rows(offset=0, limit=10)
        self.assertEqual(len(rows), 10)
        rows = ds.fetch_rows(offset=10, limit=20)
        self.assertEqual(len(rows), 20)
        rows = ds.fetch_rows(offset=60, limit=10)
        self.assertEqual(len(rows), 3)
        self.tear_down(engine)


if __name__ == '__main__':
    unittest.main()
