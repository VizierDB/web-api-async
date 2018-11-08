import os
import shutil
import unittest

from vizier.datastore.fs import FileSystemDataStore
from vizier.datastore.fs import DATA_FILE, METADATA_FILE
from vizier.filestore.base import DefaultFileServer


CSV_FILE = './data/dataset.csv'

DATASTORE_DIRECTORY = './env/ds'
FILESERVER_DIR = './env/fs'

class TestDataStore(unittest.TestCase):

    def setUp(self):
        """Create empty data store directory."""
        # Setup file server and upload file
        if os.path.isdir(FILESERVER_DIR):
            shutil.rmtree(FILESERVER_DIR)
        os.mkdir(FILESERVER_DIR)
        self.fileserver = DefaultFileServer(FILESERVER_DIR)
        # Remove directory if it exists
        if os.path.isdir(DATASTORE_DIRECTORY):
            shutil.rmtree(DATASTORE_DIRECTORY)
        os.mkdir(DATASTORE_DIRECTORY)
        self.db = FileSystemDataStore(DATASTORE_DIRECTORY)

    def tearDown(self):
        """Delete data store directory.
        """
        for d in [DATASTORE_DIRECTORY, FILESERVER_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)

    def test_datastore(self):
        """Test functionality of the file server data store."""
        ds = self.db.load_dataset(self.fileserver.upload_file(CSV_FILE))
        self.assertEquals(ds.column_counter, 3)
        self.assertEquals(ds.row_counter, 2)
        self.assertEquals(ds.row_count, 2)
        ds = self.db.get_dataset(ds.identifier)
        names = ['Name', 'Age', 'Salary']
        for i in range(3):
            col = ds.columns[i]
            self.assertEquals(col.identifier, i)
            self.assertEquals(col.name, names[i])
        rows = ds.fetch_rows()
        self.assertEquals(len(rows), ds.row_count)
        for i in range(len(rows)):
            row = rows[i]
            self.assertEquals(row.identifier, i)
        rows[0].values[0] = 'Jane'
        ds = self.db.create_dataset(columns=ds.columns, rows=rows)
        ds = self.db.get_dataset(ds.identifier)
        for i in range(3):
            col = ds.columns[i]
            self.assertEquals(col.identifier, i)
            self.assertEquals(col.name, names[i])
        rows = ds.fetch_rows()
        self.assertEquals(len(rows), ds.row_count)
        for i in range(len(rows)):
            row = rows[i]
            self.assertEquals(row.identifier, i)
        self.assertEquals(rows[0].values[0], 'Jane')


if __name__ == '__main__':
    unittest.main()
