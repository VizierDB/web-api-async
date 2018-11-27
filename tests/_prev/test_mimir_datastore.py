import os
import shutil
import unittest

import vistrails.packages.mimir.init as mimir

from vizier.datastore.mimir import MimirDataStore, COL_PREFIX, ROW_ID
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
        self.db = MimirDataStore(DATASTORE_DIRECTORY)

    def tearDown(self):
        """Delete data store directory.
        """
        for d in [DATASTORE_DIRECTORY, FILESERVER_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)

    def test_datastore(self):
        """Test functionality of the file server data store."""
        mimir.initialize()
        ds = self.db.load_dataset(self.fileserver.upload_file(CSV_FILE))
        self.assertEquals(ds.column_counter, 3)
        self.assertEquals(ds.row_counter, 2)
        self.assertEquals(ds.row_count, 2)
        cols = [
            ('NAME', COL_PREFIX + '0', 'varchar'),
            ('AGE', COL_PREFIX + '1', 'int'),
            ('SALARY', COL_PREFIX + '2', 'varchar')
        ]
        control_rows = [
            (0 ,['Alice', 23, '35K']),
            (1, ['Bob', 32, '30K'])
        ]
        for column in ds.columns:
            self.validate_column(column, cols[column.identifier])
        self.validate_rowid_column(ds.rowid_column)
        self.validate_rows(ds.fetch_rows(), control_rows)
        # Get dataset and repeat tests
        ds = self.db.get_dataset(ds.identifier)
        self.assertEquals(ds.column_counter, 3)
        self.assertEquals(ds.row_counter, 2)
        self.assertEquals(len(ds.row_ids), 2)
        for column in ds.columns:
            self.validate_column(column, cols[column.identifier])
        self.validate_rowid_column(ds.rowid_column)
        self.validate_rows(ds.fetch_rows(), control_rows)
        # Create dataset
        names = ['NAME', 'AGE', 'SALARY']
        rows = ds.fetch_rows()
        rows[0].values[0] = 'Jane'
        rows = [rows[1], rows[0]]
        ds = self.db.create_dataset(columns=ds.columns, rows=rows)
        ds = self.db.get_dataset(ds.identifier)
        for i in range(3):
            col = ds.columns[i]
            self.assertEquals(col.identifier, i)
            self.assertEquals(col.name, names[i])
        rows = ds.fetch_rows()
        for i in range(len(rows)):
            row = rows[(len(rows) - 1) - i]
            self.assertEquals(row.identifier, i)
        self.assertEquals(rows[1].values[0], 'Jane')
        # DONE
        mimir.finalize()

    def validate_column(self, column, col_props):
        """Validate that column name and data type are as expected."""
        name, name_in_rdb, data_type = col_props
        self.assertEquals(column.name, name)
        self.assertEquals(column.name_in_rdb, name_in_rdb)
        self.assertEquals(column.data_type, data_type)

    def validate_rowid_column(self, col):
        """Ensure the row id column has the correct name and a data type."""
        self.assertEquals(col.name, col.name_in_rdb)
        self.assertEquals(col.name, ROW_ID)
        self.assertEquals(col.data_type, 'int')

    def validate_rows(self, dataset_rows, control_rows):
        """Make sure all data is read correctly."""
        self.assertEquals(len(dataset_rows), len(control_rows))
        for i in range(len(dataset_rows)):
            ds_row = dataset_rows[i]
            row_id, values = control_rows[i]
            self.assertEquals(ds_row.identifier, row_id)
            self.assertEquals(ds_row.values, values)


if __name__ == '__main__':
    unittest.main()
