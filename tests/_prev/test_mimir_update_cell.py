"""Test worktrail repository implementation that uses the file system for
storage.
"""

import os
import shutil
import unittest

import vistrails.packages.mimir.init as mimir

from vizier.datastore.mimir import MimirDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.vizual.mimir import MimirVizualEngine


DATASTORE_DIR = './env/ds'
FILESERVER_DIR = './env/fs'

CSV_FILE = './data/mimir/jsonsampletocsv.csv'
CSV_FILE_DT = './data/mimir/DetectSeriesTest1.csv'


class TestLoadMimirDataset(unittest.TestCase):

    def setUp(self):
        """Create an empty work trails repository."""
        # Cleanup first
        self.cleanUp()
        self.datastore = MimirDataStore(DATASTORE_DIR)
        self.fileserver = DefaultFileServer(FILESERVER_DIR)
        self.vizual = MimirVizualEngine(self.datastore, self.fileserver)

    def tearDown(self):
        """Clean-up by deleting directories.
        """
        self.cleanUp()

    def cleanUp(self):
        """Remove datastore and fileserver directory."""
        # Delete directories
        for d in [DATASTORE_DIR, FILESERVER_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)

    def test_load(self):
        """Run workflow with default configuration."""
        mimir.initialize()
        self.update_cell(CSV_FILE, 2, 0, 'int', 10)
        self.update_cell(CSV_FILE, 2, 0, 'int', 10.3, result_type='real')
        self.update_cell(CSV_FILE, 2, 0, 'int', None)
        self.update_cell(CSV_FILE, 3, 0, 'real', 10.3)
        self.update_cell(CSV_FILE, 3, 0, 'real', 10, result_value=10.0)
        self.update_cell(CSV_FILE, 3, 0, 'real', 'A', result_type='varchar')
        self.update_cell(CSV_FILE, 3, 0, 'real', None)
        self.update_cell(CSV_FILE, 4, 0, 'varchar', 'A')
        self.update_cell(CSV_FILE, 4, 0, 'varchar', 10, result_value='10')
        self.update_cell(CSV_FILE, 4, 0, 'varchar', 10.87, result_value='10.87')
        self.update_cell(CSV_FILE, 4, 0, 'varchar', None)
        self.update_cell(CSV_FILE, 8, 0, 'bool', 'False', result_value=False)
        self.update_cell(CSV_FILE, 8, 0, 'bool', '0', result_value=False)
        self.update_cell(CSV_FILE, 8, 0, 'bool', None)
        self.update_cell(CSV_FILE, 8, 1, 'bool', True, result_value=True)
        self.update_cell(CSV_FILE, 8, 1, 'bool', '1', result_value=True)
        self.update_cell(CSV_FILE, 8, 1, 'bool', 'A', result_value='A', result_type='varchar')
        self.update_cell(CSV_FILE, 8, 1, 'bool', 10.87, result_value='10.87', result_type='varchar')
        self.update_cell(CSV_FILE_DT, 1, 0, 'date', '2018-05-09')
        self.update_cell(CSV_FILE_DT, 1, 0, 'date', '20180509', result_value='20180509', result_type='varchar')
        self.update_cell(CSV_FILE_DT, 1, 0, 'date', None)
        self.update_cell(CSV_FILE_DT, 0, 0, 'datetime', '2018-05-09 12:03:22.0000')
        self.update_cell(CSV_FILE_DT, 0, 0, 'datetime', 'ABC', result_value='ABC', result_type='varchar')
        self.update_cell(CSV_FILE_DT, 0, 0, 'datetime', None)
        mimir.finalize()

    def update_cell(self, filename, col, row, data_type, value, result_value=None, result_type=None):
        """Update the value of the given cell. The column data type is expected
        to match the given datatype. The optional result value is the expected
        value of the cell in the modified dataset.
        """
        f_handle = self.fileserver.upload_file(filename)
        ds = self.datastore.load_dataset(f_handle)
        #print [c.name_in_rdb + ' AS ' + c.name + '(' + c.data_type + ')' for c in ds.columns]
        self.assertEquals(ds.columns[col].data_type, data_type)
        rows = ds.fetch_rows()
        self.assertNotEquals(rows[row].values[col], value)
        _, ds_id = self.vizual.update_cell(ds.identifier, col, row, value)
        ds = self.datastore.get_dataset(ds_id)
        #print [c.name_in_rdb + ' AS ' + c.name + '(' + c.data_type + ')' for c in ds.columns]
        if result_type is None:
            self.assertEquals(ds.columns[col].data_type, data_type)
        else:
            self.assertEquals(ds.columns[col].data_type, result_type)
        rows = ds.fetch_rows()
        if result_value is None:
            self.assertEquals(rows[row].values[col], value)
        else:
            self.assertEquals(rows[row].values[col], result_value)
        self.fileserver.delete_file(f_handle.identifier)



if __name__ == '__main__':
    unittest.main()
