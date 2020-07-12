"""Test querying and manipulating annotations for a mimir dataset."""

import csv
import os
import shutil
import unittest

import vizier.mimir as mimir

from vizier.datastore.dataset import DatasetColumn
from vizier.datastore.mimir.store import MimirDatastore
from vizier.filestore.fs.base import FileSystemFilestore


DATA_FILE = './.files/w49k-mmkh.csv'

SERVER_DIR = '.tmp'
DATASTORE_DIRECTORY = os.path.join(SERVER_DIR, 'ds')
FILESERVER_DIR = os.path.join(SERVER_DIR, 'fs')


class TestMimirDatasetAnnotations(unittest.TestCase):

    def setUp(self):
        """Create empty server directory."""
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.mkdir(SERVER_DIR)
        self.fileserver = FileSystemFilestore(FILESERVER_DIR)
        self.db = MimirDatastore(DATASTORE_DIRECTORY)

    def tearDown(self):
        """Delete server directory."""
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_dataset_annotations(self):
        """Run test for Mimir datastore."""
        dh = self.db.load_dataset(
            f_handle=self.fileserver.upload_file(DATA_FILE)
        )
        ds = self.db.get_dataset(dh.identifier)
        rows = ds.fetch_rows()
        print(ds.row_ids)
        for row in rows:
            print(str(row.identifier) + '\t' + str(row.values))
        for row_id in ds.row_ids:
            for anno in ds.get_caveats(column_id=1, row_id=row_id):
                print(str(row_id) + '\t' + anno.key + '=' + str(anno.value))


if __name__ == '__main__':
    unittest.main()
