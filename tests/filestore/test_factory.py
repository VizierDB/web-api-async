"""Test functionality of the default file store factory."""

import os
import shutil
import unittest

from vizier.filestore.fs.base import PARA_DIRECTORY
from vizier.filestore.fs.factory import FileSystemFilestoreFactory


SERVER_DIR = './.tmp'
CSV_FILE = './.files/dataset.csv'


class TestFileSystemFilestoreFactory(unittest.TestCase):

    def setUp(self):
        """Create an empty file server repository."""
        # Drop project descriptor directory
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def tearDown(self):
        """Clean-up by dropping file server directory.
        """
        shutil.rmtree(SERVER_DIR)

    def test_create_and_delete_filestore(self):
        """Test the basic functionality of the file store factory."""
        fact = FileSystemFilestoreFactory(properties={PARA_DIRECTORY: SERVER_DIR})
        db = fact.get_filestore('0123')
        fh = db.upload_file(CSV_FILE)
        self.assertTrue(os.path.isdir(os.path.join(SERVER_DIR, '0123')))
        self.assertTrue(os.path.isfile(fh.filepath))
        db = fact.get_filestore('4567')
        fact.delete_filestore('0123')
        self.assertFalse(os.path.isdir(os.path.join(SERVER_DIR, '0123')))
        self.assertFalse(os.path.isfile(fh.filepath))
        self.assertTrue(os.path.isdir(os.path.join(SERVER_DIR, '4567')))
        db = fact.get_filestore('0123')
        self.assertTrue(os.path.isdir(os.path.join(SERVER_DIR, '0123')))
        self.assertTrue(os.path.isdir(os.path.join(SERVER_DIR, '4567')))
        # ValueError if no base path is given
        with self.assertRaises(ValueError):
            FileSystemFilestoreFactory()


if __name__ == '__main__':
    unittest.main()
