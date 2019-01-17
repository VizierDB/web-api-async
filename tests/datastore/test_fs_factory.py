"""Test functionality of the default file store factory."""

import os
import shutil
import unittest

from vizier.datastore.fs.factory import FileSystemDatastoreFactory, PARA_DIRECTORY


SERVER_DIR = './.tmp'
CSV_FILE = './.files/dataset.csv'


class TestDefaultDatastoreFactory(unittest.TestCase):

    def setUp(self):
        """Create an empty datastore directory."""
        # Drop project descriptor directory
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def tearDown(self):
        """Clean-up by dropping datastore directory.
        """
        shutil.rmtree(SERVER_DIR)

    def test_create_and_delete_datastore(self):
        """Test the basic functionality of the file store factory."""
        fact = FileSystemDatastoreFactory(properties={PARA_DIRECTORY: SERVER_DIR})
        db = fact.get_datastore('0123')
        db = fact.get_datastore('4567')
        fact.delete_datastore('0123')
        self.assertFalse(os.path.isdir(os.path.join(SERVER_DIR, '0123')))
        self.assertTrue(os.path.isdir(os.path.join(SERVER_DIR, '4567')))
        db = fact.get_datastore('0123')
        self.assertTrue(os.path.isdir(os.path.join(SERVER_DIR, '0123')))
        self.assertTrue(os.path.isdir(os.path.join(SERVER_DIR, '4567')))
        # ValueError if no base path is given
        with self.assertRaises(ValueError):
            FileSystemDatastoreFactory()


if __name__ == '__main__':
    unittest.main()
