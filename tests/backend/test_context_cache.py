"""Test the project context cache."""

import os
import shutil
import unittest

from vizier.datastore.fs.factory import FileSystemDatastoreFactory
from vizier.engine.backend.cache import ContextCache
from vizier.filestore.fs.factory import FileSystemFilestoreFactory


BASE_DIR = './.tmp'


class TestProjectContextCache(unittest.TestCase):
    def setUp(self):
        """Create an empty base directory."""
        if os.path.isdir(BASE_DIR):
            shutil.rmtree(BASE_DIR)
        os.makedirs(BASE_DIR)

    def tearDown(self):
        """Remove an existing base directory."""
        if os.path.isdir(BASE_DIR):
            shutil.rmtree(BASE_DIR)

    def test_cache(self):
        """Test adding and retrieving objects from the cache."""
        contexts = ContextCache(
            datastores=FileSystemDatastoreFactory(base_path=BASE_DIR),
            filestores=FileSystemFilestoreFactory(base_path=BASE_DIR)
        )
        self.assertEquals(len(contexts.cache), 0)
        context = contexts.get_context('ABC')
        self.assertIsNotNone(context)
        self.assertEquals(len(contexts.cache), 1)
        context = contexts.get_context('ABC')
        self.assertIsNotNone(context)
        self.assertEquals(len(contexts.cache), 1)
        context = contexts.get_context('XYZ')
        self.assertIsNotNone(context)
        self.assertEquals(len(contexts.cache), 2)


if __name__ == '__main__':
    unittest.main()
