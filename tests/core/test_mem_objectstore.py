"""Test the functionality of the in-memory object store."""

import os
import shutil
import unittest

from vizier.core.io.base import MAX_ATTEMPS
from vizier.core.io.mem import MemObjectStore
from vizier.core.util import get_short_identifier


"""Base directory for all resources."""
BASE_DIRECTORY = './.files/'


class IdFactory(object):
    """Implement unique identifier factory for testing purposes."""
    def __init__(self, max_attempts=1):
        self.attempts = 0
        self.counter = 0
        self.max_attempts=max_attempts

    def __call__(self):
        """Returns a new unique identifier after max attempts."""
        if self.attempts >= self.max_attempts:
            self.counter += 1
            self.attempts = 0
        self.attempts += 1
        return 'ID' + str(self.counter)


class TestMemObjectStore(unittest.TestCase):

    def setUp(self):
        """Create an empty directory for resource files."""
        if os.path.isdir(BASE_DIRECTORY):
            shutil.rmtree(BASE_DIRECTORY)
        os.makedirs(BASE_DIRECTORY)

    def tearDown(self):
        """Delete base directory."""
        shutil.rmtree(BASE_DIRECTORY)

    def test_create_and_delete_folder(self):
        """Test default functionality of create_folder and delete_folder
        methods.
        """
        store = MemObjectStore()
        self.assertEqual(store.create_folder(BASE_DIRECTORY, identifier='A'), 'A')
        self.assertTrue(store.exists(store.join(BASE_DIRECTORY, 'A')))
        self.assertFalse(os.path.isdir(os.path.join(BASE_DIRECTORY, 'A')))
        identifier = store.create_folder(BASE_DIRECTORY)
        self.assertTrue(store.exists(store.join(BASE_DIRECTORY, identifier)))
        self.assertFalse(os.path.isdir(os.path.join(BASE_DIRECTORY, identifier)))
        # Delete folder with identifier
        store.delete_folder(store.join(BASE_DIRECTORY, identifier))
        self.assertFalse(store.exists(store.join(BASE_DIRECTORY, identifier)))

    def test_create_file_repeat(self):
        """Test create folder with identifier factory that not always returns
        a unique identifier.
        """
        store = MemObjectStore(
            identifier_factory=IdFactory(max_attempts=MAX_ATTEMPS-1)
        )
        id1 = store.create_object(BASE_DIRECTORY)
        id2 = store.create_object(BASE_DIRECTORY)
        self.assertNotEqual(id1, id2)
        self.assertTrue(store.exists(store.join(BASE_DIRECTORY, id1)))
        self.assertTrue(store.exists(store.join(BASE_DIRECTORY, id2)))
        store.delete_object(store.join(BASE_DIRECTORY, id1))
        store.delete_object(store.join(BASE_DIRECTORY, id2))
        store = MemObjectStore(
            identifier_factory=IdFactory(max_attempts=MAX_ATTEMPS+1)
        )
        id1 = store.create_object(BASE_DIRECTORY)
        with self.assertRaises(RuntimeError):
            store.create_object(BASE_DIRECTORY)

    def test_create_folder_repeat(self):
        """Test create folder with identifier factory that not always returns
        a unique identifier.
        """
        store = MemObjectStore(
            identifier_factory=IdFactory(max_attempts=MAX_ATTEMPS-1)
        )
        id1 = store.create_folder(BASE_DIRECTORY)
        id2 = store.create_folder(BASE_DIRECTORY)
        self.assertNotEqual(id1, id2)
        self.assertTrue(store.exists(store.join(BASE_DIRECTORY, id1)))
        self.assertTrue(store.exists(store.join(BASE_DIRECTORY, id2)))
        store.delete_folder(store.join(BASE_DIRECTORY, id1))
        store.delete_folder(store.join(BASE_DIRECTORY, id2))
        store = MemObjectStore(
            identifier_factory=IdFactory(max_attempts=MAX_ATTEMPS+1)
        )
        id1 = store.create_folder(BASE_DIRECTORY)
        with self.assertRaises(RuntimeError):
            store.create_folder(BASE_DIRECTORY)

    def test_exists(self):
        """Test exists method."""
        store = MemObjectStore()
        filename = store.join(BASE_DIRECTORY, 'A.file')
        dirname = store.join(BASE_DIRECTORY, 'A.dir')
        self.assertFalse(store.exists(filename))
        store.create_object(BASE_DIRECTORY, identifier='A.file')
        self.assertTrue(store.exists(filename))
        self.assertFalse(store.exists(dirname))
        store.create_folder(BASE_DIRECTORY, identifier='A.dir')
        self.assertTrue(store.exists(dirname))

    def test_list_folders(self):
        """Test list_folders method."""
        store = MemObjectStore()
        # The result is an empty list even if the folder does not exist and
        # is not created using the create flag
        dirname = store.join(BASE_DIRECTORY, 'A')
        dirs = store.list_folders(parent_folder=dirname, create=False)
        self.assertEqual(len(dirs), 0)
        self.assertFalse(store.exists(dirname))
        # The result is an empty list after the folder is created using the
        # create flag
        dirs = store.list_folders(parent_folder=dirname, create=True)
        self.assertEqual(len(dirs), 0)
        self.assertTrue(store.exists(dirname))
        # Create directories and files
        store.create_folder(dirname, 'A')
        dirs = store.list_folders(parent_folder=dirname)
        self.assertEqual(len(dirs), 1)
        self.assertTrue('A' in dirs)
        store.create_folder(dirname, 'B')
        dirs = store.list_folders(parent_folder=dirname, create=True)
        self.assertEqual(len(dirs), 2)
        self.assertTrue('A' in dirs)
        self.assertTrue('B' in dirs)
        filename = store.join(BASE_DIRECTORY, 'A.file')
        store.create_object(BASE_DIRECTORY, identifier='A.file')
        dirs = store.list_folders(parent_folder=dirname, create=True)
        self.assertEqual(len(dirs), 2)
        self.assertTrue('A' in dirs)
        self.assertTrue('B' in dirs)


if __name__ == '__main__':
    unittest.main()
