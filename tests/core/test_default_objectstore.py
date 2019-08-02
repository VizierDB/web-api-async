"""Test the functionality of the default object store."""

import os
import shutil
import unittest

from vizier.core.io.base import DefaultObjectStore
from vizier.core.io.base import MAX_ATTEMPS, PARA_KEEP_DELETED, PARA_LONG_IDENTIFIER
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


class TestDefaultObjectStore(unittest.TestCase):

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
        store = DefaultObjectStore()
        self.assertEqual(store.create_folder(BASE_DIRECTORY, identifier='A'), 'A')
        self.assertTrue(store.exists(store.join(BASE_DIRECTORY, 'A')))
        self.assertTrue(os.path.isdir(os.path.join(BASE_DIRECTORY, 'A')))
        identifier = store.create_folder(BASE_DIRECTORY)
        self.assertTrue(store.exists(store.join(BASE_DIRECTORY, identifier)))
        self.assertTrue(os.path.isdir(os.path.join(BASE_DIRECTORY, identifier)))
        # New store with short identifier factory
        store = DefaultObjectStore(identifier_factory=get_short_identifier)
        short_id = store.create_folder(BASE_DIRECTORY)
        self.assertTrue(store.exists(store.join(BASE_DIRECTORY, short_id)))
        self.assertTrue(os.path.isdir(os.path.join(BASE_DIRECTORY, short_id)))
        # Delete folder with identifier
        store.delete_folder(store.join(BASE_DIRECTORY, identifier))
        self.assertFalse(store.exists(store.join(BASE_DIRECTORY, identifier)))
        self.assertFalse(os.path.isdir(os.path.join(BASE_DIRECTORY, identifier)))
        # Delete folder with short_id when the keep_deleted_files flag is True
        store = DefaultObjectStore(keep_deleted_files=True)
        store.delete_folder(store.join(BASE_DIRECTORY, short_id))
        self.assertTrue(store.exists(store.join(BASE_DIRECTORY, short_id)))
        self.assertTrue(os.path.isdir(os.path.join(BASE_DIRECTORY, short_id)))
        # Delete folder 'A' overriding the keep_deleted_files flag
        self.assertTrue(store.exists(store.join(BASE_DIRECTORY, 'A')))
        self.assertTrue(os.path.isdir(os.path.join(BASE_DIRECTORY, 'A')))
        store.delete_folder(store.join(BASE_DIRECTORY, 'A'), force_delete=True)
        self.assertFalse(store.exists(store.join(BASE_DIRECTORY, 'A')))
        self.assertFalse(os.path.isdir(os.path.join(BASE_DIRECTORY, 'A')))

    def test_create_object_with_identifier(self):
        """Test creating a new object with a given identifier."""
        store = DefaultObjectStore()
        store.create_object(BASE_DIRECTORY, identifier='A')
        self.assertTrue(os.path.isfile(os.path.join(BASE_DIRECTORY, 'A')))
        with self.assertRaises(ValueError):
            store.read_object(store.join(BASE_DIRECTORY, 'A'))
        store.create_object(BASE_DIRECTORY, identifier='B', content={'id': 100})
        self.assertTrue(os.path.isfile(os.path.join(BASE_DIRECTORY, 'B')))
        content = store.read_object(store.join(BASE_DIRECTORY, 'B'))
        self.assertEqual(content['id'], 100)
        store.create_object(BASE_DIRECTORY, identifier='A', content={'id': 100})
        self.assertTrue(os.path.isfile(os.path.join(BASE_DIRECTORY, 'A')))
        content = store.read_object(store.join(BASE_DIRECTORY, 'A'))
        self.assertEqual(content['id'], 100)
        store.create_object(BASE_DIRECTORY, identifier='B')
        self.assertTrue(os.path.isfile(os.path.join(BASE_DIRECTORY, 'B')))
        with self.assertRaises(ValueError):
            store.read_object(store.join(BASE_DIRECTORY, 'B'))

    def test_create_file_repeat(self):
        """Test create file with identifier factory that not always returns
        a unique identifier.
        """
        store = DefaultObjectStore(
            identifier_factory=IdFactory(max_attempts=MAX_ATTEMPS-1)
        )
        id1 = store.create_object(BASE_DIRECTORY)
        id2 = store.create_object(BASE_DIRECTORY)
        self.assertNotEqual(id1, id2)
        self.assertTrue(store.exists(store.join(BASE_DIRECTORY, id1)))
        self.assertTrue(store.exists(store.join(BASE_DIRECTORY, id2)))
        store.delete_object(store.join(BASE_DIRECTORY, id1))
        store.delete_object(store.join(BASE_DIRECTORY, id2))
        store = DefaultObjectStore(
            identifier_factory=IdFactory(max_attempts=MAX_ATTEMPS+1)
        )
        id1 = store.create_object(BASE_DIRECTORY)
        with self.assertRaises(RuntimeError):
            store.create_object(BASE_DIRECTORY)

    def test_create_folder_repeat(self):
        """Test create folder with identifier factory that not always returns
        a unique identifier.
        """
        store = DefaultObjectStore(
            identifier_factory=IdFactory(max_attempts=MAX_ATTEMPS-1)
        )
        id1 = store.create_folder(BASE_DIRECTORY)
        id2 = store.create_folder(BASE_DIRECTORY)
        self.assertNotEqual(id1, id2)
        self.assertTrue(store.exists(store.join(BASE_DIRECTORY, id1)))
        self.assertTrue(store.exists(store.join(BASE_DIRECTORY, id2)))
        store.delete_folder(store.join(BASE_DIRECTORY, id1))
        store.delete_folder(store.join(BASE_DIRECTORY, id2))
        store = DefaultObjectStore(
            identifier_factory=IdFactory(max_attempts=MAX_ATTEMPS+1)
        )
        id1 = store.create_folder(BASE_DIRECTORY)
        with self.assertRaises(RuntimeError):
            store.create_folder(BASE_DIRECTORY)

    def test_error_on_missing(self):
        """Test that reading a missing object will raise a ValueError."""
        store = DefaultObjectStore()
        filename = store.join(BASE_DIRECTORY, 'A.file')
        store.create_object(BASE_DIRECTORY, identifier='A.file', content={'A': 1})
        self.assertTrue(store.exists(filename))
        # Re-create the store to ensure that this has no effect
        store = DefaultObjectStore()
        store.read_object(filename)
        os.remove(filename)
        with self.assertRaises(ValueError):
            store.read_object(filename)

    def test_exists(self):
        """Test exists method."""
        store = DefaultObjectStore()
        filename = store.join(BASE_DIRECTORY, 'A.file')
        dirname = store.join(BASE_DIRECTORY, 'A.dir')
        self.assertFalse(store.exists(filename))
        store.create_object(BASE_DIRECTORY, identifier='A.file')
        self.assertTrue(store.exists(filename))
        self.assertFalse(store.exists(dirname))
        os.makedirs(dirname)
        self.assertTrue(store.exists(dirname))
        # Re-create the store to ensure that this has no effect
        store = DefaultObjectStore()
        self.assertTrue(store.exists(filename))
        self.assertTrue(store.exists(dirname))

    def test_list_folders(self):
        """Test list_folders method."""
        store = DefaultObjectStore()
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
        os.makedirs(store.join(dirname, 'A'))
        dirs = store.list_folders(parent_folder=dirname)
        self.assertEqual(len(dirs), 1)
        self.assertTrue('A' in dirs)
        os.makedirs(store.join(dirname, 'B'))
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
        # Re-create the store to ensure that this has no effect
        store = DefaultObjectStore()
        dirs = store.list_folders(parent_folder=dirname, create=True)
        self.assertEqual(len(dirs), 2)
        self.assertTrue('A' in dirs)
        self.assertTrue('B' in dirs)


if __name__ == '__main__':
    unittest.main()
