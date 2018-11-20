"""Test functionality of the file system viztrail handle."""

import os
import shutil
import unittest

from vizier.viztrail.driver.fs.repository import FSViztrailRepository
from vizier.viztrail.driver.fs.repository import DIR_VIZTRAILS
from vizier.viztrail.base import PROPERTY_NAME


REPO_DIR = './.temp'


class TestFSViztrailHandle(unittest.TestCase):

    def setUp(self):
        """Create an empty repository directory."""
        if os.path.isdir(REPO_DIR):
            shutil.rmtree(REPO_DIR)
        os.makedirs(REPO_DIR)

    def tearDown(self):
        """Delete repository directory.
        """
        shutil.rmtree(REPO_DIR)

    def test_create_viztrail(self):
        """Test creating a new viztrail."""
        repo = FSViztrailRepository(REPO_DIR)
        self.assertTrue(os.path.isdir(DIR_VIZTRAILS(REPO_DIR)))
        vt1 = repo.create_viztrail(exec_env_id='ENV1')
        vt2 = repo.create_viztrail(
            exec_env_id='ENV2',
            properties={PROPERTY_NAME: 'My Viztrail'}
        )
        self.assertEquals(len(repo.list_viztrails()), 2)
        self.assertIsNotNone(repo.get_viztrail(vt1.identifier))
        self.assertIsNotNone(repo.get_viztrail(vt2.identifier))
        self.assertNotEqual(vt1.identifier, vt2.identifier)
        self.assertIsNone(vt1.name)
        self.assertEquals(vt2.name, 'My Viztrail')
        self.assertEquals(vt1.exec_env_id, 'ENV1')
        self.assertEquals(vt2.exec_env_id, 'ENV2')
        # Reload the repository
        repo = FSViztrailRepository(REPO_DIR)
        self.assertEquals(len(repo.list_viztrails()), 2)
        vt1 = repo.get_viztrail(vt1.identifier)
        vt2 = repo.get_viztrail(vt2.identifier)
        self.assertIsNotNone(vt1)
        self.assertIsNotNone(vt2)
        self.assertNotEqual(vt1.identifier, vt2.identifier)
        self.assertIsNone(vt1.name)
        self.assertEquals(vt2.name, 'My Viztrail')
        self.assertEquals(vt1.exec_env_id, 'ENV1')
        self.assertEquals(vt2.exec_env_id, 'ENV2')

    def test_delete_viztrail(self):
        """Test creating a new viztrail."""
        repo = FSViztrailRepository(REPO_DIR)
        vt1 = repo.create_viztrail(exec_env_id='ENV1')
        vt2 = repo.create_viztrail(
            exec_env_id='ENV2',
            properties={PROPERTY_NAME: 'My Viztrail'}
        )
        # Returns false when deleting unknown viztrail
        self.assertIsNone(repo.get_viztrail(vt1.identifier + vt2.identifier))
        self.assertFalse(repo.delete_viztrail(vt1.identifier + vt2.identifier))
        # Reload the repository
        repo = FSViztrailRepository(REPO_DIR)
        self.assertEquals(len(repo.list_viztrails()), 2)
        self.assertTrue(repo.delete_viztrail(vt1.identifier))
        self.assertEquals(len(repo.list_viztrails()), 1)
        self.assertIsNone(repo.get_viztrail(vt1.identifier))
        self.assertIsNotNone(repo.get_viztrail(vt2.identifier))
        # Reload the repository
        repo = FSViztrailRepository(REPO_DIR)
        self.assertEquals(len(repo.list_viztrails()), 1)
        self.assertIsNone(repo.get_viztrail(vt1.identifier))
        self.assertIsNotNone(repo.get_viztrail(vt2.identifier))
        self.assertFalse(repo.delete_viztrail(vt1.identifier))
        self.assertTrue(repo.delete_viztrail(vt2.identifier))
        self.assertEquals(len(repo.list_viztrails()), 0)
        # Reload the repository
        repo = FSViztrailRepository(REPO_DIR)
        self.assertEquals(len(repo.list_viztrails()), 0)
        self.assertFalse(repo.delete_viztrail(vt1.identifier))
        self.assertFalse(repo.delete_viztrail(vt2.identifier))


if __name__ == '__main__':
    unittest.main()
