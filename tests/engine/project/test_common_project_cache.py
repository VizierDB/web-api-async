"""Test the functionality to create, access and delete projects for the default
vizier engine."""

import os
import shutil
import unittest

from vizier.datastore.fs.factory import FileSystemDatastoreFactory
from vizier.engine.project.cache.common import CommonProjectCache
from vizier.filestore.fs.factory import FileSystemFilestoreFactory
from vizier.viztrail.objectstore.repository import OSViztrailRepository
from vizier.viztrail.base import PROPERTY_NAME


SERVER_DIR = './.tmp'
DATASTORES_DIR = SERVER_DIR + '/ds'
FILESTORES_DIR = SERVER_DIR + '/fs'
VIZTRAILS_DIR = SERVER_DIR + '/vt'


class TestCommonProjectCache(unittest.TestCase):

    def setUp(self):
        """Create an instance of the default cache with an empty directory
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.makedirs(SERVER_DIR)
        self.cache = self.create_cache()

    def tearDown(self):
        """Remove the server directory."""
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def create_cache(self):
        """Create instance of the project cache."""
        return CommonProjectCache(
            datastores=FileSystemDatastoreFactory(DATASTORES_DIR),
            filestores=FileSystemFilestoreFactory(FILESTORES_DIR),
            viztrails=OSViztrailRepository(base_path=VIZTRAILS_DIR)
        )

    def test_empty_repository(self):
        """Test accessing and deleting projects for an empty repository."""
        self.assertEquals(len(self.cache.list_projects()), 0)
        self.assertIsNone(self.cache.get_project('000'))
        self.assertFalse(self.cache.delete_project('000'))

    def test_project_life_cycle(self):
        """Test creating, accessing, and deleting projects."""
        pj1 = self.cache.create_project({PROPERTY_NAME: 'My First Project'})
        self.assertEquals(len(self.cache.list_projects()), 1)
        pj2 = self.cache.create_project({PROPERTY_NAME: 'My Second Project'})
        self.assertEquals(len(self.cache.list_projects()), 2)
        self.assertEquals(self.cache.get_project(pj1.identifier).identifier, pj1.identifier)
        self.assertEquals(self.cache.get_project(pj1.identifier).name, 'My First Project')
        self.assertEquals(self.cache.get_project(pj2.identifier).identifier, pj2.identifier)
        self.assertEquals(self.cache.get_project(pj2.identifier).name, 'My Second Project')
        # Reload the repository
        self.cache = self.create_cache()
        self.assertEquals(len(self.cache.list_projects()), 2)
        self.assertEquals(self.cache.get_project(pj1.identifier).identifier, pj1.identifier)
        self.assertEquals(self.cache.get_project(pj1.identifier).name, 'My First Project')
        self.assertEquals(self.cache.get_project(pj2.identifier).identifier, pj2.identifier)
        self.assertEquals(self.cache.get_project(pj2.identifier).name, 'My Second Project')
        # Delete project
        self.assertTrue(self.cache.delete_project(pj1.identifier))
        self.assertEquals(len(self.cache.list_projects()), 1)
        self.assertIsNone(self.cache.get_project(pj1.identifier))
        self.assertEquals(self.cache.get_project(pj2.identifier).identifier, pj2.identifier)
        self.assertFalse(self.cache.delete_project(pj1.identifier))
        # Reload the repository
        self.assertFalse(self.cache.delete_project(pj1.identifier))
        self.assertEquals(len(self.cache.list_projects()), 1)
        self.assertIsNone(self.cache.get_project(pj1.identifier))
        self.assertEquals(self.cache.get_project(pj2.identifier).identifier, pj2.identifier)


if __name__ == '__main__':
    unittest.main()
