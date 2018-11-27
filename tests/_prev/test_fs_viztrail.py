import os
import shutil
import unittest

from vizier.config import ExecEnv, FileServerConfig, ENGINEENV_DEFAULT, ENGINEENV_MIMIR
from vizier.core.properties import FilePropertiesHandler
from vizier.workflow.base import DEFAULT_BRANCH, ViztrailBranch
from vizier.workflow.command import PACKAGE_MIMIR, PACKAGE_PYTHON, PACKAGE_VIZUAL
from vizier.workflow.repository.fs import PROPERTIES_FILE, FileSystemViztrailHandle, FileSystemViztrailRepository


VIZTRAIL_DIR = './data/fs/viztrail'

repos = {
    ENGINEENV_DEFAULT: ExecEnv(FileServerConfig()),
    ENGINEENV_MIMIR: ExecEnv(FileServerConfig(), identifier=ENGINEENV_MIMIR)
}


class TestFileSystemViztrails(unittest.TestCase):

    def setUp(self):
        """Create an empty file server repository."""
        # Drop the viztrail directory and re-create it
        if os.path.isdir(VIZTRAIL_DIR):
            shutil.rmtree(VIZTRAIL_DIR)
        os.makedirs(VIZTRAIL_DIR)

    def tearDown(self):
        """Clean-up by dropping file server directory.
        """
        # Drop the viztrail directory
        if os.path.isdir(VIZTRAIL_DIR):
            shutil.rmtree(VIZTRAIL_DIR)

    def test_read_write_handle(self):
        """Test reading and writing viztrail handle."""
        # An error is raised if we access a non-existing viztrail
        with self.assertRaises(IOError):
            viztrail = FileSystemViztrailHandle.from_file(VIZTRAIL_DIR, repos)
        viztrail = FileSystemViztrailHandle.create_viztrail(
            VIZTRAIL_DIR,
            'ID',
            repos[ENGINEENV_DEFAULT],
            properties={'name': 'My Viztrail'}
        )
        # Assert viztrail properties
        self.assertEquals(viztrail.identifier, 'ID')
        self.assertEquals(viztrail.properties['name'], 'My Viztrail')
        self.assertTrue(PACKAGE_VIZUAL in viztrail.command_repository)
        self.assertTrue(PACKAGE_PYTHON in viztrail.command_repository)
        self.assertFalse(PACKAGE_MIMIR in viztrail.command_repository)
        self.assertEquals(viztrail.version_counter.value, 0)
        self.assertEquals(viztrail.module_counter.value, 0)
        # Read viztrail handle from file and assert that all properties are
        # unchanges
        viztrail = FileSystemViztrailHandle.from_file(VIZTRAIL_DIR, repos)
        self.assertEquals(viztrail.identifier, 'ID')
        self.assertEquals(viztrail.properties['name'], 'My Viztrail')
        self.assertTrue(PACKAGE_VIZUAL in viztrail.command_repository)
        self.assertTrue(PACKAGE_PYTHON in viztrail.command_repository)
        self.assertFalse(PACKAGE_MIMIR in viztrail.command_repository)
        self.assertEquals(viztrail.version_counter.value, 0)
        self.assertEquals(viztrail.module_counter.value, 0)
        # Updatecommand repository, version counter and module counter
        viztrail.version_counter.inc()
        viztrail.module_counter.inc()
        viztrail.module_counter.inc()
        # Save current last updated at timestamp
        ts = viztrail.last_modified_at
        from time import sleep
        sleep(1)
        # Write and read updated viztrail
        viztrail.to_file()
        viztrail = FileSystemViztrailHandle.from_file(VIZTRAIL_DIR, repos)
        self.assertEquals(viztrail.identifier, 'ID')
        self.assertEquals(viztrail.properties['name'], 'My Viztrail')
        self.assertEquals(viztrail.version_counter.value, 1)
        self.assertEquals(viztrail.module_counter.value, 2)
        self.assertNotEquals(ts, viztrail.last_modified_at)
        # Delete the viztrail. Should raise IOError if we attempt to load the
        # viztrail again
        viztrail.delete()
        with self.assertRaises(IOError):
            viztrail = FileSystemViztrailHandle.from_file(VIZTRAIL_DIR, repos)

    def test_viztrail_branches(self):
        """Test basic functionality of creating a branch.
        """
        repo = FileSystemViztrailRepository(VIZTRAIL_DIR, repos)
        viztrail = repo.create_viztrail(ENGINEENV_DEFAULT, {'name': 'Name A'})
        # Branching of an unknown viztrail will return None
        self.assertIsNone(repo.create_branch('unknown', DEFAULT_BRANCH, {'name': 'My Branch'}))
        # Branching of an empty branch raises a ValueError
        with self.assertRaises(ValueError):
            repo.create_branch(viztrail.identifier, DEFAULT_BRANCH, {'name': 'My Branch'})
        # Re-load repository and repreat previous assertions
        repo = FileSystemViztrailRepository(VIZTRAIL_DIR, repos)
        self.assertIsNone(repo.create_branch('unknown', DEFAULT_BRANCH, {'name': 'My Branch'}))
        with self.assertRaises(ValueError):
            repo.create_branch(viztrail.identifier, DEFAULT_BRANCH, {'name': 'My Branch'})
        # The master branch provenance does not contain any information
        prov = repo.get_viztrail(viztrail.identifier).branches[DEFAULT_BRANCH].provenance
        self.assertIsNone(prov.source_branch)
        self.assertTrue(prov.workflow_version < 0)
        self.assertTrue(prov.module_id < 0)

    def test_viztrail_repository(self):
        """Test basic functionality of managing viztrails in the FS repository.
        """
        repo = FileSystemViztrailRepository(VIZTRAIL_DIR, repos)
        self.assertEquals(len(repo.list_viztrails()), 0)
        # Create two viztrails
        vt1 = repo.create_viztrail(ENGINEENV_DEFAULT, {'name': 'Name A'})
        self.assertEquals(vt1.properties['name'], 'Name A')
        self.assertTrue(PACKAGE_VIZUAL in vt1.command_repository)
        self.assertTrue(PACKAGE_PYTHON in vt1.command_repository)
        self.assertFalse(PACKAGE_MIMIR in vt1.command_repository)
        vt2 = repo.create_viztrail(ENGINEENV_MIMIR, {'name': 'Name B'})
        self.assertEquals(vt2.properties['name'], 'Name B')
        self.assertTrue(PACKAGE_VIZUAL in vt2.command_repository)
        self.assertTrue(PACKAGE_PYTHON in vt2.command_repository)
        self.assertTrue(PACKAGE_MIMIR in vt2.command_repository)
        self.assertEquals(len(repo.list_viztrails()), 2)
        # Re-load the repository
        repo = FileSystemViztrailRepository(VIZTRAIL_DIR, repos)
        self.assertEquals(len(repo.list_viztrails()), 2)
        vt1 = repo.get_viztrail(vt1.identifier)
        self.assertEquals(vt1.properties['name'], 'Name A')
        self.assertTrue(PACKAGE_VIZUAL in vt1.command_repository)
        self.assertTrue(PACKAGE_PYTHON in vt1.command_repository)
        self.assertFalse(PACKAGE_MIMIR in vt1.command_repository)
        vt2 = repo.get_viztrail(vt2.identifier)
        self.assertEquals(vt2.properties['name'], 'Name B')
        self.assertTrue(PACKAGE_VIZUAL in vt2.command_repository)
        self.assertTrue(PACKAGE_PYTHON in vt2.command_repository)
        self.assertTrue(PACKAGE_MIMIR in vt2.command_repository)
        # Delete the first viztrail
        self.assertTrue(repo.delete_viztrail(vt1.identifier))
        # Re-load the repository
        repo = FileSystemViztrailRepository(VIZTRAIL_DIR, repos)
        self.assertEquals(len(repo.list_viztrails()), 1)
        self.assertIsNone(repo.get_viztrail(vt1.identifier))
        self.assertIsNotNone(repo.get_viztrail(vt2.identifier))
        vt2 = repo.list_viztrails()[0]
        self.assertEquals(vt2.properties['name'], 'Name B')
        self.assertTrue(PACKAGE_VIZUAL in vt2.command_repository)
        self.assertTrue(PACKAGE_PYTHON in vt2.command_repository)
        self.assertTrue(PACKAGE_MIMIR in vt2.command_repository)
        self.assertFalse(repo.delete_viztrail(vt1.identifier))

    def test_viztrail_workflow(self):
        """Test basic functionality of retrieving a workflow.
        """
        repo = FileSystemViztrailRepository(VIZTRAIL_DIR, repos)
        viztrail = repo.create_viztrail(ENGINEENV_DEFAULT, {'name': 'Name A'})
        self.assertEquals(len(repo.get_workflow(viztrail.identifier, DEFAULT_BRANCH).modules), 0)
        self.assertIsNone(repo.get_workflow(viztrail.identifier, 'unknown'))
        self.assertIsNone(repo.get_workflow('unknown', DEFAULT_BRANCH))
        self.assertIsNone(repo.get_workflow(viztrail_id=viztrail.identifier, branch_id=DEFAULT_BRANCH, workflow_version=10))
        # Re-load repository
        repo = FileSystemViztrailRepository(VIZTRAIL_DIR, repos)
        self.assertEquals(len(repo.get_workflow(viztrail.identifier, DEFAULT_BRANCH).modules), 0)
        self.assertIsNone(repo.get_workflow(viztrail.identifier, 'unknown'))
        self.assertIsNone(repo.get_workflow('unknown', DEFAULT_BRANCH))
        self.assertIsNone(repo.get_workflow(viztrail_id=viztrail.identifier, branch_id=DEFAULT_BRANCH, workflow_version=10))


if __name__ == '__main__':
    unittest.main()
