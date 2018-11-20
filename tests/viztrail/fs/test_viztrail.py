"""Test functionality of the file system viztrail repository."""

import os
import shutil
import unittest


from vizier.viztrail.driver.fs.viztrail import FSViztrailHandle
from vizier.viztrail.driver.fs.viztrail import FILE_PROPERTIES, FILE_VIZTRAIL
from vizier.viztrail.driver.fs.viztrail import DIR_BRANCHES, DIR_MODULES, DIR_WORKFLOWS
from vizier.viztrail.base import PROPERTY_NAME


REPO_DIR = './.temp'


class TestFSViztrailRepository(unittest.TestCase):

    def setUp(self):
        """Create an empty repository directory."""
        if os.path.isdir(REPO_DIR):
            shutil.rmtree(REPO_DIR)
        os.makedirs(REPO_DIR)

    def tearDown(self):
        """Delete repository directory.
        """
        shutil.rmtree(REPO_DIR)

    def test_create_and_delete_branch(self):
        """Test creating and deleting a new branch."""
        base_dir = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        vt = FSViztrailHandle.create_viztrail(
            identifier='ABC',
            properties=None,
            exec_env_id='ENV1',
            base_dir=base_dir
        )
        vt.create_branch()

    def test_create_empty_properties(self):
        """Ensure that create without pre-defined properties works."""
        base_dir = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        vt = FSViztrailHandle.create_viztrail(
            identifier='ABC',
            properties=None,
            exec_env_id='ENV1',
            base_dir=base_dir
        )
        self.assertEquals(vt.identifier, 'ABC')
        self.assertEquals(vt.exec_env_id, 'ENV1')
        self.assertIsNone(vt.name)

    def test_create_load_delete(self):
        """Ensure that create and load works properly."""
        base_dir = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        vt = FSViztrailHandle.create_viztrail(
            identifier='DEF',
            properties={PROPERTY_NAME: 'My Viztrail'},
            exec_env_id='ENV1',
            base_dir=base_dir
        )
        # Ensure that all files and subfolders are created
        self.assertTrue(os.path.isfile(FILE_PROPERTIES(base_dir)))
        self.assertTrue(os.path.isfile(FILE_VIZTRAIL(base_dir)))
        self.assertTrue(os.path.isdir(DIR_BRANCHES(base_dir)))
        self.assertTrue(os.path.isdir(DIR_MODULES(base_dir)))
        self.assertTrue(os.path.isdir(DIR_WORKFLOWS(base_dir)))
        # Update name property
        self.assertEquals(vt.identifier, 'DEF')
        self.assertEquals(vt.exec_env_id, 'ENV1')
        self.assertEquals(vt.name, 'My Viztrail')
        vt.name = 'A Name'
        self.assertEquals(vt.name, 'A Name')
        # Load viztrail from disk
        vt = FSViztrailHandle.load_viztrail(base_dir)
        self.assertEquals(vt.identifier, 'DEF')
        self.assertEquals(vt.exec_env_id, 'ENV1')
        self.assertEquals(vt.name, 'A Name')
        # Make sure ValueError is raised if the viztrail directory exists
        with self.assertRaises(ValueError):
            vt = FSViztrailHandle.create_viztrail(
                identifier='XYZ',
                properties={PROPERTY_NAME: 'My Viztrail'},
                exec_env_id='ENV1',
                base_dir=os.path.join(os.path.abspath(REPO_DIR), 'ABC')
            )
        # Delete viztrail
        vt.delete_viztrail()
        self.assertFalse(os.path.exists(base_dir))
        self.assertFalse(os.path.exists(FILE_PROPERTIES(base_dir)))
        self.assertFalse(os.path.exists(FILE_VIZTRAIL(base_dir)))
        self.assertFalse(os.path.exists(DIR_BRANCHES(base_dir)))
        self.assertFalse(os.path.exists(DIR_MODULES(base_dir)))
        self.assertFalse(os.path.exists(DIR_WORKFLOWS(base_dir)))


if __name__ == '__main__':
    unittest.main()
