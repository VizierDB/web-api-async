"""Test functionality of the file system viztrail repository."""

import os
import shutil
import unittest


from vizier.client.command.pycell import python_cell
from vizier.viztrail.driver.objectstore.module import OSModuleHandle
from vizier.viztrail.driver.objectstore.viztrail import OSViztrailHandle
from vizier.viztrail.base import PROPERTY_NAME
from vizier.viztrail.module import MODULE_SUCCESS

import vizier.viztrail.driver.objectstore.branch as br
import vizier.viztrail.driver.objectstore.viztrail as viztrail


REPO_DIR = './.temp'


class TestOSViztrail(unittest.TestCase):

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
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='ABC',
            properties=None,
            base_path=base_path
        )
        self.assertEquals(len(vt.branches), 1)
        branch = vt.create_branch(properties={PROPERTY_NAME: 'My Branch'})
        self.assertEquals(len(vt.branches), 2)
        self.assertIsNone(branch.head)
        self.assertEquals(len(branch.workflows), 0)
        vt = OSViztrailHandle.load_viztrail(base_path)
        self.assertEquals(len(vt.branches), 2)
        self.assertTrue(branch.identifier in vt.branches)
        self.assertEquals(vt.get_branch(branch.identifier).name, 'My Branch')
        branch = vt.get_branch(branch.identifier)
        self.assertIsNone(branch.head)
        self.assertEquals(len(branch.workflows), 0)
        # Ensure that all branch files exist
        branch_path = os.path.join(base_path, viztrail.FOLDER_BRANCHES, branch.identifier)
        self.assertTrue(os.path.isdir(branch_path))
        self.assertTrue(os.path.isfile(os.path.join(branch_path, br.OBJ_METADATA)))
        self.assertTrue(os.path.isfile(os.path.join(branch_path, br.OBJ_PROPERTIES)))
        vt.delete_branch(branch.identifier)
        self.assertFalse(os.path.isdir(branch_path))
        self.assertEquals(len(vt.branches), 1)
        vt = OSViztrailHandle.load_viztrail(base_path)
        self.assertEquals(len(vt.branches), 1)

    def test_create_and_delete_branch_with_default_workflow(self):
        """Ensure that creating and loading branches works if the head workflow
        for the new branch is given.
        """
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='DEF',
            properties={PROPERTY_NAME: 'My Viztrail'},
            base_path=base_path
        )
        self.assertEquals(vt.last_modified_at, vt.default_branch.last_modified_at)
        # Create five modules
        modules = list()
        for i in range(5):
            identifier = 'MOD' + str(i)
            modules.append(identifier)
            OSModuleHandle(
                identifier=identifier,
                command=python_cell(source='print ' + str(i)),
                external_form='TEST MODULE ' + str(i),
                state=MODULE_SUCCESS,
                module_path=vt.object_store.join(vt.modules_folder, identifier)
            ).write_module()
        branch = vt.create_branch(
            properties={PROPERTY_NAME: 'My Branch'},
            modules=modules
        )
        self.assertIsNotNone(branch.head)
        self.assertEquals(len(branch.workflows), 1)
        vt = OSViztrailHandle.load_viztrail(base_path)
        branch = vt.get_branch(branch.identifier)
        self.assertIsNotNone(branch.head)
        self.assertEquals(len(branch.workflows), 1)
        wf = branch.get_workflow(branch.head.identifier)
        self.assertEquals(len(wf.modules), 5)
        for i in range(5):
            self.assertEquals(wf.modules[i].identifier, 'MOD' + str(i))
            self.assertEquals(wf.modules[i].external_form, 'TEST MODULE ' + str(i))
        self.assertEquals(vt.last_modified_at, branch.last_modified_at)
        self.assertEquals(vt.last_modified_at, branch.last_modified_at)

    def test_create_branch_of_active_workflow(self):
        """Ensure thatan exception is raised when attempting to branch of a
        workflow with active modules. None of the branch resources should be
        created.
        """
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='DEF',
            properties={PROPERTY_NAME: 'My Viztrail'},
            base_path=base_path
        )
        # Create one branch
        branch = vt.create_branch(properties={PROPERTY_NAME: 'My Branch'})
        branch_path = os.path.join(base_path, viztrail.FOLDER_BRANCHES, branch.identifier)
        self.assertTrue(os.path.isdir(branch_path))
        files = os.listdir(os.path.join(base_path, viztrail.FOLDER_BRANCHES))
        # Create five modules. The last one is active
        modules = list()
        for i in range(5):
            identifier = 'MOD' + str(i)
            modules.append(identifier)
            m = OSModuleHandle(
                identifier=identifier,
                command=python_cell(source='print ' + str(i)),
                external_form='TEST MODULE ' + str(i),
                state=MODULE_SUCCESS,
                module_path=vt.object_store.join(vt.modules_folder, identifier)
            ).write_module()
        m.set_running()
        with self.assertRaises(ValueError):
            vt.create_branch(
                properties={PROPERTY_NAME: 'My Branch'},
                modules=modules
            )
        # Ensure that no additional entry in the branches folder is created
        self.assertEquals(len(files), len(os.listdir(os.path.join(base_path, viztrail.FOLDER_BRANCHES))))

    def test_create_empty_properties(self):
        """Ensure that create without pre-defined properties works."""
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='ABC',
            properties=None,
            base_path=base_path
        )
        self.assertEquals(vt.identifier, 'ABC')
        self.assertIsNone(vt.name)

    def test_create_load_delete(self):
        """Ensure that create and load works properly."""
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='DEF',
            properties={PROPERTY_NAME: 'My Viztrail'},
            base_path=base_path
        )
        # Ensure that all files and subfolders are created
        vt_folder = os.path.join(REPO_DIR, 'ABC')
        self.assertTrue(os.path.isdir(vt_folder))
        self.assertTrue(os.path.isdir(os.path.join(vt_folder, viztrail.FOLDER_BRANCHES)))
        self.assertTrue(os.path.isdir(os.path.join(vt_folder, viztrail.FOLDER_MODULES)))
        self.assertTrue(os.path.isfile(os.path.join(vt_folder, viztrail.FOLDER_BRANCHES, viztrail.OBJ_BRANCHINDEX)))
        self.assertTrue(os.path.isfile(os.path.join(vt_folder, viztrail.OBJ_METADATA)))
        self.assertTrue(os.path.isfile(os.path.join(vt_folder, viztrail.OBJ_PROPERTIES)))
        # Update name property
        self.assertEquals(vt.identifier, 'DEF')
        self.assertEquals(vt.name, 'My Viztrail')
        vt.name = 'A Name'
        self.assertEquals(vt.name, 'A Name')
        # Load viztrail from disk
        vt = OSViztrailHandle.load_viztrail(base_path)
        self.assertEquals(vt.identifier, 'DEF')
        self.assertEquals(vt.name, 'A Name')
        # Delete viztrail
        vt.delete_viztrail()
        self.assertFalse(os.path.exists(vt_folder))

    def test_default_branch(self):
        """Test behaviour of the viztrail default branch."""
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='ABC',
            properties=None,
            base_path=base_path
        )
        self.assertEquals(len(vt.branches), 1)
        branch = vt.get_default_branch()
        # Attempt to delete the branch that is the default should raise
        # ValueError
        with self.assertRaises(ValueError):
            vt.delete_branch(branch.identifier)
        # Attempt to delete the default branch folder should raise runtime error
        self.assertTrue(branch.is_default)
        with self.assertRaises(RuntimeError):
            branch.delete_branch()
        # Reload viztrail to ensure that default branch information is persisted
        vt = OSViztrailHandle.load_viztrail(base_path)
        self.assertEquals(len(vt.branches), 1)
        branch = vt.get_default_branch()
        # Attempt to delete the branch that is the default should raise
        # ValueError
        with self.assertRaises(ValueError):
            vt.delete_branch(branch.identifier)
        # Attempt to delete the default branch folder should raise runtime error
        self.assertTrue(branch.is_default)
        with self.assertRaises(RuntimeError):
            branch.delete_branch()
        # Add a new branch
        second_branch = vt.create_branch(properties={PROPERTY_NAME: 'My Branch'})
        self.assertFalse(second_branch.is_default)
        self.assertNotEqual(vt.get_default_branch().identifier, second_branch.identifier)
        vt = OSViztrailHandle.load_viztrail(base_path)
        self.assertNotEqual(vt.get_default_branch().identifier, second_branch.identifier)
        # Set second branch as default branch
        second_branch = vt.set_default_branch(second_branch.identifier)
        self.assertTrue(second_branch.is_default)
        self.assertFalse(vt.get_branch(branch.identifier).is_default)
        self.assertEquals(vt.get_default_branch().identifier, second_branch.identifier)
        # It should be possible to delete the first branch now
        self.assertTrue(vt.delete_branch(branch.identifier))
        vt = OSViztrailHandle.load_viztrail(base_path)
        self.assertIsNone(vt.get_branch(branch.identifier))
        self.assertEquals(vt.get_default_branch().identifier, second_branch.identifier)
        # Set default branch to unknown branch should raise ValueError
        with self.assertRaises(ValueError):
            vt.set_default_branch(branch.identifier)


if __name__ == '__main__':
    unittest.main()
