"""Test functionality of the file system viztrail repository."""

import os
import shutil
import unittest


from vizier.engine.packages.pycell.command import python_cell
from vizier.viztrail.objectstore.module import OSModuleHandle
from vizier.viztrail.objectstore.viztrail import OSViztrailHandle
from vizier.viztrail.named_object import PROPERTY_NAME
from vizier.viztrail.module.base import MODULE_SUCCESS
from vizier.viztrail.module.output import ModuleOutputs
from vizier.viztrail.module.provenance import ModuleProvenance
from vizier.viztrail.module.timestamp import ModuleTimestamp

import vizier.viztrail.objectstore.branch as br
import vizier.viztrail.objectstore.viztrail as viztrail


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
            properties={},
            base_path=base_path
        )
        self.assertEqual(len(vt.branches), 1)
        branch = vt.create_branch(properties={PROPERTY_NAME: 'My Branch'})
        self.assertEqual(len(vt.branches), 2)
        self.assertIsNone(branch.head)
        self.assertEqual(len(branch.workflows), 0)
        vt = OSViztrailHandle.load_viztrail(base_path)
        self.assertEqual(len(vt.branches), 2)
        self.assertTrue(branch.identifier in vt.branches)
        self.assertEqual(vt.get_branch(branch.identifier).name, 'My Branch')
        branch = vt.get_branch(branch.identifier)
        self.assertIsNone(branch.head)
        self.assertEqual(len(branch.workflows), 0)
        # Ensure that all branch files exist
        branch_path = os.path.join(base_path, viztrail.FOLDER_BRANCHES, branch.identifier)
        self.assertTrue(os.path.isdir(branch_path))
        self.assertTrue(os.path.isfile(os.path.join(branch_path, br.OBJ_METADATA)))
        self.assertTrue(os.path.isfile(os.path.join(branch_path, br.OBJ_PROPERTIES)))
        vt.delete_branch(branch.identifier)
        self.assertFalse(os.path.isdir(branch_path))
        self.assertEqual(len(vt.branches), 1)
        vt = OSViztrailHandle.load_viztrail(base_path)
        self.assertEqual(len(vt.branches), 1)

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
        self.assertEqual(vt.last_modified_at, vt.default_branch.last_modified_at)
        # Create five modules
        modules = list()
        for i in range(5):
            identifier = OSModuleHandle.create_module(
                command=python_cell(source='print ' + str(i)),
                external_form='TEST MODULE ' + str(i),
                state=MODULE_SUCCESS,
                outputs=ModuleOutputs(),
                provenance=ModuleProvenance(),
                timestamp=ModuleTimestamp(),
                module_folder=vt.modules_folder,
            ).identifier
            modules.append(identifier)
        branch = vt.create_branch(
            properties={PROPERTY_NAME: 'My Branch'},
            modules=modules
        )
        self.assertIsNotNone(branch.head)
        self.assertEqual(len(branch.workflows), 1)
        vt = OSViztrailHandle.load_viztrail(base_path)
        branch = vt.get_branch(branch.identifier)
        self.assertIsNotNone(branch.head)
        self.assertEqual(len(branch.workflows), 1)
        wf = branch.get_workflow(branch.head.identifier)
        self.assertEqual(len(wf.modules), 5)
        for i in range(5):
            self.assertEqual(wf.modules[i].external_form, 'TEST MODULE ' + str(i))
        self.assertEqual(vt.last_modified_at, branch.last_modified_at)
        self.assertEqual(vt.last_modified_at, branch.last_modified_at)

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
            m = OSModuleHandle.create_module(
                command=python_cell(source='print ' + str(i)),
                external_form='TEST MODULE ' + str(i),
                state=MODULE_SUCCESS,
                outputs=ModuleOutputs(),
                provenance=ModuleProvenance(),
                timestamp=ModuleTimestamp(),
                module_folder=vt.modules_folder,
            )
            modules.append(m.identifier)
        m.set_running(external_form='TEST MODULE')
        with self.assertRaises(ValueError):
            vt.create_branch(
                properties={PROPERTY_NAME: 'My Branch'},
                modules=modules
            )
        # Ensure that no additional entry in the branches folder is created
        self.assertEqual(len(files), len(os.listdir(os.path.join(base_path, viztrail.FOLDER_BRANCHES))))

    def test_create_empty_properties(self):
        """Ensure that create without pre-defined properties works."""
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='ABC',
            properties={},
            base_path=base_path
        )
        self.assertEqual(vt.identifier, 'ABC')

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
        self.assertEqual(vt.identifier, 'DEF')
        self.assertEqual(vt.name, 'My Viztrail')
        vt.name = 'A Name'
        self.assertEqual(vt.name, 'A Name')
        # Load viztrail from disk
        vt = OSViztrailHandle.load_viztrail(base_path)
        self.assertEqual(vt.identifier, 'DEF')
        self.assertEqual(vt.name, 'A Name')
        # Delete viztrail
        vt.delete_viztrail()
        self.assertFalse(os.path.exists(vt_folder))

    def test_default_branch(self):
        """Test behaviour of the viztrail default branch."""
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='ABC',
            properties={},
            base_path=base_path
        )
        self.assertEqual(len(vt.branches), 1)
        branch = vt.get_default_branch()
        self.assertTrue(vt.is_default_branch(branch.identifier))
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
        self.assertEqual(len(vt.branches), 1)
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
        self.assertFalse(vt.is_default_branch(second_branch.identifier))
        vt = OSViztrailHandle.load_viztrail(base_path)
        self.assertNotEqual(vt.get_default_branch().identifier, second_branch.identifier)
        # Set second branch as default branch
        second_branch = vt.set_default_branch(second_branch.identifier)
        self.assertTrue(second_branch.is_default)
        self.assertFalse(vt.get_branch(branch.identifier).is_default)
        self.assertEqual(vt.get_default_branch().identifier, second_branch.identifier)
        # It should be possible to delete the first branch now
        self.assertTrue(vt.delete_branch(branch.identifier))
        vt = OSViztrailHandle.load_viztrail(base_path)
        self.assertIsNone(vt.get_branch(branch.identifier))
        self.assertEqual(vt.get_default_branch().identifier, second_branch.identifier)
        # Set default branch to unknown branch should raise ValueError
        with self.assertRaises(ValueError):
            vt.set_default_branch(branch.identifier)


if __name__ == '__main__':
    unittest.main()
