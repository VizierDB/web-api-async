"""Test viztrail repository implementation that uses the file system for
storage.
"""

import os
import shutil
import unittest

from vizier.config import TestEnv
from vizier.workflow.base import DEFAULT_BRANCH
from vizier.workflow.module import ModuleSpecification
from vizier.workflow.command import PACKAGE_PYTHON, PYTHON_CODE, PYTHON_SOURCE, python_cell
from vizier.workflow.command import PACKAGE_VIZUAL, VIZUAL_LOAD, load_dataset, PARA_FILE, PARA_NAME
from vizier.workflow.repository.fs import FileSystemViztrailRepository

VIZTRAILS_DIRECTORY = './env/vt'

ENV = TestEnv()


class TestFileSystemViztrailRepository(unittest.TestCase):

    def setUp(self):
        """Create an empty work trails repository."""
        # Clear VisTrails directory
        if os.path.isdir(VIZTRAILS_DIRECTORY):
            shutil.rmtree(VIZTRAILS_DIRECTORY)
        # Setup project repository
        self.db = FileSystemViztrailRepository(
            VIZTRAILS_DIRECTORY,
            {ENV.identifier: ENV}
        )

    def tearDown(self):
        """Clean-up by dropping viztrails directory.
        """
        shutil.rmtree(VIZTRAILS_DIRECTORY)

    def test_append_module(self):
        """Test appending modules."""
        # Create new viztrail.
        vt = self.db.create_viztrail(ENV.identifier, {'name' : 'My Project'})
        self.db.append_workflow_module(viztrail_id=vt.identifier, command=python_cell('abc'))
        self.db.append_workflow_module(viztrail_id=vt.identifier, command=load_dataset('file', 'name'))
        # The default branch should have two versions. The first versions contains
        # one module and the second version contains two modules
        self.assertEquals(len(vt.branches[DEFAULT_BRANCH].workflows), 2)
        v1 = self.db.get_workflow(viztrail_id=vt.identifier, workflow_version=vt.branches[DEFAULT_BRANCH].workflows[0].version)
        v2 = self.db.get_workflow(viztrail_id=vt.identifier, workflow_version=vt.branches[DEFAULT_BRANCH].workflows[1].version)
        head = self.db.get_workflow(viztrail_id=vt.identifier, branch_id=DEFAULT_BRANCH)
        self.assertEquals(len(v1.modules), 1)
        self.assertEquals(len(v2.modules), 2)
        self.assertEquals(len(head.modules), 2)
        # Ensure that all modules have non-negative identifier
        for m in head.modules:
            self.assertTrue(m.identifier >= 0)
        self.assertEquals(head.modules[0].command.module_type, PACKAGE_PYTHON)
        self.assertEquals(head.modules[1].command.module_type, PACKAGE_VIZUAL)
        self.assertEquals(head.version, 1)
        # Re-load the viztrails to ensure that all information has been persisted properly
        self.db = FileSystemViztrailRepository(
            VIZTRAILS_DIRECTORY,
            {ENV.identifier: ENV}
        )
        vt = self.db.get_viztrail(vt.identifier)
        self.assertEquals(len(vt.branches[DEFAULT_BRANCH].workflows), 2)
        v1 = self.db.get_workflow(viztrail_id=vt.identifier, workflow_version=vt.branches[DEFAULT_BRANCH].workflows[0].version)
        v2 = self.db.get_workflow(viztrail_id=vt.identifier, workflow_version=vt.branches[DEFAULT_BRANCH].workflows[1].version)
        head = self.db.get_workflow(viztrail_id=vt.identifier, branch_id=DEFAULT_BRANCH)
        self.assertEquals(len(v1.modules), 1)
        self.assertEquals(len(v2.modules), 2)
        self.assertEquals(len(head.modules), 2)
        # Ensure that all modules have non-negative identifier
        for m in head.modules:
            self.assertTrue(m.identifier >= 0)
        self.assertEquals(head.modules[0].command.module_type, PACKAGE_PYTHON)
        self.assertEquals(head.modules[1].command.module_type, PACKAGE_VIZUAL)
        self.assertEquals(head.version, 1)
        # Append a third moduel to the head of the default branch
        self.db.append_workflow_module(viztrail_id=vt.identifier, command=python_cell('def'))
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertEquals(len(wf.modules), 3)
        for m in wf.modules:
            self.assertTrue(m.identifier >= 0)
            self.assertEquals(m.stdout[0]['data'], 'SUCCESS ' + str(m.identifier))
        self.assertEquals(wf.modules[0].command.module_type, PACKAGE_PYTHON)
        self.assertEquals(wf.modules[1].command.module_type, PACKAGE_VIZUAL)
        self.assertEquals(wf.modules[2].command.module_type, PACKAGE_PYTHON)
        self.assertEquals(wf.version, 2)
        # Append a module to the first version in the branch. The resulting new
        # branch HEAD is expected to contain only two modules then.
        self.db.append_workflow_module(viztrail_id=vt.identifier, workflow_version=0, command=python_cell('def'))
        self.db = FileSystemViztrailRepository(
            VIZTRAILS_DIRECTORY,
            {ENV.identifier: ENV}
        )
        vt = self.db.get_viztrail(vt.identifier)
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertEquals(len(wf.modules), 2)
        for m in wf.modules:
            self.assertTrue(m.identifier >= 0)
            self.assertEquals(m.stdout[0]['data'], 'SUCCESS ' + str(m.identifier))
        self.assertEquals(wf.modules[0].command.module_type, PACKAGE_PYTHON)
        self.assertEquals(wf.modules[1].command.module_type, PACKAGE_PYTHON)
        self.assertEquals(wf.version, 3)

    def test_branching(self):
        """Test functionality to execute a workflow module."""
        # Create new viztrail and ensure that it contains exactly one branch
        vt = self.db.create_viztrail(ENV.identifier, {'name' : 'My Project'})
        self.assertEquals(len(vt.branches), 1)
        self.assertTrue(DEFAULT_BRANCH in vt.branches)
        self.assertEquals(vt.branches[DEFAULT_BRANCH].identifier, DEFAULT_BRANCH)
        # Append two modules to the defaukt branch
        self.db.append_workflow_module(viztrail_id=vt.identifier, command=python_cell('abc'))
        self.db.append_workflow_module(viztrail_id=vt.identifier, command=load_dataset('file', 'name'))
        # Create a branch at the end of the default branch. The new branch
        # contains one workflow with two modules the version number is 2
        newbranch = self.db.create_branch(viztrail_id=vt.identifier, properties={'name': 'New Branch'})
        self.assertEquals(len(newbranch.workflows), 1)
        self.assertEquals(newbranch.workflows[-1].version, 2)
        wf = vt.get_workflow(branch_id=newbranch.identifier)
        self.assertEquals(wf.version, 2)
        self.assertEquals(len(wf.modules), 2)
        self.assertTrue(newbranch.identifier in vt.branches)
        # Ensure that everything has been persisted properly
        self.db = FileSystemViztrailRepository(
            VIZTRAILS_DIRECTORY,
            {ENV.identifier: ENV}
        )
        vt = self.db.get_viztrail(vt.identifier)
        newbranch = vt.branches[newbranch.identifier]
        self.assertEquals(len(newbranch.workflows), 1)
        self.assertEquals(newbranch.workflows[-1].version, 2)
        wf = vt.get_workflow(branch_id=newbranch.identifier)
        self.assertEquals(wf.version, 2)
        self.assertEquals(len(wf.modules), 2)
        self.assertTrue(newbranch.identifier in vt.branches)
        self.assertEquals(newbranch.properties.get_properties()['name'], 'New Branch')
        # Create a third branch from the start of the master branch
        thirdbranch = self.db.create_branch(viztrail_id=vt.identifier, properties={'name': 'Next Branch'}, module_id=0)
        wf = vt.get_workflow(branch_id=thirdbranch.identifier)
        self.assertEquals(wf.version, 3)
        self.assertEquals(len(wf.modules), 1)
        # Append modules at end of master and at beginning of thirdbranch
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=python_cell('abc')
        )
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            branch_id=thirdbranch.identifier,
            command=python_cell('def'),
            before_id=0
        )
        master_head = vt.get_workflow()
        self.assertEquals(len(master_head.modules), 3)
        self.assertEquals(master_head.modules[0].command.module_type, PACKAGE_PYTHON)
        self.assertEquals(master_head.modules[1].command.module_type, PACKAGE_VIZUAL)
        self.assertEquals(master_head.modules[2].command.module_type, PACKAGE_PYTHON)
        b2_head = vt.get_workflow(branch_id=newbranch.identifier)
        self.assertEquals(len(b2_head.modules), 2)
        self.assertEquals(b2_head.modules[0].command.module_type, PACKAGE_PYTHON)
        self.assertEquals(b2_head.modules[1].command.module_type, PACKAGE_VIZUAL)
        b3_head = vt.get_workflow(branch_id=thirdbranch.identifier)
        self.assertEquals(len(b3_head.modules), 2)
        self.assertEquals(b3_head.modules[0].command.module_type, PACKAGE_PYTHON)
        self.assertEquals(b3_head.modules[1].command.module_type, PACKAGE_PYTHON)
        # Replace second module of third branch
        self.db.replace_workflow_module(
            viztrail_id=vt.identifier,
            branch_id=thirdbranch.identifier,
            module_id=b3_head.modules[1].identifier,
            command=load_dataset('file', 'name')
        )
        b3_head = vt.get_workflow(branch_id=thirdbranch.identifier)
        self.assertEquals(len(b3_head.modules), 2)
        self.assertEquals(b3_head.modules[0].command.module_type, PACKAGE_PYTHON)
        self.assertEquals(b3_head.modules[1].command.module_type, PACKAGE_VIZUAL)
        master_head = vt.get_workflow()
        self.assertEquals(len(master_head.modules), 3)
        self.assertEquals(master_head.modules[0].command.module_type, PACKAGE_PYTHON)
        self.assertEquals(master_head.modules[1].command.module_type, PACKAGE_VIZUAL)
        self.assertEquals(master_head.modules[2].command.module_type, PACKAGE_PYTHON)
        b2_head = vt.get_workflow(branch_id=newbranch.identifier)
        self.assertEquals(len(b2_head.modules), 2)
        self.assertEquals(b2_head.modules[0].command.module_type, PACKAGE_PYTHON)
        self.assertEquals(b2_head.modules[1].command.module_type, PACKAGE_VIZUAL)
        # Ensure there are exceptions raised when branching of an unknown branch
        # or module
        with self.assertRaises(ValueError):
            self.db.create_branch(viztrail_id=vt.identifier, source_branch='unknonw-branch', properties={'name': 'New Branch'})
        with self.assertRaises(ValueError):
            self.db.create_branch(viztrail_id=vt.identifier, properties={'name': 'New Branch'}, module_id=100)
        with self.assertRaises(ValueError):
            self.db.create_branch(viztrail_id=vt.identifier)
        # Test branch provenance
        self.assertEquals(newbranch.provenance.source_branch, DEFAULT_BRANCH)
        self.assertEquals(newbranch.provenance.workflow_version, 1)
        self.assertEquals(newbranch.provenance.module_id, 1)
        self.assertEquals(thirdbranch.provenance.source_branch, DEFAULT_BRANCH)
        self.assertEquals(thirdbranch.provenance.workflow_version, 1)
        self.assertEquals(thirdbranch.provenance.module_id, 0)

    def test_eval_command(self):
        """Test functionality to execute a workflow module."""
        # Create new work trail, append a module and retrieve the resulting
        # workflow from default branch HEAD.
        vt = self.db.create_viztrail(ENV.identifier, {'name' : 'My Project'})
        self.db.append_workflow_module(viztrail_id=vt.identifier, command=python_cell('abc'))
        wf = vt.get_workflow()
        self.assertEquals(wf.version, 0)
        self.assertEquals(len(wf.modules), 1)
        self.db.append_workflow_module(viztrail_id=vt.identifier, command=python_cell('def'))
        wf = vt.get_workflow(branch_id=DEFAULT_BRANCH)
        self.assertEquals(wf.version, 1)
        self.assertEquals(len(wf.modules), 2)
        self.assertEquals(len(wf.modules[0].stdout), 1)
        self.assertEquals(wf.modules[0].command.module_type, PACKAGE_PYTHON)
        self.assertEquals(wf.modules[0].command.command_identifier, PYTHON_CODE)
        self.assertEquals(wf.modules[0].command.arguments[PYTHON_SOURCE], 'abc')
        self.assertEquals(len(wf.modules[1].stdout), 1)
        self.assertEquals(wf.modules[1].command.module_type, PACKAGE_PYTHON)
        self.assertEquals(wf.modules[1].command.command_identifier, PYTHON_CODE)
        self.assertEquals(wf.modules[1].command.arguments[PYTHON_SOURCE], 'def')
        self.db.replace_workflow_module(viztrail_id=vt.identifier, module_id=0, command=load_dataset('file', 'ds'))
        wf = vt.get_workflow()
        self.assertEquals(wf.version, 2)
        self.assertEquals(len(wf.modules), 2)
        self.assertEquals(len(wf.modules[0].stdout), 1)
        self.assertEquals(wf.modules[0].command.module_type, PACKAGE_VIZUAL)
        self.assertEquals(wf.modules[0].command.command_identifier, VIZUAL_LOAD)
        self.assertEquals(wf.modules[0].command.arguments[PARA_FILE]['fileid'], 'file')
        self.assertEquals(wf.modules[0].command.arguments[PARA_NAME], 'ds')
        self.assertEquals(len(wf.modules[1].stdout), 2)
        self.assertEquals(wf.modules[1].command.module_type, PACKAGE_PYTHON)
        self.assertEquals(wf.modules[1].command.command_identifier, PYTHON_CODE)
        self.assertEquals(wf.modules[1].command.arguments[PYTHON_SOURCE], 'def')

    def test_workflow_life_cycle(self):
        """Test functionality to execute a workflow module."""
        # Create new work trail.
        vt = self.db.create_viztrail(ENV.identifier, {'name' : 'My Project'})
        # Append two modules
        self.db.append_workflow_module(viztrail_id=vt.identifier, command=python_cell('abc'))
        self.db.append_workflow_module(viztrail_id=vt.identifier, command=load_dataset('file', 'name'))
        # Create a branch at the end of the default branch
        newbranch = self.db.create_branch(viztrail_id=vt.identifier, properties={'name': 'New Branch'})
        # Append modules at end ofnew branch
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            branch_id=newbranch.identifier,
            command=python_cell('xyz')
        )
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            branch_id=newbranch.identifier,
            command=load_dataset('file', 'myname'),
            before_id=0
        )
        # Ensure that all version files exist
        self.check_files(vt.identifier, vt.branches[DEFAULT_BRANCH].workflows, True)
        new_versions = vt.branches[newbranch.identifier].workflows
        self.check_files(vt.identifier, new_versions, True)
        # Delete new branch. Ensure that only the master versions exist
        self.assertTrue(self.db.delete_branch(viztrail_id=vt.identifier, branch_id=newbranch.identifier))
        self.check_files(vt.identifier, vt.branches[DEFAULT_BRANCH].workflows, True)
        self.check_files(vt.identifier, new_versions, False)
        # Deleting a non-existing branch should return False
        self.assertFalse(self.db.delete_branch(viztrail_id=vt.identifier, branch_id=newbranch.identifier))
        self.assertFalse(self.db.delete_branch(viztrail_id=vt.identifier, branch_id='unknown'))
        # Deleting master branch should raise exception
        with self.assertRaises(ValueError):
            self.db.delete_branch(viztrail_id=vt.identifier, branch_id=DEFAULT_BRANCH)

    def test_viztrail_life_cycle(self):
        """Test API methods to create and delete work trails."""
        # Create work trail and ensure that deleting it returns True
        vt = self.db.create_viztrail(ENV.identifier, {'name' : 'My Project'})
        # Ensure that the viztrail has property name = 'My Project'
        self.assertEquals(vt.properties.get_properties()['name'], 'My Project')
        self.assertEquals(len(self.db.list_viztrails()), 1)
        self.assertTrue(self.db.delete_viztrail(vt.identifier))
        self.assertEquals(len(self.db.list_viztrails()), 0)
        # Multiple deletes should return False
        self.assertFalse(self.db.delete_viztrail(vt.identifier))
        # Deleting an unknown work trail should return False
        self.assertFalse(self.db.delete_viztrail('invalid id'))
        self.assertFalse(self.db.delete_viztrail('f0f0f0f0f0f0f0f0f0f0f0f0'))
        # Cannot create viztrail for unknown engine
        with self.assertRaises(ValueError):
            self.db.create_viztrail('UNKNOWN', {'name' : 'My Project'})

    def check_files(self, viztrail_id, versions, check_exists):
        for wf_desc in versions:
            filename = os.path.join(VIZTRAILS_DIRECTORY, viztrail_id, str(wf_desc.version) + '.yaml')
            self.assertEquals(os.path.isfile(filename), check_exists)

if __name__ == '__main__':
    unittest.main()
