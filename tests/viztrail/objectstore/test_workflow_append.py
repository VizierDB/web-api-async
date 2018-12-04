"""Test functionality of the file system viztrail repository."""

import os
import shutil
import unittest


from vizier.client.command.pycell import python_cell
from vizier.core.timestamp import get_current_time
from vizier.engine.packages.pycell.base import PACKAGE_PYTHON, PYTHON_CODE
from vizier.viztrail.driver.objectstore.module import OSModuleHandle
from vizier.viztrail.driver.objectstore.viztrail import OSViztrailHandle
from vizier.viztrail.module import ModuleHandle, ModuleOutputs
from vizier.viztrail.module import ModuleProvenance, ModuleTimestamp
from vizier.viztrail.module import TextOutput
from vizier.viztrail.module import MODULE_SUCCESS
from vizier.viztrail.workflow import ACTION_DELETE, ACTION_INSERT


REPO_DIR = './.temp'


class TestOSWorkflowAppend(unittest.TestCase):

    def setUp(self):
        """Create an empty repository directory."""
        if os.path.isdir(REPO_DIR):
            shutil.rmtree(REPO_DIR)
        os.makedirs(REPO_DIR)

    def tearDown(self):
        """Delete repository directory.
        """
        shutil.rmtree(REPO_DIR)

    def test_completed_append(self):
        """Test appending a completed workflow to a branch."""
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='ABC',
            properties=None,
            base_path=base_path
        )
        branch = vt.get_default_branch()
        for i in range(10):
            ts = get_current_time()
            command = python_cell(source='print ' + str(i) + '+' + str(i))
            module = OSModuleHandle.create_module(
                command=command,
                external_form='print ' + str(i) + '+' + str(i),
                state=MODULE_SUCCESS,
                datasets=dict(),
                outputs=ModuleOutputs(stdout=[TextOutput(str(i + i))]),
                provenance=ModuleProvenance(),
                timestamp=ModuleTimestamp(created_at=ts,started_at=ts,finished_at=ts),
                module_folder=vt.modules_folder,
                object_store=vt.object_store
            )
            if not branch.head is None:
                modules = branch.head.modules + [module]
            else:
                modules = [module]
            branch.append_completed_workflow(
                modules=modules,
                action=ACTION_INSERT,
                command=command
            )
        head_modules = branch.get_head().modules
        wf = branch.append_completed_workflow(
            modules=head_modules[:-1],
            action=ACTION_DELETE,
            command=head_modules[-1].command
        )
        self.assertEquals(len(wf.modules), 9)
        self.assertEquals(wf.descriptor.identifier, '0000000A')
        self.assertEquals(wf.descriptor.action, ACTION_DELETE)
        self.assertEquals(wf.descriptor.package_id, PACKAGE_PYTHON)
        self.assertEquals(wf.descriptor.command_id, PYTHON_CODE)
        vt = OSViztrailHandle.load_viztrail(base_path)
        branch = vt.get_default_branch()
        history = branch.get_history()
        self.assertEquals(len(history), 11)
        wf = branch.get_head()
        self.assertEquals(len(wf.modules), 9)
        self.assertEquals(wf.descriptor.identifier, '0000000A')
        self.assertEquals(wf.descriptor.action, ACTION_DELETE)
        self.assertEquals(wf.descriptor.package_id, PACKAGE_PYTHON)
        self.assertEquals(wf.descriptor.command_id, PYTHON_CODE)
        # Ensure exception is thrown if a module is active
        m = head_modules[-1]
        head_modules.append(
            ModuleHandle(command=m.command, external_form=m.command)
        )
        with self.assertRaises(ValueError):
            branch.append_completed_workflow(
                modules=head_modules,
                action=ACTION_DELETE,
                command=head_modules[-1].command
            )

    def test_multi_append(self):
        """Test appending modules to viztrail branch."""
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='ABC',
            properties=None,
            base_path=base_path
        )
        branch = vt.get_default_branch()
        # Append ten modules
        for i in range(10):
            ts = get_current_time()
            command = python_cell(source='print ' + str(i) + '+' + str(i))
            module = OSModuleHandle.create_module(
                command=command,
                external_form='print ' + str(i) + '+' + str(i),
                state=MODULE_SUCCESS,
                datasets=dict(),
                outputs=ModuleOutputs(stdout=[TextOutput(str(i + i))]),
                provenance=ModuleProvenance(),
                timestamp=ModuleTimestamp(created_at=ts,started_at=ts,finished_at=ts),
                module_folder=vt.modules_folder,
                object_store=vt.object_store
            )
            if not branch.head is None:
                modules = branch.head.modules + [module]
            else:
                modules = [module]
            branch.append_completed_workflow(
                modules=modules,
                action=ACTION_INSERT,
                command=command
            )
            self.assertEquals(len(branch.get_history()), (i + 1))
        vt = OSViztrailHandle.load_viztrail(base_path)
        branch = vt.get_default_branch()
        history = branch.get_history()
        self.assertEquals(len(history), 10)
        for i in range(10):
            wf = branch.get_workflow(history[i].identifier)
            self.assertEquals(len(wf.modules), (i + 1))
            for m in range(i + 1):
                module = wf.modules[m]
                self.assertEquals(module.external_form, 'print ' + str(m) + '+' + str(m))
                self.assertEquals(module.outputs.stdout[-1].value, str(m+m))

    def test_pending_append(self):
        """Test appending a workflow with pending modules to a branch."""
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='ABC',
            properties=None,
            base_path=base_path
        )
        branch = vt.get_default_branch()
        for i in range(10):
            ts = get_current_time()
            command = python_cell(source='print ' + str(i) + '+' + str(i))
            module = OSModuleHandle.create_module(
                command=command,
                external_form='print ' + str(i) + '+' + str(i),
                state=MODULE_SUCCESS,
                datasets=dict(),
                outputs=ModuleOutputs(stdout=[TextOutput(str(i + i))]),
                provenance=ModuleProvenance(),
                timestamp=ModuleTimestamp(created_at=ts,started_at=ts,finished_at=ts),
                module_folder=vt.modules_folder,
                object_store=vt.object_store
            )
            if not branch.head is None:
                modules = branch.head.modules + [module]
            else:
                modules = [module]
            branch.append_completed_workflow(
                modules=modules,
                action=ACTION_INSERT,
                command=command
            )
        head_modules = branch.get_head().modules
        before_ids = [m.identifier for m in head_modules]
        modules = head_modules[:5]
        pending_modules = [ModuleHandle(command=m.command, external_form=m.external_form) for m in head_modules[5:]]
        wf = branch.append_pending_workflow(
            modules=head_modules[:5],
            pending_modules=pending_modules,
            action=ACTION_DELETE,
            command=head_modules[-1].command
        )
        for m in wf.modules[:5]:
            self.assertTrue(m.identifier in before_ids)
        for m in wf.modules[5:]:
            self.assertFalse(m.identifier in before_ids)
        self.assertEquals(len(wf.modules), 10)
        self.assertEquals(wf.descriptor.identifier, '0000000A')
        self.assertEquals(wf.descriptor.action, ACTION_DELETE)
        self.assertEquals(wf.descriptor.package_id, PACKAGE_PYTHON)
        self.assertEquals(wf.descriptor.command_id, PYTHON_CODE)
        vt = OSViztrailHandle.load_viztrail(base_path)
        branch = vt.get_default_branch()
        history = branch.get_history()
        self.assertEquals(len(history), 11)
        wf = branch.get_head()
        for m in wf.modules[:5]:
            self.assertTrue(m.identifier in before_ids)
        for m in wf.modules[5:]:
            self.assertFalse(m.identifier in before_ids)
        self.assertEquals(len(wf.modules), 10)
        self.assertEquals(wf.descriptor.identifier, '0000000A')
        self.assertEquals(wf.descriptor.action, ACTION_DELETE)
        self.assertEquals(wf.descriptor.package_id, PACKAGE_PYTHON)
        self.assertEquals(wf.descriptor.command_id, PYTHON_CODE)

    def test_single_append(self):
        """Test appending a single module to an empty viztrail branch."""
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='ABC',
            properties=None,
            base_path=base_path
        )
        branch = vt.get_default_branch()
        command = python_cell(source='print 2+2')
        ts = get_current_time()
        module = OSModuleHandle.create_module(
            command=command,
            external_form='print 2+2',
            state=MODULE_SUCCESS,
            datasets=dict(),
            outputs=ModuleOutputs(stdout=[TextOutput('4')]),
            provenance=ModuleProvenance(),
            timestamp=ModuleTimestamp(created_at=ts,started_at=ts,finished_at=ts),
            module_folder=vt.modules_folder,
            object_store=vt.object_store
        )
        wf = branch.append_completed_workflow(
            modules=[module],
            action=ACTION_INSERT,
            command=command
        )
        # We expect that there exists a file for the workflow handle and one for
        # the new module
        self.assertTrue(os.path.isfile(os.path.join(branch.base_path, wf.identifier)))
        self.assertTrue(os.path.isfile(os.path.join(wf.modules[-1].module_path)))
        # Load the viztrail and get the module at the branch head
        vt = OSViztrailHandle.load_viztrail(base_path)
        module = vt.get_default_branch().get_head().modules[-1]
        self.assertEquals(module.external_form, 'print 2+2')
        self.assertEquals(module.outputs.stdout[-1].value, '4')


if __name__ == '__main__':
    unittest.main()
