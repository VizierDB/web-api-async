"""Test functionality of the file system viztrail repository."""

import os
import shutil
import unittest


from vizier.client.command.pycell import python_cell
from vizier.core.timestamp import get_current_time
from vizier.viztrail.driver.objectstore.module import OSModuleHandle
from vizier.viztrail.driver.objectstore.viztrail import OSViztrailHandle
from vizier.viztrail.module import ModuleProvenance, ModuleTimestamp, ModuleOutputs
from vizier.viztrail.module import TextOutput
from vizier.viztrail.module import MODULE_RUNNING, MODULE_SUCCESS
from vizier.viztrail.workflow import ACTION_INSERT


REPO_DIR = './.temp'


class TestOSWorkflow(unittest.TestCase):

    def setUp(self):
        """Create an empty repository directory."""
        if os.path.isdir(REPO_DIR):
            shutil.rmtree(REPO_DIR)
        os.makedirs(REPO_DIR)

    def tearDown(self):
        """Delete repository directory.
        """
        shutil.rmtree(REPO_DIR)

    def test_load_active(self):
        """Test loading workflows with active modules."""
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='ABC',
            properties=None,
            base_path=base_path
        )
        branch = vt.get_default_branch()
        # Append ten modules
        for i in range(5):
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
        # This is a hack to similate loading workflows with active modules
        # Change state of last two modules in branch head to an active state
        m = branch.get_head().modules[-2]
        m.state = MODULE_RUNNING
        m.write_module()
        m = branch.get_head().modules[-1]
        m.state = MODULE_RUNNING
        m.write_module()
        vt = OSViztrailHandle.load_viztrail(base_path)
        branch = vt.get_default_branch()
        self.assertTrue(branch.get_head().modules[0].is_success)
        self.assertTrue(branch.get_head().modules[1].is_success)
        self.assertTrue(branch.get_head().modules[2].is_success)
        self.assertTrue(branch.get_head().modules[3].is_canceled)
        self.assertTrue(branch.get_head().modules[4].is_canceled)
        # Change state of last module in second workflow to an active state
        m = branch.get_head().modules[1]
        m.state = MODULE_RUNNING
        m.write_module()
        vt = OSViztrailHandle.load_viztrail(base_path)
        branch = vt.get_default_branch()
        wf = branch.get_workflow(branch.get_history()[1].identifier)
        self.assertTrue(wf.modules[0].is_success)
        self.assertTrue(wf.modules[1].is_canceled)


if __name__ == '__main__':
    unittest.main()
