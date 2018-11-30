"""Test functionality of the file system viztrail repository."""

import os
import shutil
import unittest


from vizier.client.command.pycell import python_cell
from vizier.core.timestamp import get_current_time
from vizier.viztrail.driver.objectstore.viztrail import OSViztrailHandle
from vizier.viztrail.module import ModuleProvenance, ModuleTimestamp, ModuleOutputs
from vizier.viztrail.module import TextOutput
from vizier.viztrail.module import MODULE_SUCCESS


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
        wf = branch.append_exec_result(
            command=python_cell(source='print 2+2'),
            external_form='print 2+2',
            state=MODULE_SUCCESS,
            datasets=dict(),
            outputs=ModuleOutputs(stdout=[TextOutput('4')]),
            provenance=ModuleProvenance(),
            timestamp=ModuleTimestamp(
                created_at=get_current_time(),
                started_at=get_current_time(),
                finished_at=get_current_time()
            )
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
            branch.append_exec_result(
                command=python_cell(source='print ' + str(i) + '+' + str(i)),
                external_form='print ' + str(i) + '+' + str(i),
                state=MODULE_SUCCESS,
                datasets=dict(),
                outputs=ModuleOutputs(stdout=[TextOutput(str(i + i))]),
                provenance=ModuleProvenance(),
                timestamp=ModuleTimestamp(
                    created_at=ts,
                    started_at=ts,
                    finished_at=ts
                )
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
            self.assertEquals(wf.descriptor.created_at, module.timestamp.finished_at)


if __name__ == '__main__':
    unittest.main()
