"""Test functionality of the file system viztrail repository."""

import os
import shutil
import unittest


from vizier.client.command.pycell import python_cell
from vizier.core.timestamp import get_current_time
from vizier.viztrail.driver.objectstore.branch import DEFAULT_CACHE_SIZE
from vizier.viztrail.driver.objectstore.viztrail import OSViztrailHandle
from vizier.viztrail.module import ModuleProvenance, ModuleTimestamp, ModuleOutputs
from vizier.viztrail.module import TextOutput
from vizier.viztrail.module import MODULE_SUCCESS


REPO_DIR = './.temp'


class TestOSBranchCache(unittest.TestCase):

    def setUp(self):
        """Create an empty repository directory."""
        if os.path.isdir(REPO_DIR):
            shutil.rmtree(REPO_DIR)
        os.makedirs(REPO_DIR)

    def tearDown(self):
        """Delete repository directory.
        """
        shutil.rmtree(REPO_DIR)

    def test_branch_cache(self):
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
        self.assertFalse(wf.identifier in [w.identifier for w in branch.cache])
        for i in range(DEFAULT_CACHE_SIZE):
            branch.append_exec_result(
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
            self.assertEquals(len(branch.cache), (i + 1))
            self.assertTrue(wf.identifier in [w.identifier for w in branch.cache])
        branch.append_exec_result(
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
        self.assertEquals(len(branch.cache), DEFAULT_CACHE_SIZE)
        self.assertFalse(wf.identifier in [w.identifier for w in branch.cache])
        vt = OSViztrailHandle.load_viztrail(base_path)
        branch = vt.get_default_branch()
        self.assertEquals(len(branch.cache), 0)
        self.assertFalse(wf.identifier in [w.identifier for w in branch.cache])
        branch.get_workflow(wf.identifier)
        self.assertTrue(wf.identifier in [w.identifier for w in branch.cache])
        for wf_desc in branch.get_history():
            if wf_desc.identifier != wf.identifier:
                branch.get_workflow(wf_desc.identifier)
        self.assertEquals(len(branch.cache), DEFAULT_CACHE_SIZE)
        self.assertFalse(wf.identifier in [w.identifier for w in branch.cache])


if __name__ == '__main__':
    unittest.main()