"""Test get_state method for workflow handles."""
import unittest

from vizier.client.command.pycell import python_cell
from vizier.viztrail.module.base import ModuleHandle
from vizier.viztrail.workflow import WorkflowDescriptor, WorkflowHandle, ACTION_CREATE

import vizier.viztrail.module as state


class TestWorkflowState(unittest.TestCase):

    def test_get_state(self):
        """Test get_state method."""
        modules = list()
        modules.append(
            ModuleHandle(
                identifier='MOD0',
                command=python_cell(source='print 2+2'),
                external_form='TEST MODULE',
                state=state.MODULE_SUCCESS
            )
        )
        modules.append(
            ModuleHandle(
                identifier='MOD1',
                command=python_cell(source='print 2+2'),
                external_form='TEST MODULE',
                state=state.MODULE_SUCCESS
            )
        )
        wf = WorkflowHandle(
            identifier='0',
            branch_id='0',
            modules=modules,
            descriptor=WorkflowDescriptor(identifier='0', action=ACTION_CREATE)
        )
        self.assertTrue(wf.get_state().is_success)
        modules.append(
            ModuleHandle(
                identifier='MOD1',
                command=python_cell(source='print 2+2'),
                external_form='TEST MODULE',
                state=state.MODULE_CANCELED
            )
        )
        modules.append(
            ModuleHandle(
                identifier='MOD1',
                command=python_cell(source='print 2+2'),
                external_form='TEST MODULE',
                state=state.MODULE_SUCCESS
            )
        )
        wf = WorkflowHandle(
            identifier='0',
            branch_id='0',
            modules=modules,
            descriptor=WorkflowDescriptor(identifier='0', action=ACTION_CREATE)
        )
        self.assertTrue(wf.get_state().is_canceled)


if __name__ == '__main__':
    unittest.main()
