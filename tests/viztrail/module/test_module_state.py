"""Test properties of the module state object."""
import unittest

from vizier.viztrail.module import ModuleState
import vizier.viztrail.module as state


class TestModuleState(unittest.TestCase):

    def test_init(self):
        """Ensure that a ValueError is raised only if an invalid state value is
        used to initialize the object.
        """
        ModuleState(state.MODULE_CANCELED)
        ModuleState(state.MODULE_ERROR)
        ModuleState(state.MODULE_PENDING)
        ModuleState(state.MODULE_RUNNING)
        ModuleState(state.MODULE_SUCCESS)
        with self.assertRaises(ValueError):
            ModuleState(sum(state.MODULE_STATE))

    def test_unique(self):
        """Only one of the state properties can be true at a time."""
        # CANCELED
        s = ModuleState(state.MODULE_CANCELED)
        self.assertTrue(s.is_canceled)
        self.assertFalse(s.is_error)
        self.assertFalse(s.is_pending)
        self.assertFalse(s.is_running)
        self.assertFalse(s.is_success)
        self.assertTrue(s.is_stopped)
        self.assertFalse(s.is_active)
        # ERROR
        s = ModuleState(state.MODULE_ERROR)
        self.assertFalse(s.is_canceled)
        self.assertTrue(s.is_error)
        self.assertFalse(s.is_pending)
        self.assertFalse(s.is_running)
        self.assertFalse(s.is_success)
        self.assertTrue(s.is_stopped)
        self.assertFalse(s.is_active)
        # PENDING
        s = ModuleState(state.MODULE_PENDING)
        self.assertFalse(s.is_canceled)
        self.assertFalse(s.is_error)
        self.assertTrue(s.is_pending)
        self.assertFalse(s.is_running)
        self.assertFalse(s.is_success)
        self.assertFalse(s.is_stopped)
        self.assertTrue(s.is_active)
        # RUNNING
        s = ModuleState(state.MODULE_RUNNING)
        self.assertFalse(s.is_canceled)
        self.assertFalse(s.is_error)
        self.assertFalse(s.is_pending)
        self.assertTrue(s.is_running)
        self.assertFalse(s.is_success)
        self.assertFalse(s.is_stopped)
        self.assertTrue(s.is_active)
        # SUCCESS
        s = ModuleState(state.MODULE_SUCCESS)
        self.assertFalse(s.is_canceled)
        self.assertFalse(s.is_error)
        self.assertFalse(s.is_pending)
        self.assertFalse(s.is_running)
        self.assertTrue(s.is_success)
        self.assertFalse(s.is_stopped)
        self.assertFalse(s.is_active)

if __name__ == '__main__':
    unittest.main()
