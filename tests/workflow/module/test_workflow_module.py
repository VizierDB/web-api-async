import time
import unittest

from vizier.core.timestamp import get_current_time
from vizier.workflow.module import ModuleHandle, ModuleProvenance, TextObject
from vizier.workflow.module import OUTPUT_TEXT


class TestWorkflowModule(unittest.TestCase):

    def test_workflows(self):
        """Test copying a module and updating the module state. Ensure that
        modifying a module copy does not affect other (previous) modules."""
        # Create original module
        module = ModuleHandle(identifier=0, command=dict(), external_form="MY TEXT")
        self.assertTrue(module.is_pending)
        self.assertFalse(module.is_running)
        self.assertFalse(module.is_canceled)
        self.assertFalse(module.is_error)
        self.assertFalse(module.is_success)
        # RUN
        module.set_running(started_at=get_current_time())
        self.assertFalse(module.is_pending)
        self.assertTrue(module.is_running)
        self.assertFalse(module.is_canceled)
        self.assertFalse(module.is_error)
        self.assertFalse(module.is_success)
        self.assertIsNotNone(module.timestamp.started_at)
        self.assertIsNone(module.timestamp.finished_at)
        module.outputs.stdout(content=TextObject('First run'))
        # SUCCESS
        module.set_success(
            datasets=dict({'A': '123', 'B': '345'}),
            prov=ModuleProvenance(read={'A': '000'}, write={'A': '123'}),
            finished_at=get_current_time()
        )
        self.assertEquals(len(module.outputs.stdout()), 1)
        self.assertEquals(len(module.outputs.stderr()), 0)
        self.assertEquals(module.outputs.stdout()[0].type, OUTPUT_TEXT)
        self.assertTrue(module.outputs.stdout()[0].is_text)
        self.assertFalse(module.is_pending)
        self.assertFalse(module.is_running)
        self.assertFalse(module.is_canceled)
        self.assertFalse(module.is_error)
        self.assertTrue(module.is_success)
        self.assertIsNotNone(module.timestamp.started_at)
        self.assertIsNotNone(module.timestamp.finished_at)
        # COPY
        m1 = module.copy_pending()
        self.assertTrue(m1.is_pending)
        self.assertTrue(module.is_success)
        # Delay for 1 second
        time.sleep(1)
        m1.set_running(started_at=get_current_time())
        self.assertTrue(m1.is_running)
        self.assertTrue(module.is_success)
        self.assertNotEqual(module.timestamp.started_at, m1.timestamp.started_at)
        m1.outputs.stdout(content=TextObject('Second run'))
        m1.outputs.stderr(content=TextObject('Error'))
        m1.set_error(finished_at=get_current_time())
        self.assertTrue(m1.is_error)
        self.assertIsNotNone(m1.timestamp.started_at)
        self.assertIsNotNone(m1.timestamp.finished_at)
        self.assertNotEqual(module.timestamp.finished_at, m1.timestamp.finished_at)
        self.assertEquals(len(module.outputs.stderr()), 0)
        self.assertEquals(len(m1.outputs.stderr()), 1)
        self.assertEquals(module.outputs.stdout()[0].value, 'First run')
        self.assertEquals(m1.outputs.stdout()[0].value, 'Second run')
        # COPY
        m2 = module.copy_pending()
        m2.set_running(started_at=get_current_time())
        m2.set_success(
            datasets=dict({'A': '456', 'B': '345', 'C': '333'}),
            prov=ModuleProvenance(read={'A': '000'}, write={'A': '456'}),
            finished_at=get_current_time()
        )
        self.assertEquals(len(module.datasets), 2)
        self.assertEquals(module.datasets['A'], '123')
        self.assertEquals(len(m2.datasets), 3)
        self.assertEquals(m2.datasets['A'], '456')
        self.assertEquals(module.prov.write['A'], '123')
        self.assertEquals(m2.prov.write['A'], '456')


if __name__ == '__main__':
    unittest.main()
