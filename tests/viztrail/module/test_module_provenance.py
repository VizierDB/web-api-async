"""Test functionality of the module provenance handler. The provenance handler
is used to determine whether a module requires execution based on provenence
information from previous module executions.
"""
import unittest

from vizier.datastore.dataset import DatasetDescriptor
from vizier.viztrail.module.provenance import ModuleProvenance


class TestModuleProvenance(unittest.TestCase):

    def test_adjust_state(self):
        """Test adjusting state for modules that do not require execution."""
        # Current database state
        datasets = {
            'A': DatasetDescriptor(identifier='123'),
            'B': DatasetDescriptor(identifier='345'),
            'C': DatasetDescriptor(identifier='567')
        }
        # Read 'A', write 'B', delete 'C' and create new dataset 'D'
        prov = ModuleProvenance(
            read={'A':'123', 'B': '345'},
            write={
                'B': DatasetDescriptor(identifier='666'),
                'D': DatasetDescriptor(identifier='999')
            },
            delete=['C']
        )
        self.assertFalse(prov.requires_exec(datasets))
        state = prov.get_database_state(prev_state=datasets)
        # The resulting start should contain 'A'->123, 'B'->666, and 'D'->999
        self.assertEquals(len(state), 3)
        for name in ['A', 'B', 'D']:
            self.assertTrue(name in state)
        self.assertEquals(state['A'].identifier, '123')
        self.assertEquals(state['B'].identifier, '666')
        self.assertEquals(state['D'].identifier, '999')

    def test_requires_exec(self):
        """Test .requires_exec() method for the module provenance object."""
        # Current database state
        datasets = {
            'A': DatasetDescriptor(identifier='123'),
            'B': DatasetDescriptor(identifier='345'),
            'C': DatasetDescriptor(identifier='567')
        }
        # For an empty read or write set the .requires_exec() method should
        # always return True
        self.assertTrue(ModuleProvenance().requires_exec(datasets))
        self.assertTrue(ModuleProvenance(read={'A':'123'}).requires_exec(datasets))
        self.assertTrue(ModuleProvenance(write={'A':DatasetDescriptor(identifier='789')}, delete=['A']).requires_exec(datasets))
        # If the module modifies a dataset that it doesn't read but that does
        # exist the result is True
        prov = ModuleProvenance(read={'A':'123'}, write={'C':DatasetDescriptor(identifier='567')}, delete=['A'])
        self.assertTrue(prov.requires_exec(datasets))
        # If the input data has changed the module needs to execute
        prov = ModuleProvenance(read={'A':'abc'}, write={'A':DatasetDescriptor(identifier='123')})
        self.assertTrue(prov.requires_exec(datasets))
        # No execution needed if all input data is present and in the expected
        # state
        prov = ModuleProvenance(read={'A':'123'}, write={'A':DatasetDescriptor(identifier='abc')}, delete=['A'])
        self.assertFalse(prov.requires_exec(datasets))
        prov = ModuleProvenance(read={'B':'345', 'C':'567'}, write={'B':DatasetDescriptor(identifier='abc')})
        self.assertFalse(prov.requires_exec(datasets))
        prov = ModuleProvenance(read={'B':'345', 'C':'567'}, write={})
        self.assertFalse(prov.requires_exec(datasets))
        # Re-execute if a dataset is being deleted that does not exist
        prov = ModuleProvenance(read={'B':'345', 'C':'567'}, write={'B': DatasetDescriptor(identifier='345')})
        self.assertFalse(prov.requires_exec(datasets))
        prov = ModuleProvenance(read={'B':'345', 'C':'567'}, write={'B': DatasetDescriptor(identifier='345')}, delete=['D'])
        self.assertTrue(prov.requires_exec(datasets))


if __name__ == '__main__':
    unittest.main()
