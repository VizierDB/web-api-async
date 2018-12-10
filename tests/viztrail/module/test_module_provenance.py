"""Test functionality of the module provenance handler. The provenance handler
is used to determine whether a module requires execution based on provenence
information from previous module executions.
"""
import unittest

from vizier.datastore.dataset import DatasetDescriptor
from vizier.viztrail.module.provenance import ModuleProvenance


class TestModuleProvenance(unittest.TestCase):

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
        self.assertTrue(ModuleProvenance(write={'A':'789'}).requires_exec(datasets))
        # If the module modifies a dataset that it doesn't read but that does
        # exist the result is True
        prov = ModuleProvenance(read={'A':'123'}, write={'C':'567'})
        self.assertTrue(prov.requires_exec(datasets))
        # If the input data has changed the module needs to execute
        prov = ModuleProvenance(read={'A':'abc'}, write={'A':'123'})
        self.assertTrue(prov.requires_exec(datasets))
        # No execution needed if all input data is present and in the expected
        # state
        prov = ModuleProvenance(read={'A':'123'}, write={'A':'abc'})
        self.assertFalse(prov.requires_exec(datasets))
        prov = ModuleProvenance(read={'B':'345', 'C':'567'}, write={'B':'abc'})
        self.assertFalse(prov.requires_exec(datasets))
        prov = ModuleProvenance(read={'B':'345', 'C':'567'}, write={})
        self.assertFalse(prov.requires_exec(datasets))


if __name__ == '__main__':
    unittest.main()
