"""Test functionality of the branch provenance class."""

import unittest

from vizier.viztrail.branch import BranchProvenance


class TestBranchProvenance(unittest.TestCase):

    def test_init(self):
        """Ensure that invalid argument combinations for the provenance object
        are detected properly.
        """
        prov = BranchProvenance()
        prov = BranchProvenance(source_branch='A', workflow_id='B', module_id='C')
        # If either one but not all arguments are None an exception is thrown
        with self.assertRaises(ValueError):
            BranchProvenance(source_branch='A', workflow_id='B')
        with self.assertRaises(ValueError):
            BranchProvenance(source_branch='A')
        with self.assertRaises(ValueError):
            BranchProvenance(source_branch='A', module_id='C')
        with self.assertRaises(ValueError):
            BranchProvenance(module_id='C')
        with self.assertRaises(ValueError):
            BranchProvenance(workflow_id='B', module_id='C')
        with self.assertRaises(ValueError):
            BranchProvenance(workflow_id='B')


if __name__ == '__main__':
    unittest.main()
