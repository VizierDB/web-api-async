"""Test functionality of the file system viztrail repository."""

import unittest


from vizier.viztrail.driver.objectstore.branch import get_workflow_id


class TestOSBranchWorkflowID(unittest.TestCase):

    def test_workflow_identifier(self):
        """Ensure that the get_workflow_id function returns unique identifier
        in correct order.
        """
        workflows = list()
        for i in range(10000):
            workflow_id = get_workflow_id(i)
            self.assertFalse(workflow_id in workflows)
            if len(workflows) > 0:
                self.assertTrue(workflows[-1] < workflow_id)
            workflows.append(workflow_id)


if __name__ == '__main__':
    unittest.main()
