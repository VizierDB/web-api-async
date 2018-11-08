"""Test functionality of the workflow context and dataset mappings."""

import unittest

from vizier.config import TestEnv
from vizier.workflow.context import WorkflowContext
import vizier.workflow.context as ctx

VZRENV_DATASETS_MODULEID = 'moduleId'
VZRENV_DATASETS_MAPPING = 'mapping'

def DATASET_MAPPINGS():
    return list([
        {
            ctx.VZRENV_DATASETS_MODULEID: 0,
            ctx.VZRENV_DATASETS_MAPPING: {'A': 'DS0'}
        },
        {
            ctx.VZRENV_DATASETS_MODULEID: 5,
            ctx.VZRENV_DATASETS_MAPPING: {'A': 'DS0', 'B': 'DS1'}
        },
        {
            ctx.VZRENV_DATASETS_MODULEID: 3,
            ctx.VZRENV_DATASETS_MAPPING: {'A': 'DS0', 'B': 'DS1'}
        },
        {
            ctx.VZRENV_DATASETS_MODULEID: 4,
            ctx.VZRENV_DATASETS_MAPPING: {'A': 'DS0', 'B': 'DS2'}
        },
        {
            ctx.VZRENV_DATASETS_MODULEID: 2,
            ctx.VZRENV_DATASETS_MAPPING: {'B': 'DS2'}
        },
    ])


class TestDataset(unittest.TestCase):

    def test_create_context(self):
        """Test creating a workflow context with different configurations."""
        context = WorkflowContext(TestEnv())
        self.validate_keys(context, [ctx.VZRENV_ENV, ctx.VZRENV_DATASETS, ctx.VZRENV_VARS, ctx.VZRENV_TYPE])
        self.assertEquals(context[ctx.VZRENV_TYPE], ctx.CONTEXT_DEFAULT)
        self.assertEquals(len(context[ctx.VZRENV_ENV]), 3)
        self.validate_keys(
            context[ctx.VZRENV_ENV],
            [ctx.VZRENV_ENV_DATASTORE, ctx.VZRENV_ENV_FILESERVER, ctx.VZRENV_ENV_IDENTIFIER]
        )
        self.assertEquals(len(context[ctx.VZRENV_DATASETS]), 0)
        self.assertEquals(len(context[ctx.VZRENV_VARS]), 0)
        context = WorkflowContext(
            TestEnv(),
            context_type=ctx.CONTEXT_VOLATILE,
            datasets= DATASET_MAPPINGS(),
            variables={'a': 1}
        )
        self.validate_keys(context, [ctx.VZRENV_ENV, ctx.VZRENV_DATASETS, ctx.VZRENV_VARS, ctx.VZRENV_TYPE])
        self.assertEquals(context[ctx.VZRENV_TYPE], ctx.CONTEXT_VOLATILE)
        self.assertEquals(len(context[ctx.VZRENV_DATASETS]), 5)
        self.assertEquals(len(context[ctx.VZRENV_VARS]), 1)
        self.assertEquals(context[ctx.VZRENV_VARS]['a'], 1)
        with self.assertRaises(ValueError):
            context = WorkflowContext(TestEnv(), context_type='UNKNOWN')

    def validate_keys(self, obj, keys):
        self.assertEquals(len(obj), len(keys))
        for key in keys:
            self.assertTrue(key in obj)


if __name__ == '__main__':
    unittest.main()
