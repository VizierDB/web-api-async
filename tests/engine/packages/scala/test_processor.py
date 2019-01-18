"""Test scala processor."""

import unittest

from vizier.engine.packages.scala.command import scala_cell
from vizier.engine.packages.scala.processor import ScalaTaskProcessor
from vizier.engine.task.base import TaskContext

import vizier.mimir as mimir


class TestScalaProcessor(unittest.TestCase):

    def test_run_simple_script(self):
        """Test running a very simple script to ensure that the basic
        functionality is there.
        """
        mimir.initialize()
        cmd = scala_cell(
            source='println("Hello, world!")',
            validate=True
        )
        result = ScalaTaskProcessor().compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                datastore=None,
                filestore=None
            )
        )
        self.assertTrue(result.is_success)
        self.assertIsNone(result.provenance.read)
        self.assertIsNone(result.provenance.write)
        self.assertEquals(result.outputs.stdout[0].value, 'Hello, world!')
        mimir.finalize()


if __name__ == '__main__':
    unittest.main()
