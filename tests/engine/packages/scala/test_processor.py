"""Test scala processor."""

import unittest

from vizier.engine.packages.scala.command import scala_cell
from vizier.engine.packages.scala.processor import ScalaTaskProcessor
from vizier.engine.task.base import TaskContext

import vizier.mimir as mimir # noqa: F401


class TestScalaProcessor(unittest.TestCase):

    def test_run_simple_script(self):
        """Test running a very simple script to ensure that the basic
        functionality is there.
        """
        cmd = scala_cell(
            source='print("Hello, world!")',
            validate=True
        )
        result = ScalaTaskProcessor().compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                project_id=4,
                datastore=None,
                filestore=None,
                artifacts={}
            )
        )
        self.assertTrue(result.is_success)
        self.assertTrue(result.provenance.read is None)
        self.assertTrue(result.provenance.write is None)
        self.assertEqual(result.outputs.stdout[0].value, 'Hello, world!')


if __name__ == '__main__':
    unittest.main()
