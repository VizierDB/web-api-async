"""Test validation of commands in the scala package."""

import unittest

from vizier.engine.packages.scala.command import scala_cell
from vizier.viztrail.command import ModuleCommand, ARG, ARG_ID
import vizier.engine.packages.base as pckg
import vizier.engine.packages.scala.base as scala


PACKAGES = {scala.PACKAGE_SCALA: pckg.PackageIndex(scala.SCALA_COMMANDS)}


class TestScalaCommandValidation(unittest.TestCase):

    def test_scala_cell(self):
        """Test validation of the scala command."""
        scala_cell(
            source='ABC',
            validate=True
        )
        # Have an error raised if values of invalid data type are given
        with self.assertRaises(ValueError):
            scala_cell(
                source=[],
                validate=True
            )
        # Get dictionary serialization of command arguments. Ensure that we
        # can create a valid command instance from the returned result.
        obj = scala_cell(
            source='ABC',
            validate=True
        ).arguments.to_list()
        ModuleCommand(
            package_id=scala.PACKAGE_SCALA,
            command_id=scala.SCALA_CODE,
            arguments=obj,
            packages=PACKAGES
        )
        # Delete the only mandatory element from the serialization to ensure
        # that validation fails
        del obj[0]
        with self.assertRaises(ValueError):
            ModuleCommand(
                package_id=scala.PACKAGE_SCALA,
                command_id=scala.SCALA_CODE,
                arguments=obj,
                packages=PACKAGES
            )
        # Add an unknown argument to ensure that the validation fails
        obj = scala_cell(
            source='ABC',
            validate=True
        ).arguments.to_list()
        obj.append(ARG(id='someUnknownLabel', value=''))
        with self.assertRaises(ValueError):
            ModuleCommand(
                package_id=scala.PACKAGE_SCALA,
                command_id=scala.SCALA_CODE,
                arguments=obj,
                packages=PACKAGES
            )


if __name__ == '__main__':
    unittest.main()
