"""Test validation of commands in the python cell package."""

import unittest

from vizier.client.command.pycell import python_cell
from vizier.viztrail.module.command import ModuleCommand, ARG, ARG_ID
import vizier.engine.packages.base as pckg
import vizier.engine.packages.pycell.base as pycell


PACKAGES = {pycell.PACKAGE_PYTHON: pckg.PackageIndex(pycell.PYTHON_COMMANDS)}


class TestPyCellCommandValidation(unittest.TestCase):

    def test_python_cell(self):
        """Test validation of the python cell command."""
        python_cell(
            source='ABC',
            validate=True
        )
        # Have an error raised if values of invalid data type are given
        with self.assertRaises(ValueError):
            python_cell(
                source=[],
                validate=True
            )
        # Get dictionary serialization of command arguments. Ensure that we
        # can create a valid command instance from the returned result.
        obj = python_cell(
            source='ABC',
            validate=True
        ).arguments.to_list()
        ModuleCommand(
            package_id=pycell.PACKAGE_PYTHON,
            command_id=pycell.PYTHON_CODE,
            arguments=obj,
            packages=PACKAGES
        )
        # Delete the only mandatory element from the serialization to ensure
        # that validation fails
        del obj[0]
        with self.assertRaises(ValueError):
            ModuleCommand(
                package_id=pycell.PACKAGE_PYTHON,
                command_id=pycell.PYTHON_CODE,
                arguments=obj,
                packages=PACKAGES
            )
        # Add an unknown argument to ensure that the validation fails
        obj = python_cell(
            source='ABC',
            validate=True
        ).arguments.to_list()
        obj.append(ARG(id='someUnknownLabel', value=''))
        with self.assertRaises(ValueError):
            ModuleCommand(
                package_id=pycell.PACKAGE_PYTHON,
                command_id=pycell.PYTHON_CODE,
                arguments=obj,
                packages=PACKAGES
            )


if __name__ == '__main__':
    unittest.main()
