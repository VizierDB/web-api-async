"""Test validation and external representation for commands in the PyCell
package.
"""

import unittest

from vizier.client.command.pycell import python_cell

import vizier.packages.base as pckg
import vizier.packages.pycell.base as pycell
import vizier.viztrail.command as md


PACKAGE = pckg.PackageIndex(pycell.PYTHON_COMMANDS)


class TestValidatePyCell(unittest.TestCase):

    def test_python_cell(self):
        """Test validation of Pyhton cell."""
        cmd = python_cell(
            source='print 2+2',
            validate=True
        ).to_external_form(
            command=PACKAGE.get(pycell.PYTHON_CODE)
        )
        self.assertEquals(cmd, 'print 2+2')


if __name__ == '__main__':
    unittest.main()
