"""Test validation and external representation for commands in the Scala
package.
"""

import unittest

from vizier.engine.packages.scala.command import scala_cell

import vizier.engine.packages.base as pckg
import vizier.engine.packages.scala.base as scala
import vizier.viztrail.command as md


PACKAGE = pckg.PackageIndex(scala.SCALA_COMMANDS)


class TestValidateScala(unittest.TestCase):

    def test_scala_cell(self):
        """Test validation of Scala cell."""
        cmd = scala_cell(
            source='println("Hello, world!")',
            validate=True
        ).to_external_form(
            command=PACKAGE.get(scala.SCALA_CODE)
        )
        self.assertEqual(cmd, 'println("Hello, world!")')


if __name__ == '__main__':
    unittest.main()
