"""Test validation and external representation for commands in the SQL
package.
"""

import unittest

from vizier.engine.packages.sql.command import sql_cell

import vizier.engine.packages.base as pckg
import vizier.engine.packages.sql.base as sql
import vizier.viztrail.command as md


PACKAGE = pckg.PackageIndex(sql.SQL_COMMANDS)


class TestValidateSQL(unittest.TestCase):

    def test_sql_cell(self):
        """Test validation of the SQL cell."""
        cmd = sql_cell(
            source='SELECT * FROM dataset',
            validate=True
        ).to_external_form(
            command=PACKAGE.get(sql.SQL_QUERY)
        )
        self.assertEquals(cmd, 'SELECT * FROM dataset')
        # Validate with given output dataset
        cmd = sql_cell(
            source='SELECT * FROM dataset',
            output_dataset='result',
            validate=True
        ).to_external_form(
            command=PACKAGE.get(sql.SQL_QUERY)
        )
        self.assertEquals(cmd, 'SELECT * FROM dataset AS result')


if __name__ == '__main__':
    unittest.main()
