"""Test validation of commands in the vizual package. Here we only test that
the command instances which are created by the helper methods in the client
module are valid. Further functionality of module validation is tested in
other modules.
"""

import unittest

import vizier.client.command.vizual as vizual


class TestVizualCommandValidation(unittest.TestCase):

    def test_delete_column(self):
        """Test validation of the delete column command."""
        vizual.delete_column(
            dataset_name='ABC',
            column=1,
            validate=True
        )

    def test_delete_row(self):
        """Test validation of the delete row command."""
        vizual.delete_row(
            dataset_name='ABC',
            row=10,
            validate=True
        )

    def test_drop_dataset(self):
        """Test validation of the drop dataset command."""
        vizual.drop_dataset(
            dataset_name='ABC',
            validate=True
        )


if __name__ == '__main__':
    unittest.main()
