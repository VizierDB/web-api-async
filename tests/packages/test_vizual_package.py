"""Test validation of commands in the vizual package. Here we only test that
the command instances which are created by the helper methods in the client
module are valid. Further functionality of module validation is tested in
other modules.
"""

import unittest

import vizier.client.command.vizual as vizual
import vizier.engine.packages.base as pckg


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

    def test_insert_column(self):
        """Test validation of the insert column command."""
        vizual.insert_column(
            dataset_name='ABC',
            position=1,
            name='My Name',
            validate=True
        )

    def test_insert_row(self):
        """Test validation of the insert row command."""
        vizual.insert_row(
            dataset_name='ABC',
            position=1,
            validate=True
        )

    def test_load_dataset(self):
        """Test validation of the load dataset command."""
        vizual.load_dataset(
            dataset_name='ABC',
            file_id={pckg.FILE_ID: '493ewkfj485ufjw490feofj'},
            validate=True
        )

    def test_move_column(self):
        """Test validation of the move column command."""
        vizual.move_column(
            dataset_name='ABC',
            column=1,
            position=0,
            validate=True
        )

    def test_move_row(self):
        """Test validation of the move row command."""
        vizual.move_row(
            dataset_name='ABC',
            row=1,
            position=0,
            validate=True
        )

    def test_projection(self):
        """Test validation of the projection command."""
        vizual.projection(
            dataset_name='ABC',
            columns=[
                {'column': 1},
                {'column': 2, 'name': 'MyName'},
                {'column': 3}
            ],
            validate=True
        )

    def test_rename_column(self):
        """Test validation of the rename column command."""
        vizual.rename_column(
            dataset_name='ABC',
            column=1,
            name='MyCol',
            validate=True
        )

    def test_rename_dataset(self):
        """Test validation of the rename dataset command."""
        vizual.rename_dataset(
            dataset_name='ABC',
            new_name='XYZ',
            validate=True
        )

    def test_sort_dataset(self):
        """Test validation of the sort dataset command."""
        vizual.sort_dataset(
            dataset_name='ABC',
            columns=[
                {'column': 1, 'order': 'Z-A'},
                {'column': 2, 'order': 'A-Z'},
                {'column': 3, 'order': '???'}
            ],
            validate=True
        )

    def test_update_cell(self):
        """Test validation of the update cell command."""
        # Test with integer, float, and string value
        vizual.update_cell(
            dataset_name='ABC',
            column=1,
            row=1,
            value=1,
            validate=True
        )
        vizual.update_cell(
            dataset_name='ABC',
            column=1,
            row=1,
            value=1.568686,
            validate=True
        )
        vizual.update_cell(
            dataset_name='ABC',
            column=1,
            row=1,
            value='X',
            validate=True
        )


if __name__ == '__main__':
    unittest.main()
