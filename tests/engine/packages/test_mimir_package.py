"""Test validation of commands in the mimir_domain package. Here we only test that
the command instances which are created by the helper methods in the client
module are valid. Further functionality of module validation is tested in
other modules.
"""

import unittest

import vizier.engine.packages.mimir.command as mimir


class TestMimirCommandValidation(unittest.TestCase):

    def test_mimir_geocode(self):
        """Test validation of the mimir geocode command."""
        # Test minimal options
        mimir.mimir_geocode(
            dataset_name='ABC',
            geocoder='OMG',
            materialize_input=False,
            validate=True
        )
        # Test maximal options
        mimir.mimir_geocode(
            dataset_name='ABC',
            geocoder='OMG',
            house_nr=1,
            street=2,
            city=3,
            state=4,
            materialize_input=False,
            validate=True
        )

    def test_mimir_key_repair(self):
        """Test validation of the mimir key repair command."""
        mimir.mimir_key_repair(
            dataset_name='ABC',
            column=1,
            materialize_input=False,
            validate=True
        )

    def test_mimir_missing_key(self):
        """Test validation of the mimir missing key command."""
        mimir.mimir_missing_key(
            dataset_name='ABC',
            column=1,
            materialize_input=False,
            validate=True
        )

    def test_mimir_missing_value(self):
        """Test validation of the mimir missing value command."""
        # Test without constraint
        mimir.mimir_missing_value(
            dataset_name='ABC',
            columns=[{'column': 1}],
            materialize_input=False,
            validate=True
        )
        # Test with constraint
        mimir.mimir_missing_value(
            dataset_name='ABC',
            columns=[
                {'column': 1},
                {'column': 2, 'constraint': '>40'}
            ],
            materialize_input=False,
            validate=True
        )

    def test_mimir_picker(self):
        """Test validation of the mimir picker command."""
        schema = [
            {'pickFrom': 0, 'pickAs': 'X'},
            {'pickFrom': 1},
            {'pickFrom': 2, 'pickAs': 'Y'}
        ]
        # Test without pick as option
        mimir.mimir_picker(
            dataset_name='ABC',
            schema=schema,
            materialize_input=False,
            validate=True
        )
        # Test with pick as option
        mimir.mimir_picker(
            dataset_name='ABC',
            schema=schema,
            pick_as='MyCol',
            materialize_input=False,
            validate=True
        )

    def test_mimir_type_inference(self):
        """Test validation of the mimir type inference command."""
        mimir.mimir_type_inference(
            dataset_name='ABC',
            percent_conform=0.5,
            materialize_input=False,
            validate=True
        )


if __name__ == '__main__':
    unittest.main()
