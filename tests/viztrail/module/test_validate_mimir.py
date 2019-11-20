"""Test validation and external representation for commands in the Mimir
package.
"""

import unittest

from vizier.engine.packages.mimir.command import mimir_domain, mimir_geocode
from vizier.engine.packages.mimir.command import mimir_key_repair, mimir_missing_key
from vizier.engine.packages.mimir.command import mimir_missing_value, mimir_picker
from vizier.engine.packages.mimir.command import mimir_schema_matching
from vizier.datastore.dataset import DatasetColumn, DatasetDescriptor

import vizier.engine.packages.base as pckg
import vizier.engine.packages.mimir.base as mimir
import vizier.viztrail.command as md


DATASETS = {
    'ds': DatasetDescriptor(
        identifier='0000',
        columns=[
            DatasetColumn(identifier=2, name='Some Name'),
            DatasetColumn(identifier=1, name='Street')
        ]
    )
}
PACKAGE = pckg.PackageIndex(mimir.MIMIR_LENSES)


class TestValidateMimir(unittest.TestCase):

    def test_mimir_domain(self):
        """Test validation of Mimir domain lens."""
        cmd = mimir_domain(
            dataset_name='ds',
            column=1,
            materialize_input=False,
            validate=True
        ).to_external_form(
            command=PACKAGE.get(mimir.MIMIR_DOMAIN),
            datasets=DATASETS
        )
        self.assertEqual(cmd, 'DOMAIN FOR Street IN ds')
        with self.assertRaises(ValueError):
            md.ModuleCommand(
                mimir.PACKAGE_MIMIR,
                mimir.MIMIR_DOMAIN,
                arguments =[
                    md.ARG(id=pckg.PARA_COLUMN, value=1),
                    md.ARG(id=mimir.PARA_MATERIALIZE_INPUT, value=False)
                ],
                packages={mimir.PACKAGE_MIMIR: PACKAGE}
            )
        with self.assertRaises(ValueError):
            md.ModuleCommand(
                mimir.PACKAGE_MIMIR,
                mimir.MIMIR_DOMAIN,
                arguments =[
            md.ARG(id=pckg.PARA_DATASET, value='DS'),
                    md.ARG(id=mimir.PARA_MATERIALIZE_INPUT, value=False)
                ],
                packages={mimir.PACKAGE_MIMIR: PACKAGE}
            )

    def test_mimir_geocode(self):
        """Test validation of Mimir geocode lens."""
        cmd = mimir_geocode(
            dataset_name='ds',
            geocoder='GOOGLE',
            street=1,
            city=2,
            materialize_input=False,
            validate=True
        ).to_external_form(
            command=PACKAGE.get(mimir.MIMIR_GEOCODE),
            datasets=DATASETS
        )
        self.assertEqual(cmd, 'GEOCODE ds COLUMNS STREET=Street CITY=\'Some Name\' USING GOOGLE')

    def test_mimir_key_repair(self):
        """Test validation of Mimir key repair lens."""
        cmd = mimir_key_repair(
            dataset_name='ds',
            column=2,
            materialize_input=False,
            validate=True
        ).to_external_form(
            command=PACKAGE.get(mimir.MIMIR_KEY_REPAIR),
            datasets=DATASETS
        )
        self.assertEqual(cmd, 'KEY REPAIR FOR \'Some Name\' IN ds')
        with self.assertRaises(ValueError):
            mimir_key_repair(
                dataset_name='ds',
                column=2.34,
                materialize_input=False,
                validate=True
            )

    def test_mimir_missing_key(self):
        """Test validation of Mimir missing key lens."""
        cmd = mimir_missing_key(
            dataset_name='ds',
            column=1,
            materialize_input=False,
            validate=True
        ).to_external_form(
            command=PACKAGE.get(mimir.MIMIR_MISSING_KEY),
            datasets=DATASETS
        )
        self.assertEqual(cmd, 'MISSING KEYS FOR Street IN ds')

    def test_mimir_missing_value(self):
        """Test validation of Mimir missing value lens."""
        cmd = mimir_missing_value(
            dataset_name='ds',
            columns=[{'column': 1}],
            materialize_input=False,
            validate=True
        ).to_external_form(
            command=PACKAGE.get(mimir.MIMIR_MISSING_VALUE),
            datasets=DATASETS
        )
        self.assertEqual(cmd, 'MISSING VALUES FOR Street IN ds')
        cmd = mimir_missing_value(
            dataset_name='ds',
            columns=[
                {'column': 1, 'constraint': '> 40'},
                {'column': 2}
            ],
            materialize_input=False,
            validate=True
        ).to_external_form(
            command=PACKAGE.get(mimir.MIMIR_MISSING_VALUE),
            datasets=DATASETS
        )
        self.assertEqual(cmd, 'MISSING VALUES FOR Street WITH CONSTRAINT \'> 40\', \'Some Name\' IN ds')

    def test_mimir_picker(self):
        """Test validation of Mimir picker lens."""
        cmd = mimir_picker(
            dataset_name='ds',
            schema=[{'pickFrom': 1,'pickAs': 'The Street'}, {'pickFrom': 2}],
            materialize_input=False,
            validate=True
        ).to_external_form(
            command=PACKAGE.get(mimir.MIMIR_PICKER),
            datasets=DATASETS
        )
        self.assertEqual(cmd, 'PICK FROM Street, \'Some Name\' IN ds')
        cmd = mimir_picker(
            dataset_name='ds',
            schema=[{'pickFrom': 1,'pickAs': 'The Street'}, {'pickFrom': 2}],
            pick_as='My Street',
            materialize_input=False,
            validate=True
        ).to_external_form(
            command=PACKAGE.get(mimir.MIMIR_PICKER),
            datasets=DATASETS
        )
        self.assertEqual(cmd, 'PICK FROM Street, \'Some Name\' AS \'My Street\' IN ds')

    def test_mimir_schema_matching(self):
        """Test validation of Mimir schema matching lens."""
        cmd = mimir_schema_matching(
            dataset_name='ds',
            schema=[{'column': 'COL_A', 'type': 'int'}, {'column': 'COL_2', 'type': 'string'}],
            result_name='My DS',
            materialize_input=False,
            validate=True
        ).to_external_form(
            command=PACKAGE.get(mimir.MIMIR_SCHEMA_MATCHING),
            datasets=DATASETS
        )
        self.assertEqual(cmd, 'SCHEMA MATCHING ds (COL_A int, COL_2 string) AS \'My DS\'')


if __name__ == '__main__':
    unittest.main()
