"""
"""

import unittest

from vizier.config import AppConfig
from vizier.viztrail.module import ModuleSpecification

import vizier.engine.packages.mimir.base as mimir
import vizier.engine.packages.plot.base as plot
import vizier.engine.packages.pycell.base as pycell
import vizier.engine.packages.vizual.base as vizual
import vizier.engine.command as cmd


class TestValidateCommand(unittest.TestCase):

    def setUp(self):
        """Initialize the command repository."""
        self.packages = AppConfig().packages

    def test_validate_mimir(self):
        """Test validation of Mimir cell command specifications."""
        obj = mimir.mimir_missing_value('ds', 'colA', True)
        cmd.validate_command(self.packages, obj)
        obj.arguments['row'] = 'row'
        with self.assertRaises(ValueError):
            cmd.validate_command(self.packages, obj)
        obj = mimir.mimir_missing_value('ds', 'colA', True)
        del obj.arguments['dataset']
        with self.assertRaises(ValueError):
            cmd.validate_command(self.packages, obj)
        obj = mimir.mimir_missing_value('ds', 'colA', True)
        obj.arguments['row'] = 'row'
        with self.assertRaises(ValueError):
            cmd.validate_command(self.packages, obj)
        # GEOCODE
        obj = mimir.mimir_geocode('ds', 'GOOGLE', house_nr='23', street='5TH AVE', city='NYC', state='NY')
        cmd.validate_command(self.packages, obj)
        obj = mimir.mimir_geocode('ds', 'GOOGLE')
        cmd.validate_command(self.packages, obj)
        # KEY REPAIR
        obj = mimir.mimir_key_repair('ds', 'colA', True)
        cmd.validate_command(self.packages, obj)
        # MISSING KEY
        obj = mimir.mimir_missing_key('ds', 'colA', make_input_certain=True)
        cmd.validate_command(self.packages, obj)
        # PICKER
        obj = mimir.mimir_picker(
            'ds',
            [{'pickFrom': 'A'}, {'pickFrom': 'B'}],
            pick_as='A',
            make_input_certain=True
        )
        cmd.validate_command(self.packages, obj)
        # SCHEMA Matching
        obj = mimir.mimir_schema_matching(
            'ds',
            [{'column': 'colA', 'type':'int'}, {'column':'colB', 'type':'int'}],
            'myds'
        )
        cmd.validate_command(self.packages, obj)
        # TYPE INFERENCE
        obj = mimir.mimir_type_inference('ds', 0.6, make_input_certain=True)
        cmd.validate_command(self.packages, obj)

    def test_validate_nested(self):
        """Validate nested parameter specification."""
        spec = dict()
        spec['dbname'] = cmd.parameter_specification('dbname', '', 'int', 0, label='name')
        spec['tables'] = cmd.parameter_specification('tables', '', 'int', 1)
        spec['tabname'] = cmd.parameter_specification('tabname', '', 'int', 2, label='name', parent='tables')
        spec['schema'] = cmd.parameter_specification('schema', '', 'int', 3, parent='tables')
        spec['column'] = cmd.parameter_specification('column', '', 'int', 4, parent='schema')
        spec['type'] = cmd.parameter_specification('type', '', 'int', 5, parent='schema')
        args = {
            'name': 'My Name',
            'tables': [{
                'name': 'T1',
                'schema': [{
                    'column': 'A',
                    'type': 'int'
                },
                {
                    'column': 'B',
                    'type': 'varchar'
                }]
            },
            {
                'name': 'T1',
                'schema': [{
                    'column': 'A',
                    'type': 'int'
                },
                {
                    'column': 'B',
                    'type': 'varchar'
                }]
            }]
        }
        cmd.validate_arguments(spec, args)
        del spec['type']
        with self.assertRaises(ValueError):
            cmd.validate_arguments(spec, args)

    def test_validate_plot(self):
        """Test validation of plot cell command specifications."""
        series = [
            {'series_column': 'A', 'series_label': 'Fatal', 'series_range':'0:20'},
            {'series_column': 'B'}
        ]
        chart = plot.create_plot('accidents', 'My Chart', series, chart_type='bar', chart_grouped=True)
        cmd.validate_command(self.packages, chart)


    def test_validate_python(self):
        """Test validation of python cell command specifications."""
        cmd.validate_command(self.packages, pycell.python_cell('print 2'))
        with self.assertRaises(ValueError):
            cmd.validate_command(
                self.packages,
                ModuleSpecification(
                    cmd.PACKAGE_PYTHON,
                    cmd.PYTHON_CODE,
                    {'content' : 'abc'}
                )
            )
        obj = pycell.python_cell('print 2')
        obj.arguments['content'] = 'abc'
        with self.assertRaises(ValueError):
            cmd.validate_command(self.packages, obj)

    def test_validate_vizual(self):
        """Test validation ofVizUAL cell command specifications."""
        # DELETE COLUMN
        obj = vizual.delete_column('dataset', 'column')
        cmd.validate_command(self.packages, obj)
        obj.arguments['row'] = 'row'
        with self.assertRaises(ValueError):
            cmd.validate_command(self.packages, obj)
        obj = vizual.delete_column('dataset', 'column')
        del obj.arguments['dataset']
        with self.assertRaises(ValueError):
            cmd.validate_command(self.packages, obj)
        obj = vizual.delete_column('dataset', 'column')
        obj.arguments['row'] = 'row'
        with self.assertRaises(ValueError):
            cmd.validate_command(self.packages, obj)
        # DELETE ROW
        obj = vizual.delete_row('dataset', 'row')
        cmd.validate_command(self.packages, obj)
        # DROP DATASET
        obj = vizual.drop_dataset('dataset')
        cmd.validate_command(self.packages, obj)
        # INSERT COLUMN
        obj = vizual.insert_column('dataset', 1, 'A')
        cmd.validate_command(self.packages, obj)
        # INSERT ROW
        obj = vizual.insert_row('dataset', 1)
        cmd.validate_command(self.packages, obj)
        # LOAD DATASET
        obj = vizual.load_dataset('file', 'dataset', filename='My File')
        cmd.validate_command(self.packages, obj)
        # MOVE COLUMN
        obj = vizual.move_column('dataset', 'A', 2)
        cmd.validate_command(self.packages, obj)
        # MOVE ROW
        obj = vizual.move_row('dataset', 1, 2)
        cmd.validate_command(self.packages, obj)
        # RENAME COLUMN
        obj = vizual.rename_column('dataset', 'A', 'B')
        cmd.validate_command(self.packages, obj)
        # RENAME DATASET
        obj = vizual.rename_dataset('ds1', 'ds2')
        cmd.validate_command(self.packages, obj)
        # UPDATE CELL
        obj = vizual.update_cell('dataset', 'A', 1, 'X')
        cmd.validate_command(self.packages, obj)
        # Unknown VizUAL Command
        obj = {
            'name' : 'unknown',
            'arguments': {
                'dataset': '1',
                'name': '2',
                'position': '3'
            }
        }
        with self.assertRaises(ValueError):
            cmd.validate_command(
                self.packages,
                ModuleSpecification(cmd.PACKAGE_VIZUAL, 'unknown', obj)
            )


if __name__ == '__main__':
    unittest.main()
