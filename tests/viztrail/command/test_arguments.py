"""Test getting values from command arguments."""

import unittest

from vizier.client.command.plot import create_plot

import vizier.engine.packages.base as pckg
import vizier.engine.packages.plot.base as plot


class TestCommandArguments(unittest.TestCase):

    def test_plot(self):
        """Test getting argument values using create plot command."""
        cmd = create_plot(
            dataset_name='ds',
            chart_name='My Chart',
            series=[{'column': 1, 'range': '0:10', 'label': 'A'}, {'column': 2}],
            xaxis_range='0:100',
            xaxis_column=1,
            validate=True
        )
        self.assertEquals(cmd.arguments.get_value(pckg.PARA_DATASET), 'ds')
        self.assertEquals(cmd.arguments.get_value(pckg.PARA_NAME), 'My Chart')
        self.assertEquals(cmd.arguments.get_value(pckg.PARA_NAME, as_int=True), 'My Chart')
        self.assertEquals(cmd.arguments.get_value(plot.PARA_XAXIS).get_value(plot.PARA_XAXIS_COLUMN, as_int=True), 1)
        self.assertEquals(cmd.arguments.get_value(plot.PARA_XAXIS_COLUMN, as_int=True, default_value='ABC'), 'ABC')
        self.assertNotEqual(cmd.arguments.get_value(plot.PARA_XAXIS_COLUMN, as_int=True, default_value='100'), 100)
        with self.assertRaises(ValueError):
            cmd.arguments.get_value(plot.PARA_XAXIS_COLUMN)


if __name__ == '__main__':
    unittest.main()