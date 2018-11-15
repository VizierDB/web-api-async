"""Test validation of commands in the plot package."""

import unittest

from vizier.client.command.plot import create_plot
from vizier.viztrail.module.command import ModuleCommand, ARG, ARG_ID
import vizier.engine.packages.base as pckg
import vizier.engine.packages.plot.base as plot


PACKAGES = {plot.PACKAGE_PLOT: pckg.PackageIndex(plot.PLOT_COMMANDS)}


class TestPlotCommandValidation(unittest.TestCase):

    def test_create_plot(self):
        """Test validation of the create plot command."""
        create_plot(
            ds_name='ds',
            chart_name='My Chart',
            series=[{'column': 1}],
            validate=True
        )
        create_plot(
            ds_name='ds',
            chart_name='My Chart',
            series=[{'column': 1}],
            xaxis_range='0:10',
            validate=True
        )
        # Have an error raised if values of invalid data type are given
        with self.assertRaises(ValueError):
            create_plot(
                ds_name='ds',
                chart_name='My Chart',
                series=[{'column': 'abc'}],
                xaxis_range='0:10',
                validate=True
            )
        with self.assertRaises(ValueError):
            create_plot(
                ds_name='ds',
                chart_name='My Chart',
                series=[{'column': 1, 'label': [], 'range': '0-10'}],
                xaxis_range='0:10',
                validate=True
            )
        # Get dictionary serialization of command arguments. Ensure that we
        # can create a valid command instance from the returned result.
        obj = create_plot(
            ds_name='ds',
            chart_name='My Chart',
            series=[{'column': 1}],
            xaxis_range='0:10',
            validate=True
        ).arguments.to_list()
        ModuleCommand(
            package_id=plot.PACKAGE_PLOT,
            command_id=plot.PLOT_SIMPLE_CHART,
            arguments=obj,
            packages=PACKAGES
        )
        # Delete a mandatory element from the serialization to ensure that
        # validation fails
        index = -1
        for i in range(len(obj)):
            if obj[i][ARG_ID] == pckg.PARA_NAME:
                index = i
                break
        del obj[i]
        with self.assertRaises(ValueError):
            ModuleCommand(
                package_id=plot.PACKAGE_PLOT,
                command_id=plot.PLOT_SIMPLE_CHART,
                arguments=obj,
                packages=PACKAGES
            )
        # Add an unknown argument to ensure that the validation fails
        obj = create_plot(
            ds_name='ds',
            chart_name='My Chart',
            series=[{'column': 1}],
            xaxis_range='0:10',
            validate=True
        ).arguments.to_list()
        obj.append(ARG(id='someUnknownLabel', value=''))
        with self.assertRaises(ValueError):
            ModuleCommand(
                package_id=plot.PACKAGE_PLOT,
                command_id=plot.PLOT_SIMPLE_CHART,
                arguments=obj,
                packages=PACKAGES
            )


if __name__ == '__main__':
    unittest.main()
