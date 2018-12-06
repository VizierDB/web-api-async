"""Test validation and external representation for commands in the Plot
package.
"""

import unittest

from vizier.client.command.plot import create_plot
from vizier.datastore.dataset import DatasetColumn, DatasetDescriptor

import vizier.engine.packages.base as pckg
import vizier.engine.packages.plot.base as plot
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
PACKAGE = pckg.PackageIndex(plot.PLOT_COMMANDS)


class TestValidatePlot(unittest.TestCase):

    def test_plot(self):
        """Test validation of create plot command."""
        cmd = create_plot(
            dataset_name='ds',
            chart_name='My Chart',
            series=[{'column': 1, 'range': '0:10', 'label': 'A'}, {'column': 2}],
            xaxis_range='0:100',
            xaxis_column=1,
            validate=True
        ).to_external_form(
            command=PACKAGE.get(plot.PLOT_SIMPLE_CHART),
            datasets=DATASETS
        )
        self.assertEquals(cmd, 'CREATE PLOT \'My Chart\' FOR ds')


if __name__ == '__main__':
    unittest.main()
