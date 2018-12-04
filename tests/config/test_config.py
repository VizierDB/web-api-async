import unittest

from vizier.config import AppConfig
from vizier.engine.packages.mimir.base import PACKAGE_MIMIR
from vizier.engine.packages.plot.base import PACKAGE_PLOT
from vizier.engine.packages.pycell.base import PACKAGE_PYTHON
from vizier.engine.packages.vizual.base import PACKAGE_VIZUAL

CONFIG_FILE = './.files/config.yaml'
CONFIG_WITH_ERRORS = '.files/config_with_errors.yaml'


class TestConfig(unittest.TestCase):

    def test_default_config(self):
        """Test the default configuration settings.
        """
        config = AppConfig(configuration_file=CONFIG_FILE)
        self.validate_default_settings(config)
        # Make sure that the configuration file is found if no parameter is
        # given
        config = AppConfig()
        self.validate_default_settings(config)

    def validate_default_settings(self, settings):
        """Validate expected application settings for the default configuration
        file.
        """
        for pckg in [PACKAGE_MIMIR, PACKAGE_PLOT, PACKAGE_PYTHON, PACKAGE_VIZUAL]:
            self.assertTrue(pckg in settings.packages)
        self.assertEquals(len(settings.package_parameters), 1)
        self.assertTrue(PACKAGE_PYTHON in settings.package_parameters)
        parameters = settings.package_parameters[PACKAGE_PYTHON]
        self.assertEquals(parameters['moduleName'], 'NoModule')
        self.assertEquals(parameters['className'], 'NoClass')
        # API
        self.assertEquals(settings.webservice.server_url, 'http://vizier-db.info')
        self.assertEquals(settings.webservice.server_port, 80)
        self.assertEquals(settings.webservice.server_local_port, 5000)
        self.assertEquals(settings.webservice.app_path, '/vizier-db/api/v2')
        self.assertEquals(settings.webservice.doc_url, 'http://vizier-db.info/doc/api/v1')
        self.assertEquals(settings.webservice.name, 'Vizier Web API')
        self.assertEquals(settings.app_base_url, 'http://vizier-db.info/vizier-db/api/v2')
        self.assertEquals(settings.webservice.defaults.max_file_size, 1024)
        self.assertEquals(settings.webservice.defaults.row_limit, 25)
        self.assertEquals(settings.webservice.defaults.max_row_limit, -1)
        # File server
        self.assertEquals(settings.filestore.class_name, 'NoName')
        self.assertEquals(settings.filestore.properties['directory'], 'fs-directory')
        # Viztrails
        self.assertEquals(settings.viztrails.properties['directory'], '../.env/vt')
        # Misc
        self.assertEquals(settings.debug, True)
        # Logs
        self.assertEquals(settings.logs.server, 'logs')

    def test_config_with_error(self):
        """Test loading configuration file with erroneous package specification."""
        with self.assertRaises(ValueError):
            AppConfig(configuration_file=CONFIG_WITH_ERRORS)


if __name__ == '__main__':
    unittest.main()
