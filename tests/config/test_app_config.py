import os
import unittest

from vizier.config.app import AppConfig, DEFAULT_SETTINGS, ENV_CONFIG
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
        # Test the default configuration if not configuration file is given.
        # Ensure that the environment variable VIZIERSERVER_CONFIG is not set.
        if not os.getenv(ENV_CONFIG) is None:
            raise ValueError('environment variable ' + ENV_CONFIG + ' is set')
        config = AppConfig()
        self.assertTrue(PACKAGE_VIZUAL in config.packages)
        for pckg in [PACKAGE_MIMIR, PACKAGE_PLOT, PACKAGE_PYTHON]:
            self.assertFalse(pckg in config.packages)
        self.assertEquals(config.webservice.server_url, DEFAULT_SETTINGS['webservice']['server_url'])
        self.assertEquals(config.webservice.server_port, DEFAULT_SETTINGS['webservice']['server_port'])
        self.assertEquals(config.webservice.defaults.row_limit, DEFAULT_SETTINGS['webservice']['defaults']['row_limit'])
        self.assertEquals(config.debug, DEFAULT_SETTINGS['debug'])
        self.assertEquals(config.engine)
        #self.validate_default_settings(config, 'MySystemDatastore')
        # Load configuration from a file. Ensure that default values are
        # overwritten if present in the file
        #config = AppConfig(configuration_file=CONFIG_FILE)
        #self.validate_default_settings(config, 'FileSystemDatastoreFactory')

    def validate_default_settings(self, settings, class_name):
        """Validate expected application settings for the default configuration
        file.
        """
        for pckg in [PACKAGE_MIMIR, PACKAGE_PLOT, PACKAGE_PYTHON, PACKAGE_VIZUAL]:
            self.assertTrue(pckg in settings.packages)
        self.assertEquals(settings.packages[PACKAGE_PYTHON].engine['properties']['moduleName'], 'NoModule')
        self.assertEquals(settings.packages[PACKAGE_PYTHON].engine['properties']['className'], 'NoClass')
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
        # Engine
        self.assertEquals(settings.engine.module_name, 'vizier.engine.environments.local')
        self.assertEquals(settings.engine.class_name, 'DefaultLocalEngine')
        self.assertEquals(settings.engine.properties['datastore']['className'], class_name)
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
