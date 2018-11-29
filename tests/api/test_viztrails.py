import os
import shutil
import unittest

from vizier.api.base import VizierApi
from vizier.client.cli.interpreter import CommandInterpreter
from vizier.config import AppConfig
from vizier.viztrail.base import PROPERTY_NAME


CONFIG_FILE = './.files/config.yaml'
CSV_FILE = './.files/dataset.csv'
REPO_DIR = './.temp'


class TestViztrailsApi(unittest.TestCase):

    def setUp(self):
        """Create an empty repository directory."""
        if os.path.isdir(REPO_DIR):
            shutil.rmtree(REPO_DIR)
        os.makedirs(REPO_DIR)
        config = AppConfig(configuration_file=CONFIG_FILE)
        self.api = VizierApi(
            filestore=config.filestore.create_instance(),
            viztrails_repository=config.viztrails.create_instance()
        )

    def tearDown(self):
        """Delete repository directory."""
        shutil.rmtree(REPO_DIR)

    def test_branch_life_cycle(self):
        """Test the Api methods to create and maintain project branches."""
        pr = self.api.projects.create_project(
            properties={PROPERTY_NAME: 'My Project'}
        )
        self.assertEquals(len(self.api.projects.list_branches(pr.identifier)), 1)
        # CREATE BRANCH
        # Error on missing branch name
        with self.assertRaises(ValueError):
            self.api.projects.create_branch(
                project_id=pr.identifier,
                properties=dict()
            )
        # Error on invalid branch point
        with self.assertRaises(ValueError):
            self.api.projects.create_branch(
                project_id=pr.identifier,
                branch_id='ABC',
                properties=dict()
            )
        with self.assertRaises(ValueError):
            self.api.projects.create_branch(
                project_id=pr.identifier,
                workflow_id='ABC',
                properties=dict()
            )
        with self.assertRaises(ValueError):
            self.api.projects.create_branch(
                project_id=pr.identifier,
                module_id='ABC',
                properties=dict()
            )
        branch = self.api.projects.create_branch(
            project_id=pr.identifier,
            properties={PROPERTY_NAME: 'master'}
        )
        self.assertEquals(branch.name, 'master')
        branches = self.api.projects.list_branches(pr.identifier)
        self.assertEquals(len(branches), 2)
        self.assertTrue(branch.identifier in [b.identifier for b in branches])
        # GET BRANCH
        self.assertEquals(
            self.api.projects.get_branch(
                project_id=pr.identifier,
                branch_id=branch.identifier
            ).identifier,
            branch.identifier
        )
        self.assertIsNone(self.api.projects.get_branch(project_id=pr.identifier, branch_id='no-id'))
        self.assertIsNone(self.api.projects.get_branch(project_id='no-id', branch_id=branch.identifier))
        # SET DEFAULT BRANCH
        self.assertIsNone(self.api.projects.set_default_branch(project_id='no-id', branch_id=branch.identifier))
        with self.assertRaises(ValueError):
            self.api.projects.set_default_branch(project_id=pr.identifier, branch_id='no-id')
        master_branch = pr.get_default_branch()
        self.assertEquals(
            self.api.projects.set_default_branch(
                project_id=pr.identifier,
                branch_id=branch.identifier
            ).identifier,
            branch.identifier
        )
        self.api.projects.set_default_branch(
            project_id=pr.identifier,
            branch_id=master_branch.identifier
        )
        # UPDATE BRANCH NAME
        # Error when updating name with None or empty string
        with self.assertRaises(ValueError):
            self.api.projects.update_branch_properties(
                project_id=pr.identifier,
                branch_id=branch.identifier,
                properties={PROPERTY_NAME: ''}
            )
        with self.assertRaises(ValueError):
            self.api.projects.update_branch_properties(
                project_id=pr.identifier,
                branch_id=branch.identifier,
                properties={PROPERTY_NAME: None}
            )
        branch = self.api.projects.update_branch_properties(
            project_id=pr.identifier,
            branch_id=branch.identifier,
            properties={PROPERTY_NAME: 'The Name', 'tags': ['A']}
        )
        self.assertEquals(branch.name, 'The Name')
        self.assertEquals(branch.properties.find_all('tags'), ['A'])
        branch = self.api.projects.update_branch_properties(
            project_id=pr.identifier,
            branch_id=branch.identifier,
            properties={'tags': ['B', 'C']}
        )
        self.assertEquals(branch.name, 'The Name')
        self.assertEquals(branch.properties.find_all('tags'), ['B', 'C'])
        # Update unknown project
        self.assertIsNone(
            self.api.projects.update_branch_properties(
                project_id='no-id',
                branch_id=branch.identifier,
                properties={'tags': ['B', 'C']}
            )
        )
        # DELETE BRANCH
        self.assertFalse(self.api.projects.delete_branch(project_id='no-id', branch_id=branch.identifier))
        self.assertFalse(self.api.projects.delete_branch(project_id=pr.identifier, branch_id='no-id'))
        self.assertTrue(self.api.projects.delete_branch(project_id=pr.identifier, branch_id=branch.identifier))
        self.assertFalse(self.api.projects.delete_branch(project_id=pr.identifier, branch_id=branch.identifier))
        self.assertIsNone(self.api.projects.get_branch(project_id=pr.identifier, branch_id=branch.identifier))
        self.assertEquals(len(self.api.projects.list_branches(project_id=pr.identifier)), 1)
        with self.assertRaises(ValueError):
            self.api.projects.delete_branch(project_id=pr.identifier, branch_id=master_branch.identifier)

    def test_list_projects(self):
        """Test project listings."""
        self.assertEquals(len(self.api.projects.list_projects()), 0)
        pr = self.api.projects.create_project(
            properties={PROPERTY_NAME: 'My Project'}
        )
        projects = self.api.projects.list_projects()
        self.assertEquals(len(projects), 1)
        self.assertEquals(projects[0].identifier, pr.identifier)
        pr = self.api.projects.create_project(
            properties={PROPERTY_NAME: 'My Project'}
        )
        projects = self.api.projects.list_projects()
        self.assertEquals(len(projects), 2)
        self.api.projects.delete_project(pr.identifier)
        projects = self.api.projects.list_projects()
        self.assertEquals(len(projects), 1)
        self.api.projects.delete_project(projects[0].identifier)
        projects = self.api.projects.list_projects()
        self.assertEquals(len(projects), 0)

    def test_project_life_cycle(self):
        """Test the Api methods to create and maintain projects."""
        # CREATE PROJECT
        # Error on missing project name
        with self.assertRaises(ValueError):
            pr = self.api.projects.create_project(properties=dict())
        pr = self.api.projects.create_project(
            properties={PROPERTY_NAME: 'My Project'}
        )
        self.assertEquals(pr.name, 'My Project')
        # GET PROJECT
        self.assertEquals(self.api.projects.get_project(pr.identifier).identifier, pr.identifier)
        self.assertIsNone(self.api.projects.get_project('no-id'))
        # UPDATE PROPERTIES
        # Error when updating name with None or empty string
        with self.assertRaises(ValueError):
            self.api.projects.update_project_properties(
                project_id=pr.identifier,
                properties={PROPERTY_NAME: ''}
            )
        with self.assertRaises(ValueError):
            self.api.projects.update_project_properties(
                project_id=pr.identifier,
                properties={PROPERTY_NAME: None}
            )
        pr = self.api.projects.update_project_properties(
            project_id=pr.identifier,
            properties={PROPERTY_NAME: 'The Name', 'tags': ['A']}
        )
        self.assertEquals(pr.name, 'The Name')
        self.assertEquals(pr.properties.find_all('tags'), ['A'])
        pr = self.api.projects.update_project_properties(
            project_id=pr.identifier,
            properties={'tags': ['B', 'C']}
        )
        self.assertEquals(pr.name, 'The Name')
        self.assertEquals(pr.properties.find_all('tags'), ['B', 'C'])
        # Update unknown project
        self.assertIsNone(
            self.api.projects.update_project_properties(
                project_id='no-id',
                properties={'tags': ['B', 'C']}
            )
        )
        # DELETE PROJECT
        self.assertFalse(self.api.projects.delete_project('no-id'))
        self.assertTrue(self.api.projects.delete_project(pr.identifier))
        self.assertFalse(self.api.projects.delete_project(pr.identifier))
        self.assertIsNone(self.api.projects.get_project(pr.identifier))


if __name__ == '__main__':
    unittest.main()
