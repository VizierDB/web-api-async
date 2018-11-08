import csv
import os
import shutil
import sys
import time
import unittest

from vizier.config import AppConfig, ExecEnv, FileServerConfig
from vizier.datastore.fs import FileSystemDataStore
from vizier.datastore.metadata import DatasetMetadata
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.base import DEFAULT_BRANCH
from vizier.workflow.command import PACKAGE_PYTHON, PACKAGE_VIZUAL
from vizier.workflow.command import python_cell, load_dataset, update_cell
from vizier.workflow.repository.fs import FileSystemViztrailRepository

from vizier.api import VizierWebService
from vizier.config import AppConfig

CSV_FILE = './data/dataset.csv'
CONFIG_FILE = './data/api-config.yaml'
TSV_FILE = './data/dataset.tsv'
FILESERVER_DIR = './env/fs'
DATASTORE_DIR = './env/ds'
WORKTRAILS_DIR = './env/wt'


def list_modules_arguments_values(modules):
    values = []
    for m in modules:
        for arg in m['command']['arguments']:
            values.append(arg['value'])
    return values


class TestWebServiceAPI(unittest.TestCase):

    def setUp(self):
        """Create an new Web Service API."""
        # Clear various directories
        for d in [WORKTRAILS_DIR, DATASTORE_DIR, FILESERVER_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)
            os.mkdir(d)
        # Setup datastore and API
        self.config = AppConfig()
        self.ENV = ExecEnv(
                FileServerConfig().from_dict({'directory': FILESERVER_DIR}),
                packages=[PACKAGE_VIZUAL, PACKAGE_PYTHON]
            ).from_dict({'datastore': {'directory': DATASTORE_DIR}})
        self.ENGINE_ID = self.ENV.identifier
        self.config.envs[self.ENGINE_ID] = self.ENV
        self.config.fileserver = self.ENV.fileserver
        self.datastore = FileSystemDataStore(DATASTORE_DIR)
        self.fileserver = DefaultFileServer(FILESERVER_DIR)
        self.api = VizierWebService(
            FileSystemViztrailRepository(
                WORKTRAILS_DIR,
                {self.ENV.identifier: self.ENV}
            ),
            self.datastore,
            self.fileserver,
            self.config
        )

    def tearDown(self):
        """Clean-up by deleting created directories.
        """
        for d in [WORKTRAILS_DIR, DATASTORE_DIR, FILESERVER_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)

    def test_service_descriptors(self):
        """Ensure validity of the service descriptor and build information."""
        desc = self.api.service_overview()
        # The descriptor is expected to contain three elements: name, title, and
        # links. Name and title should be the same as in the default config
        self.validate_keys(desc, ['name', 'envs', 'properties', 'links'])
        self.assertEquals(desc['name'], self.config.name)
        self.assertFalse(len(desc['envs']) == 0)
        for env in desc['envs']:
            self.validate_keys(env, ['id', 'name', 'description', 'default', 'packages'])
        # Expect five references in the link listing: self, build, upload, doc,
        # and projects
        self.validate_links(desc['links'], ['self', 'build', 'doc', 'upload', 'projects', 'notebooks', 'files'])
        # The build information should have two elements: components and links
        build = self.api.system_build()
        self.assertEquals(len(build), 2)
        for key in ['components', 'links']:
            self.assertTrue(key in build)
        # The components list should include three entries (projects, datasets,
        # and workflows, each with name and version information.
        components = {c['name'] : c['build'] for c in build['components']}
        self.assertEquals(len(components), 3)
        for key in ['datastore', 'fileserver', 'viztrails']:
            self.assertTrue(key in components)
            for info in ['name', 'version']:
                self.assertTrue(info in components[key])

    def test_files(self):
        """Test API calls to upload and retrieve datasets."""
        # Upload a new dataset
        fh = self.api.upload_file(CSV_FILE)
        # The result should contain five elements: id, name, columns, rows, and
        # links
        self.validate_file_handle(fh)
        # Retrieve the full dataset from the API
        fh = self.api.get_file(fh['id'])
        self.validate_file_handle(fh)
        # Retrieving an unknown dataset should return None
        self.assertIsNone(self.api.get_file('invalid id'))
        self.assertIsNone(self.api.get_file('f0f0f0f0f0f0f0f0f0f0f0f0'))
        self.validate_file_listing(self.api.list_files(), 1)
        self.api.upload_file(TSV_FILE)
        self.validate_file_listing(self.api.list_files(), 2)
        self.validate_file_handle(self.api.rename_file(fh['id'], 'myfile'))
        self.validate_file_listing(self.api.list_files(), 2)
        self.assertIsNone(self.api.rename_file('invalid id', 'afile'))
        self.assertTrue(self.api.delete_file(fh['id']))
        self.validate_file_listing(self.api.list_files(), 1)

    def test_datasets(self):
        """Test retireval of datasets."""
        ds = self.datastore.load_dataset(self.fileserver.upload_file(CSV_FILE))
        ds = self.datastore.create_dataset(
            columns=ds.columns,
            rows=ds.fetch_rows()
        )
        self.validate_dataset_handle(self.api.get_dataset(ds.identifier))
        anno = self.api.update_dataset_annotation(ds.identifier, column_id=0, key='comment', value='Hello')
        anno_id = anno['annotations'][0]['id']
        self.api.update_dataset_annotation(ds.identifier, row_id=1, key='comment', value='World')
        self.api.update_dataset_annotation(ds.identifier, column_id=1, row_id=0, key='comment', value='!')
        self.validate_dataset_annotations(ds.identifier, column_id=0, expected={'comment': 'Hello'})
        self.validate_dataset_annotations(ds.identifier, row_id=1, expected={'comment': 'World'})
        self.validate_dataset_annotations(ds.identifier, column_id=1, row_id=0, expected={'comment': '!'})
        # Update annotations
        self.api.update_dataset_annotation(ds.identifier, anno_id=anno_id, column_id=0, key='comment', value='Some Name')
        self.validate_dataset_annotations(ds.identifier, column_id=0, expected={'comment': 'Some Name'})
        # Make sure unknown datasets are handeled correctly
        self.assertIsNone(self.api.get_dataset('someunknonwidentifier'))
        self.assertIsNone(self.api.get_dataset_annotations('someunknonwidentifier'))

    def test_projects(self):
        """Test API calls to create and manipulate projects."""
        # Create a new project
        ph = self.api.create_project(self.ENV.identifier, {'name' : 'My Project'})
        self.validate_project_descriptor(ph)
        self.validate_project_handle(self.api.get_project(ph['id']))
        # Project listing
        self.validate_project_listing(self.api.list_projects(), 1)
        ph = self.api.create_project(self.ENV.identifier, {'name' : 'A Project'})
        self.validate_project_handle(self.api.get_project(ph['id']))
        self.validate_project_listing(self.api.list_projects(), 2)
        # Update project properties
        props = {p['key'] : p['value'] for p in ph['properties']}
        self.assertEquals(props['name'], 'A Project')
        ph = self.api.update_project_properties(ph['id'], {'name': 'New Name'})
        self.validate_project_descriptor(ph)
        props = {p['key'] : p['value'] for p in ph['properties']}
        self.assertEquals(props['name'], 'New Name')
        # Module specifications
        modules = self.api.list_module_specifications_for_project(ph['id'])
        self.assertEquals(len(modules), 3)
        self.validate_keys(modules, ['modules', 'project', 'links'])
        self.validate_project_descriptor(modules['project'])
        for m in modules['modules']:
            self.validate_keys(m, ['type', 'id', 'name', 'arguments'], optional_keys=['group'])
            arg_keys = ['id', 'label', 'name', 'datatype', 'index', 'required']
            for arg in m['arguments']:
                self.assertTrue(len(arg) >= len(arg_keys))
                for k in arg_keys:
                    self.assertTrue(k in arg)
        # Delete project
        self.assertTrue(self.api.delete_project(ph['id']))
        self.validate_project_listing(self.api.list_projects(), 1)
        # Retrieve non-existing project should return None
        self.assertIsNone(self.api.get_project('invalid-id'))
        # Delete a non existing project should return False
        self.assertFalse(self.api.delete_project(ph['id']))
        # Updating a non exisiting project should return None
        self.assertIsNone(self.api.update_project_properties(ph['id'], {'name': 'New Name'}))

    def test_spreadsheet(self):
        """Ensure that the includeDataset option is working for spreadsheet
        updates."""
        # Upload file
        fh = self.fileserver.upload_file(CSV_FILE)
        # Create project
        ph = self.api.create_project(self.ENV.identifier, {'name' : 'My Project'})
        pid = ph['id']
        # Load dataset
        DS_NAME = 'myDS'
        cmd = load_dataset(fh.identifier, DS_NAME)
        result = self.api.append_module(pid, DEFAULT_BRANCH, -1, cmd)
        self.validate_keys(result, ['workflow', 'modules', 'datasets'])
        # Update cell and request to include dataset
        cmd = update_cell(DS_NAME, 0, 0, 'A')
        result = self.api.append_module(pid, DEFAULT_BRANCH, -1, cmd, includeDataset={'name': DS_NAME, 'offset': 0})
        self.validate_keys(result, ['workflow', 'dataset'])
        self.validate_dataset_handle(result['dataset'])
        # In case of an error the result contains the modules
        cmd = update_cell(DS_NAME, 100, 0, 'A')
        result = self.api.append_module(pid, DEFAULT_BRANCH, -1, cmd, includeDataset={'name': DS_NAME, 'offset': 0})
        self.validate_keys(result, ['workflow', 'modules', 'datasets'])

    def test_workflows(self):
        """Test API calls to retrieve and manipulate workflows."""
        # Create a new project
        ph = self.api.create_project(self.ENV.identifier, {'name' : 'My Project'})
        self.validate_branch_listing(self.api.list_branches(ph['id']), 1)
        self.validate_branch_handle(self.api.get_branch(ph['id'], DEFAULT_BRANCH))
        wf = self.api.get_workflow(ph['id'], DEFAULT_BRANCH)
        self.validate_workflow_handle(wf)
        # Raise exception when creating branch of empty workflow
        with self.assertRaises(ValueError):
            self.api.create_branch(ph['id'], DEFAULT_BRANCH, -1, 0, {'name': 'My Branch'})
        # Result is None when creating of a non existing branch
        with self.assertRaises(ValueError):
            self.api.create_branch(ph['id'], 'unknown', -1, 0, {'name': 'My Branch'})
        # Execute a new command
        last_modified = ph['lastModifiedAt']
        result = self.api.append_module(ph['id'], DEFAULT_BRANCH, -1, python_cell('2+2'))
        self.validate_workflow_update_result(result)
        wf = self.api.get_workflow(ph['id'], DEFAULT_BRANCH)
        self.validate_workflow_handle(wf)
        modules = self.api.get_workflow_modules(ph['id'], DEFAULT_BRANCH, -1)
        self.validate_workflow_modules(modules, number_of_modules=1)
        self.assertNotEquals(last_modified, wf['project']['lastModifiedAt'])
        last_modified =  wf['project']['lastModifiedAt']
        # Create a new branch
        time.sleep(1)
        desc = self.api.create_branch(ph['id'], DEFAULT_BRANCH, -1, 0, {'name': 'My Branch'})
        self.validate_branch_handle(desc)
        branch_wf = self.api.get_workflow(ph['id'], desc['id'])
        self.assertNotEquals(last_modified, branch_wf['project']['lastModifiedAt'])
        last_modified = branch_wf['project']['lastModifiedAt']
        self.validate_workflow_handle(branch_wf)
        modules = self.api.get_workflow_modules(ph['id'], desc['id'], -1)
        self.validate_workflow_modules(modules, number_of_modules=1)
        # Replace module in new branch
        time.sleep(1)
        result = self.api.replace_module(ph['id'], desc['id'], -1, 0, python_cell('3+3'))
        self.validate_workflow_update_result(result)
        modules = self.api.get_workflow_modules(ph['id'], desc['id'], -1)
        self.validate_workflow_modules(modules, number_of_modules=1)
        branch_wf = self.api.get_workflow(ph['id'], desc['id'])
        # Ensure that the last modified date of the project has changed
        self.assertNotEquals(last_modified, branch_wf['project']['lastModifiedAt'])
        # Append module to new branch
        self.validate_workflow_update_result(self.api.append_module(ph['id'], desc['id'], -1, python_cell('4+4')))
        modules = self.api.get_workflow_modules(ph['id'], desc['id'], -1)
        self.validate_workflow_modules(modules, number_of_modules=2)
        branch_wf = self.api.get_workflow_modules(ph['id'], desc['id'])
        self.assertEquals(len(branch_wf['modules']), 2)
        wf = self.api.get_workflow_modules(ph['id'], DEFAULT_BRANCH)
        self.assertEquals(len(wf['modules']), 1)
        self.validate_branch_listing(self.api.list_branches(ph['id']), 2)
        # Update new branch name
        branch_wf = self.api.update_branch(ph['id'], desc['id'], {'name':'Some Branch'})
        self.validate_branch_handle(branch_wf)
        n1 = self.api.get_branch(ph['id'], DEFAULT_BRANCH)['properties'][0]['value']
        n2 = self.api.get_branch(ph['id'], desc['id'])['properties'][0]['value']
        self.assertEquals(n2, 'Some Branch')
        self.assertNotEquals(n1, n2)
        # Retrieving the workflow for an unknown project should return None
        self.assertIsNone(self.api.get_workflow('invalid id', DEFAULT_BRANCH))
        self.assertIsNone(self.api.get_workflow('f0f0f0f0f0f0f0f0f0f0f0f0', DEFAULT_BRANCH))
        # Delete workflow
        self.assertTrue(self.api.delete_branch(ph['id'], desc['id']))
        self.assertFalse(self.api.delete_branch(ph['id'], desc['id']))
        with self.assertRaises(ValueError):
            self.api.delete_branch(ph['id'], DEFAULT_BRANCH)

    def test_workflow_commands(self):
        """Test API calls to execute workflow modules."""
        # Create a new project
        pj = self.api.create_project(self.ENV.identifier, {'name' : 'My Project'})
        # Use Python load command to test module execution
        self.api.append_module(pj['id'], DEFAULT_BRANCH, -1, python_cell('2+2'))
        self.api.append_module(pj['id'], DEFAULT_BRANCH, -1, python_cell('3+3'))
        wf_master = self.api.get_workflow_modules(pj['id'], DEFAULT_BRANCH)
        content = list_modules_arguments_values(wf_master['modules'])
        self.assertEquals(len(content), 2)
        self.assertEquals(content[0], '2+2')
        self.assertEquals(content[1], '3+3')
        branch_id = self.api.create_branch(pj['id'], DEFAULT_BRANCH, -1, 0, {'name':'My Name'})['id']
        wf_branch = self.api.get_workflow_modules(pj['id'], branch_id)
        content = list_modules_arguments_values(wf_branch['modules'])
        self.assertEquals(len(content), 1)
        self.assertEquals(content[0], '2+2')
        # Replace first module in master and append to second branch_id
        self.api.replace_module(pj['id'], DEFAULT_BRANCH, -1, 0, python_cell('4+4'))
        self.api.append_module(pj['id'], branch_id, -1, python_cell('5+5'))
        wf_master = self.api.get_workflow_modules(pj['id'], DEFAULT_BRANCH)
        content = list_modules_arguments_values(wf_master['modules'])
        self.assertEquals(len(content), 2)
        self.assertEquals(content[0], '4+4')
        self.assertEquals(content[1], '3+3')
        wf_branch = self.api.get_workflow_modules(pj['id'], branch_id)
        content = list_modules_arguments_values(wf_branch['modules'])
        self.assertEquals(len(content), 2)
        self.assertEquals(content[0], '2+2')
        self.assertEquals(content[1], '5+5')
        # Delete module
        m_count = len(wf_branch['modules'])
        m_id = wf_branch['modules'][-1]['id']
        self.api.delete_module(pj['id'], branch_id, -1, m_id)
        wf_branch = self.api.get_workflow_modules(pj['id'], branch_id)
        self.assertEquals(len(wf_branch['modules']), m_count - 1)
        for m in wf_branch['modules']:
            self.assertNotEquals(m['id'], m_id)
        self.assertIsNone(self.api.delete_module(pj['id'], branch_id, -1, 100))

    def validate_branch_listing(self, listing, number_of_branches):
        self.validate_keys(listing, ['branches', 'links'])
        self.validate_links(listing['links'], ['self', 'create', 'project'])
        self.assertEquals(len(listing['branches']), number_of_branches)
        for br in listing['branches']:
            self.validate_branch_descriptor(br)

    def validate_branch_descriptor(self, branch):
        self.validate_keys(branch, ['id', 'properties', 'links'])
        self.validate_links(branch['links'], ['self', 'delete', 'head', 'project', 'update'])

    def validate_branch_handle(self, branch):
        self.validate_keys(branch, ['id', 'project', 'workflows', 'properties', 'links'])
        self.validate_links(branch['links'], ['self', 'delete', 'head', 'project', 'update'])
        self.validate_project_descriptor(branch['project'])
        for wf in branch['workflows']:
            self.validate_workflow_descriptor(wf)

    def validate_dataset_handle(self, ds):
        self.validate_keys(ds, ['id', 'columns', 'rows', 'links', 'offset', 'rowcount', 'annotatedCells'])
        for col in ds['columns']:
            self.validate_keys(col, ['id', 'name'])
        for row in ds['rows']:
            self.validate_keys(row, ['id', 'index', 'values'])
        self.validate_links(ds['links'], ['self', 'download', 'annotations', 'pagefirst', 'pagefirstanno'])

    def validate_dataset_annotations(self, ds_id, column_id=-1, row_id=-1, expected=dict()):
        annotations = self.api.get_dataset_annotations(ds_id, column_id=column_id, row_id=row_id)
        keys = ['links', 'annotations']
        if column_id >= 0:
            keys.append('column')
        if row_id >= 0:
            keys.append('row')
        self.validate_keys(annotations, keys)
        annos = annotations['annotations']
        self.assertEquals(len(annos), len(expected))
        for anno in annos:
            self.validate_keys(anno, ['id', 'key', 'value'])
            key = anno['key']
            self.assertTrue(key in expected)
            self.assertEquals(anno['value'], expected[key])

    def validate_file_handle(self, fh):
        self.validate_keys(fh, ['id', 'name', 'columns', 'rows', 'filesize', 'createdAt', 'lastModifiedAt', 'links'])
        links = {l['rel'] : l['href'] for l in fh['links']}
        self.validate_links(fh['links'], ['self', 'delete', 'rename', 'download'])

    def validate_file_listing(self, fl, number_of_files):
        self.validate_keys(fl, ['files', 'links'])
        links = {l['rel'] : l['href'] for l in fl['links']}
        self.validate_links(fl['links'],['self', 'upload'])
        self.assertEquals(len(fl['files']), number_of_files)
        for fh in fl['files']:
            self.validate_file_handle(fh)

    def validate_keys(self, obj, keys, optional_keys=list()):
        if len(obj) > len(keys):
            for key in obj:
                self.assertTrue(key in keys or key in optional_keys, msg='Invalid key ' + key)
        else:
            for key in keys:
                self.assertTrue(key in obj, msg='Missing key ' + key)

    def validate_links(self, links, keys):
        self.validate_keys({l['rel'] : l['href'] for l in links}, keys)

    def validate_module_handle(self, module):
        self.validate_keys(module, ['id', 'command', 'text', 'stdout', 'stderr', 'datasets', 'links', 'views'])
        self.validate_keys(module['command'], ['type', 'id', 'arguments'])
        self.validate_links(module['links'], ['delete', 'insert', 'replace'])
        for ds in module['datasets']:
            self.validate_keys(ds, ['id', 'name'])

    def validate_project_descriptor(self, pd):
        self.validate_keys(pd, ['id', 'environment', 'createdAt', 'lastModifiedAt', 'properties', 'links'])
        links = {l['rel'] : l['href'] for l in pd['links']}
        self.validate_keys(links, ['self', 'delete', 'home', 'update', 'branches', 'environment'])

    def validate_project_handle(self, ph, br_count=1):
        self.validate_keys(ph, ['id', 'environment', 'createdAt', 'lastModifiedAt', 'properties', 'branches', 'links'])
        self.validate_links(ph['links'], ['self', 'delete', 'home', 'update', 'branches', 'environment'])
        self.validate_keys(ph['environment'], ['id', 'modules'])
        self.assertEquals(len(ph['branches']), br_count)
        for br in ph['branches']:
            self.validate_branch_descriptor(br)

    def validate_project_listing(self, pl, number_of_projects):
        self.validate_keys(pl, ['projects', 'links'])
        self.validate_links(pl['links'], ['self', 'create', 'home'])
        self.assertEquals(len(pl['projects']), number_of_projects)
        for pj in pl['projects']:
            self.validate_project_descriptor(pj)

    def validate_workflow_descriptor(self, wf):
        self.validate_keys(wf, ['version', 'links', 'createdAt', 'packageId', 'commandId', 'action', 'statement'])
        self.validate_links(wf['links'], ['self', 'branch', 'branches', 'append', 'modules'])

    def validate_workflow_handle(self, wf):
        self.validate_keys(wf,['project', 'branch', 'version', 'createdAt', 'state', 'links', 'readOnly'])
        self.validate_links(wf['links'], ['self', 'branch', 'branches', 'append', 'modules'])
        self.validate_project_descriptor(wf['project'])
        state = wf['state']
        self.validate_keys(state,['datasets', 'charts', 'hasError', 'moduleCount'])

    def validate_workflow_modules(self, wf, number_of_modules=0):
        self.validate_keys(wf,['project', 'branch', 'version', 'modules', 'createdAt', 'links', 'datasets', 'readOnly'])
        self.validate_links(wf['links'], ['self', 'workflow'])
        self.validate_project_descriptor(wf['project'])
        self.assertEquals(len(wf['modules']), number_of_modules)
        for m in wf['modules']:
            self.validate_module_handle(m)


    def validate_workflow_update_result(self, wf):
        self.validate_keys(wf,['workflow', 'modules', 'datasets'])
        self.validate_workflow_handle(wf['workflow'])
        for m in wf['modules']:
            self.validate_module_handle(m)
        for ds in wf['datasets']:
            self.validate_dataset_handle(ds)


if __name__ == '__main__':
    unittest.main()
