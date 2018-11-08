"""Test worktrail repository implementation that uses the file system for
storage.
"""

import os
import shutil
import unittest

import vistrails.packages.mimir.init as mimir

from vizier.config import ExecEnv, FileServerConfig
from vizier.config import ENGINEENV_MIMIR
from vizier.datastore.client import DatasetClient
from vizier.datastore.mimir import MimirDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.base import DEFAULT_BRANCH
from vizier.workflow.engine.viztrails import DefaultViztrailsEngine
from vizier.workflow.module import ModuleSpecification
from vizier.workflow.repository.fs import FileSystemViztrailRepository
from vizier.workflow.vizual.mimir import MimirVizualEngine

import vizier.config as config
import vizier.workflow.command as cmd

DATASTORE_DIR = './env/ds'
FILESERVER_DIR = './env/fs'
VIZTRAILS_DIR = './env/wt'

CSV_FILE = './data/dataset.csv'
GEO_FILE = './data/mimir/geo.csv'
KEY_REPAIR_FILE = './data/key_repair.csv'
INCOMPLETE_CSV_FILE = './data/dataset_with_missing_values.csv'
PICKER_FILE = './data/dataset_pick.csv'

ENV = ExecEnv(
        FileServerConfig().from_dict({'directory': FILESERVER_DIR}),
        identifier=ENGINEENV_MIMIR
    ).from_dict({'datastore': {'directory': DATASTORE_DIR}})
ENGINE_ID = ENV.identifier

DS_NAME = 'people'


class TestMimirLenses(unittest.TestCase):

    def setUp(self):
        """Create an empty work trails repository."""
        # Create fresh set of directories
        for d in [DATASTORE_DIR, FILESERVER_DIR, VIZTRAILS_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)
            os.mkdir(d)
        self.datastore = MimirDataStore(DATASTORE_DIR)
        self.fileserver = DefaultFileServer(FILESERVER_DIR)
        vizual = MimirVizualEngine(self.datastore, self.fileserver)
        self.db = FileSystemViztrailRepository(
            VIZTRAILS_DIR,
            {ENV.identifier: ENV}
        )

    def tearDown(self):
        """Clean-up by dropping the MongoDB colelction used by the engine.
        """
        # Delete directories
        for d in [DATASTORE_DIR, FILESERVER_DIR, VIZTRAILS_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)

    def test_domain_lens(self):
        """Test DOMAIN lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.fileserver.upload_file(INCOMPLETE_CSV_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        col_age = ds.column_by_name('Age')
        self.assertFalse(wf.has_error)
        # Missing Value Lens
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_domain(DS_NAME, col_age.identifier)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        if wf.has_error:
            print wf.modules[-1].stderr[0]
        self.assertEquals(wf.modules[-1].command_text.upper(), 'DOMAIN FOR AGE IN PEOPLE')
        self.assertFalse(wf.has_error)
        self.assertEquals(len(wf.modules), 2)
        # Get dataset
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        rows = ds.fetch_rows()
        self.assertNotEquals(rows[2].values[ds.column_index('Age')], '')
        # Introduce an error. Make sure command formating is correct
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_domain('MY DS', 'MY COL')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertTrue(wf.has_error)
        self.assertEquals(wf.modules[-1].command_text.upper(), 'DOMAIN FOR \'MY COL\' IN \'MY DS\'')
        mimir.finalize()

    def test_geocode_lens(self):
        """Test GEOCODE lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.fileserver.upload_file(GEO_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.assertFalse(wf.has_error)
        # Geocode Lens
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_geocode(
                DS_NAME,
                'GOOGLE',
                house_nr=ds.column_by_name('STRNUMBER').identifier,
                street=ds.column_by_name('STRNAME').identifier,
                city=ds.column_by_name('CITY').identifier,
                state=ds.column_by_name('STATE').identifier
            )
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        if wf.has_error:
            print wf.modules[-1].stderr[0]
        self.assertEquals(wf.modules[-1].command_text.upper(), 'GEOCODE HOUSE_NUMBER=STRNUMBER,STREET=STRNAME,CITY=CITY,STATE=STATE PEOPLE USING GOOGLE')
        self.assertFalse(wf.has_error)
        self.assertEquals(len(wf.modules), 2)
        # Get dataset
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.assertEquals(len(ds.columns), 6)
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_geocode(
                DS_NAME,
                'GOOGLE'
            )
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        if wf.has_error:
            print wf.modules[-1].stderr[0]
        self.assertEquals(wf.modules[-1].command_text.upper(), 'GEOCODE PEOPLE USING GOOGLE')
        self.assertFalse(wf.has_error)
        self.assertEquals(len(wf.modules), 3)
        # Get dataset
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.assertEquals(len(ds.columns), 8)
        mimir.finalize()

    def test_key_repair_lens(self):
        """Test KEY REPAIR lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.fileserver.upload_file(KEY_REPAIR_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        ds1 = self.datastore.get_dataset(wf.modules[0].datasets[DS_NAME])
        # Missing Value Lens
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_key_repair(DS_NAME, ds1.column_by_name('Empid').identifier)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        self.assertEquals(wf.modules[-1].command_text.upper(), 'KEY REPAIR FOR EMPID IN ' + DS_NAME.upper())
        # Get dataset
        ds2 = self.datastore.get_dataset(wf.modules[0].datasets[DS_NAME])
        self.assertEquals(ds1.row_count, ds2.row_count)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.assertEquals(len(ds.columns), 4)
        self.assertEquals(ds.row_count, 2)
        names = set()
        empids = set()
        rowids = set()
        for row in DatasetClient(dataset=ds).rows:
            rowids.add(row.identifier)
            empids.add(int(row.get_value('empid')))
            names.add(row.get_value('name'))
        self.assertTrue(1 in empids)
        self.assertTrue(2 in rowids)
        self.assertTrue('Alice' in names)
        self.assertTrue('Carla' in names)
        # Test error case and command text
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_key_repair('MY DS', 'MY COL')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertTrue(wf.has_error)
        self.assertEquals(wf.modules[-1].command_text.upper(), 'KEY REPAIR FOR \'MY COL\' IN \'MY DS\'')
        mimir.finalize()

    def test_missing_value_lens(self):
        """Test MISSING_VALUE lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.fileserver.upload_file(INCOMPLETE_CSV_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.assertFalse(wf.has_error)
        # Missing Value Lens
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_missing_value(DS_NAME, ds.column_by_name('AGE').identifier)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        self.assertEquals(wf.modules[-1].command_text.upper(), 'MISSING VALUES FOR AGE IN ' + DS_NAME.upper())
        self.assertEquals(len(wf.modules), 2)
        # Get dataset
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        rows = ds.fetch_rows()
        self.assertNotEquals(rows[2].values[ds.column_index('Age')], '')
        # Annotations
        annotations = ds.get_annotations(column_id=1, row_id=4)
        self.assertEquals(len(annotations), 2)
        # MISSING VALUE Lens with value constraint
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'New Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_missing_value(
                DS_NAME,
                ds.column_by_name('AGE').identifier,
                constraint='> 30')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        if wf.has_error:
            print wf.modules[-1].stderr[0]
        self.assertFalse(wf.has_error)
        self.assertEquals(wf.modules[-1].command_text.upper(), 'MISSING VALUES FOR AGE IN ' + DS_NAME.upper() + ' WITH CONSTRAINT > 30')
        #self.assertEquals(wf.modules[-1].command_text.upper(), 'MISSING VALUES FOR AGE IN ' + DS_NAME.upper())
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        rows = ds.fetch_rows()
        self.assertTrue(rows[2].values[ds.column_index('Age')] > 30)
        # Command text in case of error
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_missing_value('MY DS', '?', constraint='A B')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertTrue(wf.has_error)
        cmd_text = wf.modules[-1].command_text.upper()
        expected_text = 'MISSING VALUES FOR ? IN \'MY DS\'' + ' WITH CONSTRAINT A B'
        self.assertEquals(cmd_text, expected_text)
        mimir.finalize()

    def test_missing_key_lens(self):
        """Test MISSING_KEY lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.fileserver.upload_file(INCOMPLETE_CSV_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        # Missing Value Lens
        age_col = ds.columns[ds.column_index('Age')].identifier
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_missing_key(DS_NAME, age_col, missing_only=True)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertEquals(wf.modules[-1].command_text.upper(), 'MISSING KEYS FOR AGE IN ' + DS_NAME.upper())
        self.assertFalse(wf.has_error)
        # Get dataset
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.assertEquals(len(ds.columns), 3)
        rows = ds.fetch_rows()
        self.assertEquals(len(rows), 24)
        #self.db.append_workflow_module(
        #    viztrail_id=vt.identifier,
        #    command=cmd.load_dataset(f_handle.identifier, DS_NAME + '2')
        #)
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_missing_key(
                DS_NAME,
                ds.columns[ds.column_index('Salary')].identifier,
                missing_only=True
            )
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Get dataset
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.assertEquals(len(ds.columns), 3)
        rows = ds.fetch_rows()
        self.assertEquals(len(rows), 55)
        mimir.finalize()

    def test_picker_lens(self):
        """Test PICKER lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.fileserver.upload_file(PICKER_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Missing Value Lens
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_picker(DS_NAME, [
                {'pickFrom': ds.column_by_name('Age').identifier},
                {'pickFrom': ds.column_by_name('Salary').identifier}
            ])
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        if wf.modules[-1].has_error:
            print wf.modules[-1].stderr
        self.assertFalse(wf.has_error)
        self.assertEquals(wf.modules[-1].command_text.upper(), 'PICK FROM AGE,SALARY IN ' + DS_NAME.upper())
        # Get dataset
        self.assertEquals(len(wf.modules[-1].datasets), 1)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        columns = [c.name for c in ds.columns]
        self.assertEquals(len(ds.columns), 5)
        self.assertTrue('PICK_ONE_AGE_SALARY' in columns)
        # Pick another column, this time with custom name
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_picker(DS_NAME, [
                {'pickFrom': ds.column_by_name('Age').identifier},
                {'pickFrom': ds.column_by_name('Salary').identifier}
            ],
            pick_as='My Column')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Get dataset
        self.assertEquals(len(wf.modules[-1].datasets), 1)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        columns = [c.name for c in ds.columns]
        self.assertEquals(len(ds.columns), 6)
        self.assertTrue('PICK_ONE_AGE_SALARY' in columns)
        self.assertTrue('My Column' in columns)
        # Pick from a picked column
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_picker(DS_NAME, [
                {'pickFrom': ds.column_by_name('Age').identifier},
                {'pickFrom': ds.column_by_name('PICK_ONE_AGE_SALARY').identifier}
            ],
            pick_as='My Column')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        if wf.modules[-1].has_error:
            print wf.modules[-1].stderr
        self.assertFalse(wf.has_error)
        self.assertEquals(wf.modules[-1].command_text.upper(), 'PICK FROM AGE,PICK_ONE_AGE_SALARY AS \'MY COLUMN\' IN ' + DS_NAME.upper())
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        mimir.finalize()

    def test_schema_matching_lens(self):
        """Test SCHEMA_MATCHING lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.fileserver.upload_file(CSV_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Missing Value Lens
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_schema_matching(DS_NAME, [
                {'column': 'BDate', 'type': 'int'},
                {'column': 'PName', 'type': 'varchar'}
            ], 'new_' + DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        self.assertEquals(wf.modules[-1].command_text.upper(), 'SCHEMA MATCHING PEOPLE (BDATE INT, PNAME VARCHAR) AS NEW_' + DS_NAME.upper())
        # Get dataset
        self.assertEquals(len(wf.modules[-1].datasets), 2)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets['new_' + DS_NAME])
        self.assertEquals(len(ds.columns), 2)
        self.assertEquals(ds.row_count, 2)
        # Error if adding an existing dataset
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_schema_matching(
                DS_NAME,
                [{'column': 'BDate', 'type': 'int'}],
                'new_' + DS_NAME
            )
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertTrue(wf.has_error)
        self.db.replace_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_schema_matching(
                DS_NAME,
                [{'column': 'BDate', 'type': 'int'}],
                'a_new_' + DS_NAME
            ),
            module_id=wf.modules[-1].identifier,
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        self.assertEquals(wf.modules[-1].command_text.upper(), 'SCHEMA MATCHING PEOPLE (BDATE INT) AS A_NEW_' + DS_NAME.upper())
        # Error when adding a dataset with an invalid name
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_schema_matching(
                DS_NAME,
                [{'column': 'BDate', 'type': 'int'}],
                'SOME NAME'
            )
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertTrue(wf.has_error)
        self.assertEquals(wf.modules[-1].command_text.upper(), 'SCHEMA MATCHING PEOPLE (BDATE INT) AS \'SOME NAME\'')
        mimir.finalize()

    def test_type_inference_lens(self):
        """Test TYPE INFERENCE lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.fileserver.upload_file(INCOMPLETE_CSV_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        ds1 = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.assertFalse(wf.has_error)
        # Infer type
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_type_inference(DS_NAME, 0.6)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        print wf.modules[-1].command_text.upper()
        self.assertEquals(wf.modules[-1].command_text.upper(), 'TYPE INFERENCE FOR COLUMNS IN ' + DS_NAME.upper() + ' WITH PERCENT_CONFORM = 0.6')
        # Get dataset
        ds2 = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.assertEquals(len(ds2.columns), 3)
        self.assertEquals(ds2.row_count, 7)
        ds1_rows = ds1.fetch_rows()
        ds2_rows = ds2.fetch_rows()
        for i in range(ds2.row_count):
            self.assertEquals(ds1_rows[i].values, ds2_rows[i].values)
        mimir.finalize()


if __name__ == '__main__':
    unittest.main()
