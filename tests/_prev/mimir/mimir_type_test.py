"""Test worktrail repository implementation that uses the file system for
storage.
"""

import os
import shutil
import unittest

import vistrails.packages.mimir.init as mimir

from vizier.config import ExecEnv, FileServerConfig
from vizier.config import ENGINEENV_MIMIR
from vizier.datastore.mimir import MimirDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.repository.fs import FileSystemViztrailRepository
from vizier.workflow.vizual.mimir import MimirVizualEngine

import vizier.workflow.command as cmd


DATASTORE_DIR = './env/ds'
FILESERVER_DIR = './env/fs'
VIZTRAILS_DIR = './env/wt'

CSV_FILE = '../data/mimir/pick.csv'

DS_NAME = 'pickone'


def cleanUp():
    """Remove datastore and fileserver directory."""
    # Delete directories
    for d in [DATASTORE_DIR, FILESERVER_DIR, VIZTRAILS_DIR]:
        if os.path.isdir(d):
            shutil.rmtree(d)

def print_dataset(ds):
    print [col.name_in_rdb + ' AS ' + col.name + '(' + col.data_type + ')' for col in ds.columns]
    print str(ds.row_count) + ' row(s)'
    rows = ds.fetch_rows()
    for i in range(len(rows)):
        row = rows[i]
        print row.values


cleanUp()

ENV = ExecEnv(
        FileServerConfig().from_dict({'directory': FILESERVER_DIR}),
        identifier=ENGINEENV_MIMIR
    ).from_dict({'datastore': {'directory': DATASTORE_DIR}})

datastore = MimirDataStore(DATASTORE_DIR)
fileserver = DefaultFileServer(FILESERVER_DIR)
vizual = MimirVizualEngine(datastore, fileserver)
db = FileSystemViztrailRepository(VIZTRAILS_DIR, {ENV.identifier: ENV})

mimir.initialize()

vt = db.create_viztrail(ENV.identifier, {'name' : 'My Project'})

#
# LOAD DATASET
#
f_handle = fileserver.upload_file(CSV_FILE)
db.append_workflow_module(
    viztrail_id=vt.identifier,
    command=cmd.load_dataset(f_handle.identifier, DS_NAME)
)
wf = db.get_workflow(viztrail_id=vt.identifier)
ds = datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
print_dataset(ds)

"""
#
# PICKER LENS
#
db.append_workflow_module(
    viztrail_id=vt.identifier,
    command=cmd.mimir_picker(
        DS_NAME,
        [
            {'pickFrom': 'A'},
            {'pickFrom': 'B'}
        ],
        pick_as='A_B')
)
wf = db.get_workflow(viztrail_id=vt.identifier)
ds = datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
print_dataset(ds)

"""
#
# INSERT ROW
#
db.append_workflow_module(
    viztrail_id=vt.identifier,
    command=cmd.insert_row(
        DS_NAME,
        2
    )
)
wf = db.get_workflow(viztrail_id=vt.identifier)
ds = datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
print_dataset(ds)


#
# INSERT COLUMN
#
db.append_workflow_module(
    viztrail_id=vt.identifier,
    command=cmd.insert_column(
        DS_NAME,
        2,
        'New_column_01'
    )
)
wf = db.get_workflow(viztrail_id=vt.identifier)
ds = datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
print_dataset(ds)
"""
#
# MISSING VALUE LENS
#
db.append_workflow_module(
    viztrail_id=vt.identifier,
    command=cmd.mimir_missing_value(
        DS_NAME,
        3
    )
)
wf = db.get_workflow(viztrail_id=vt.identifier)
ds = datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
print_dataset(ds)
"""
#
# UPDATE CELL
#
db.append_workflow_module(
    viztrail_id=vt.identifier,
    command=cmd.update_cell(DS_NAME, 0, 2, 10)
)
wf = db.get_workflow(viztrail_id=vt.identifier)
#print wf.modules[-1].stderr
ds = datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
print_dataset(ds)

mimir.finalize()
cleanUp()
