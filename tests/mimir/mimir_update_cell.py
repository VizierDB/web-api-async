"""Test worktrail repository implementation that uses the file system for
storage.
"""

import os
import shutil
import unittest

import vistrails.packages.mimir.init as mimir

from vizier.datastore.mimir import MimirDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.vizual.mimir import MimirVizualEngine


DATASTORE_DIR = './env/ds'
FILESERVER_DIR = './env/fs'

CSV_FILE = '../data/mimir/Employee.csv' #pick.csv


def cleanUp():
    """Remove datastore and fileserver directory."""
    # Delete directories
    for d in [DATASTORE_DIR, FILESERVER_DIR]:
        if os.path.isdir(d):
            shutil.rmtree(d)

cleanUp()

datastore = MimirDataStore(DATASTORE_DIR)
fileserver = DefaultFileServer(FILESERVER_DIR)
vizual = MimirVizualEngine(datastore, fileserver)

mimir.initialize()

filename = CSV_FILE
print 'LOAD ' + filename
f_handle = fileserver.upload_file(filename)
ds = datastore.load_dataset(f_handle)

ds_load = datastore.get_dataset(ds.identifier)
print [col.name_in_rdb + ' AS ' + col.name + '(' + col.data_type + ')' for col in ds_load.columns]
print str(ds.row_count) + ' row(s)'
rows = ds.fetch_rows()
for i in range(len(rows)):
    row = rows[i]
    print row.values

_, ds_id = vizual.update_cell(ds.identifier, 2, 0, '2015-01-10')
ds_load = datastore.get_dataset(ds_id)
print [col.name_in_rdb + ' AS ' + col.name + '(' + col.data_type + ')' for col in ds_load.columns]
print str(ds_load.row_count) + ' row(s)'
rows = ds_load.fetch_rows()
for i in range(len(rows)):
    row = rows[i]
    print row.values


mimir.finalize()
cleanUp()
