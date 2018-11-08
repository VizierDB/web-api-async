"""Test worktrail repository implementation that uses the file system for
storage.
"""

import os
import shutil
import unittest

import vistrails.packages.mimir.init as mimir

from vizier.datastore.mimir import MimirDataStore
from vizier.filestore.base import DefaultFileServer


DATASTORE_DIR = './env/ds'
FILESERVER_DIR = './env/fs'

CSV_FILE = '../data/mimir/jsonsampletocsv.csv' #DetectSeriesTest1.csv' #Employee.csv'


def cleanUp():
    """Remove datastore and fileserver directory."""
    # Delete directories
    for d in [DATASTORE_DIR, FILESERVER_DIR]:
        if os.path.isdir(d):
            shutil.rmtree(d)

cleanUp()

datastore = MimirDataStore(DATASTORE_DIR)
fileserver = DefaultFileServer(FILESERVER_DIR)

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

mimir.finalize()
cleanUp()
