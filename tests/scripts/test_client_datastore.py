"""Test the vizier client datastore. This requires the web service API to run
and a project with datastores to have been setup.
"""

from vizier.api.client.datastore.base import DatastoreClient
from vizier.api.routes.base import UrlFactory
from vizier.api.routes.datastore import DatastoreClientUrlFactory
from vizier.datastore.annotation.base import DatasetAnnotation
from vizier.datastore.annotation.dataset import DatasetMetadata
from vizier.datastore.dataset import DatasetColumn, DatasetRow


PROJECT_ID = '23885dc1'


def print_annotations(elements):
    for anno in elements:
        line = '['
        if not anno.column_id is None:
            line += str(anno.column_id)
        line += ','
        if not anno.row_id is None:
            line += str(anno.row_id)
        line += ']: ' + anno.key + ' = ' + str(anno.value)
        print line


def print_metadata(annotations):
    if len(annotations.columns) > 0:
        print 'COLUMNS'
        print_annotations(annotations.columns)
    if len(annotations.rows) > 0:
        print 'ROWS'
        print_annotations(annotations.rows)
    if len(annotations.cells) > 0:
        print 'CELLS'
        print_annotations(annotations.cells)


store = DatastoreClient(
    urls=DatastoreClientUrlFactory(
        urls=UrlFactory(
            base_url='http://localhost:5000/vizier-db/api/v1'),
            project_id=PROJECT_ID
        )
)

ds = store.create_dataset(
    columns=[DatasetColumn(identifier=0, name='Name'), DatasetColumn(identifier=1, name='Age')],
    rows=[DatasetRow(identifier=0, values=['Alice', 32]), DatasetRow(identifier=1, values=['Bob', 23])],
    annotations=DatasetMetadata(rows=[DatasetAnnotation(row_id=1, key='user:comment', value='Needs cleaning')])
)

print ds
print [col.identifier for col in ds.columns]
print [col.name for col in ds.columns]

dh = store.get_dataset(ds.identifier)
for row in dh.fetch_rows():
    print [row.identifier] + row.values

annotations = dh.get_annotations()
print_metadata(annotations)

store.update_annotation(
    identifier=dh.identifier,
    column_id=1,
    row_id=1,
    key='user:comment',
    new_value='Older'
)

store.update_annotation(
    identifier=dh.identifier,
    column_id=1,
    row_id=1,
    key='user:comment',
    new_value='Older than before'
)

annotations = dh.get_annotations()
print_metadata(annotations)

store.update_annotation(
    identifier=dh.identifier,
    column_id=1,
    row_id=1,
    key='user:comment',
    old_value='Older than before'
)

annotations = dh.get_annotations()
print_metadata(annotations)

store.update_annotation(
    identifier=dh.identifier,
    column_id=1,
    row_id=1,
    key='user:comment',
    old_value='Older',
    new_value='Older than before'
)

annotations = dh.get_annotations()
print_metadata(annotations)
