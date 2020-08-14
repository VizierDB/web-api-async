"""Test the vizier client datastore. This requires the web service API to run
and a project with datastores to have been setup.
"""

from vizier.api.client.base import VizierApiClient
from vizier.api.client.datastore.base import DatastoreClient
from vizier.api.routes.base import UrlFactory
from vizier.api.routes.datastore import DatastoreClientUrlFactory
from vizier.datastore.dataset import DatasetColumn, DatasetRow

from atexit import register as at_exit

URLS = UrlFactory(base_url='http://localhost:5000/vizier-db/api/v1')

api = VizierApiClient(URLS)
PROJECT_ID = api.create_project({ "name" : "Test Client Datastore"}).identifier

at_exit(api.delete_project, PROJECT_ID)

# We're just doing some unit testing on the fields specific to DatastoreClient, so 
# ignore complaints about instantiating an abstract class
store = DatastoreClient( # type: ignore[abstract]
    urls=DatastoreClientUrlFactory(
        urls = URLS,
        project_id=PROJECT_ID
    )
)


ds = store.create_dataset(
    columns=[DatasetColumn(identifier=0, name='Name'), DatasetColumn(identifier=1, name='Age', data_type="int")],
    rows=[DatasetRow(identifier=0, values=['Alice', 32]), DatasetRow(identifier=1, values=['Bob', 23])],
    properties={ "example_property" : "foo" }
)

# print(ds)
# print([col.identifier for col in ds.columns])
# print([col.name for col in ds.columns])

dh = store.get_dataset(ds.identifier)
assert dh is not None
for row in dh.fetch_rows():
    print([row.identifier] + row.values)

caveats = dh.get_caveats()
# print("\n".join(c.__repr__ for c in caveats))
