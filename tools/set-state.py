from vizier.api.webservice.routes import UrlFactory
from vizier.client.api.base import VizierApiClient
from vizier.core.timestamp import to_datetime
from vizier.datastore.dataset import DatasetColumn, DatasetDescriptor
from vizier.viztrail.module.output import ModuleOutputs, TextOutput
from vizier.viztrail.module.provenance import ModuleProvenance


api = VizierApiClient(urls=UrlFactory(base_url='http://localhost:5000/vizier-db/api/v1'))

result = api.set_success(
    project_id='c6f6dfd26dc04c6ea1f84b71fd90e882',
    task_id='ABC',
    finished_at=to_datetime('2018-12-11T22:55:05.346257'),
    outputs=ModuleOutputs(
        stdout=[TextOutput('All'), TextOutput('Good')],
        stderr=[TextOutput('Not Here')]
    ),
    datasets={
        'My DS': DatasetDescriptor(identifier='ABC', row_count=0),
        'The DS': DatasetDescriptor(identifier='ABC', columns=[DatasetColumn(0, 'AB')], row_count=100)
    },
    provenance=ModuleProvenance(
        read={'My DS': '000'},
        write={'Thy DS': '111', 'A DS': '567'},
        delete=['A', 'B'],
        resources={'fileId': 'The File Is Deleted'}
    )
)
print result
