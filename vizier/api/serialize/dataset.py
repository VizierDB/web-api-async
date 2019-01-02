# Copyright (C) 2018 New York University,
#                    University at Buffalo,
#                    Illinois Institute of Technology.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This module contains helper methods for the webservice that are used to
serialize datasets.
"""

import vizier.api.serialize.base as serialize
import vizier.api.serialize.labels as labels


def DATASET_ANNOTATIONS(project, dataset, annotations, column_id, row_id, urls):
    """Get dictionary serialization for dataset component annotations.

    Parameters
    ----------
    project: vizier.engine.project.base.ProjectHandle
        Handle for project containing the dataset
    dataset: vizier.datastore.dataset.DatasetDescriptor
        Dataset descriptor
    annotations: list(vizier.datastore.metadata.Annotation)
        Set of annotations for dataset components
    column_id: int
        Unique column identifier for component
    row_id: int
        Unique row identifier for component
    urls: vizier.api.routes.base.UrlFactory, optional
        Factory for resource urls

    Returns
    -------
    dict
    """
    obj = {
        'annotations': [{
            labels.ID: a.identifier,
            'key': a.key,
            'value': a.value
        } for a in annotations]
    }
    if column_id >= 0:
        obj[labels.COLUMN] = column_id
    if row_id >= 0:
        obj[labels.ROW] = row_id
    # Add references to update annotations
    obj[labels.LINKS] = serialize.HATEOAS({
        'annotations:update': urls.update_dataset_annotations(
            project_id=project.identifier,
            dataset_id=dataset.identifier
        )
    })
    return obj


def DATASET_DESCRIPTOR(dataset, name=None, project=None, urls=None):
    """Dictionary serialization for a dataset descriptor.

    Parameters
    ----------
    dataset: vizier.datastore.dataset.DatasetDescriptor
        Dataset descriptor
    name : string, optional
        User-defined dataset name
    project: vizier.engine.project.base.ProjectHandle, optional
        Handle for project containing the dataset
    urls: vizier.api.routes.base.UrlFactory, optional
        Factory for resource urls

    Returns
    -------
    dict
    """
    obj = {
        labels.ID: dataset.identifier,
        labels.COLUMNS: [
            {labels.ID: col.identifier, labels.NAME: col.name} for col in dataset.columns
        ],
        labels.ROWCOUNT: dataset.row_count
    }
    if not name is None:
        obj[labels.NAME] = name
    # Add self reference if the project and url factory are given
    if not project is None and not urls is None:
        project_id = project.identifier
        dataset_id = dataset.identifier
        obj[labels.LINKS] = serialize.HATEOAS({
            labels.SELF: urls.get_dataset(
                project_id=project_id,
                dataset_id=dataset_id
            ),
            'dataset:download': urls.download_dataset(
                project_id=project_id,
                dataset_id=dataset_id
            ),
            'annotations:get': urls.get_dataset_annotations(
                project_id=project_id,
                dataset_id=dataset_id
            ),
            'annotations:update': urls.update_dataset_annotations(
                project_id=project_id,
                dataset_id=dataset_id
            )
        })
    return obj


def DATASET_HANDLE(project, dataset, rows, defaults, urls, offset=0, limit=-1):
    """Dictionary serialization for dataset handle. Includes (part of) the
    dataset rows.

    Parameters
    ----------
    project: vizier.engine.project.base.ProjectHandle
        Handle for project containing the dataset
    dataset : vizier.datastore.dataset.DatasetDescriptor
        Dataset descriptor
    rows: list(vizier.datastore.dataset.DatasetRow)
        List of rows from the dataset
    defaults : vizier.config.base.ConfigObject
        Web service default values
    urls: vizier.api.routes.base.UrlFactory
        Factory for resource urls
    offset: int, optional
        Number of rows at the beginning of the list that are skipped.
    limit: int, optional
        Limits the number of rows that are returned.

    Returns
    -------
    dict
    """
    # Use the dataset descriptor as the base
    obj = DATASET_DESCRIPTOR(dataset=dataset, project=project, urls=urls)
    # Serialize rows. The default dictionary representation for a row does
    # not include the row index position nor the annotation information.
    serialized_rows = list()
    annotated_cells = list()
    for row in rows:
        obj = row.to_dict()
        obj[labels.ROWINDEX] = len(serialized_rows) + offset
        serialized_rows.append(obj)
        for i in range(len(dataset.columns)):
            if row.cell_annotations[i] == True:
                annotated_cells.append({
                    labels.COLUMN: dataset.columns[i].identifier,
                    labels.ROW: row.identifier
                })
    # Serialize the dataset schema and cells
    obj[labels.ROWS] = serialized_rows
    obj[labels.ROWCOUNT] = dataset.row_count
    obj[labels.OFFSET] = serialized_rows
    obj[labels.ANNOTATED_CELLS] = annotated_cells
    # Add pagination references
    links = obj[labels.LINKS]
    # Max. number of records shown
    if not limit is None and limit >= 0:
        max_rows_per_request = int(limit)
    elif defaults.row_limit >= 0:
        max_rows_per_request = defaults.row_limit
    elif defaults.max_row_limit >= 0:
        max_rows_per_request = defaults.max_row_limit
    else:
        max_rows_per_request = -1
    # List of pagination Urls
    # FIRST: Always include Url's to access the first page
    links.extend(
        serialize.HATEOAS({
            labels.PAGE_FIRST: urls.dataset_pagination(
                project_id=project_id,
                dataset_id=dataset_id,
                offset=offset,
                limit=limit
            )
        })
    )
    # PREV: If offset is greater than zero allow to fetch previous page
    if not offset is None and offset > 0:
        if max_rows_per_request >= 0:
            if offset > 0:
                prev_offset = max(offset - max_rows_per_request, 0)
                links.extend(
                    serialize.HATEOAS({
                        labels.PAGE_PREV: urls.dataset_pagination(
                            project_id=project_id,
                            dataset_id=dataset_id,
                            offset=prev_offset,
                            limit=limit
                        )
                    })
                )
    # NEXT & LAST: If there are rows beyond the current offset+limit include
    # Url's to fetch next page and last page.
    if offset < dataset.row_count and max_rows_per_request >= 0:
        next_offset = offset + max_rows_per_request
        if next_offset < dataset.row_count:
            links.extend(
                serialize.HATEOAS({
                    labels.PAGE_NEXT: urls.dataset_pagination(
                        project_id=project_id,
                        dataset_id=dataset_id,
                        offset=next_offset,
                        limit=limit
                    )
                })
            )
        last_offset = (dataset.row_count - max_rows_per_request)
        if last_offset > offset:
            links.extend(
                serialize.HATEOAS({
                    labels.PAGE_LAST: urls.dataset_pagination(
                        project_id=project_id,
                        dataset_id=dataset_id,
                        offset=last_offset,
                        limit=limit
                    )
                })
            )
    # Return pagination Url list
    return page_urls
