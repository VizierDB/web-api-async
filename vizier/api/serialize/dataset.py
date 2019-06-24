# Copyright (C) 2017-2019 New York University,
#                         University at Buffalo,
#                         Illinois Institute of Technology.
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
import vizier.api.routes.base as routes
import vizier.api.serialize.hateoas as ref
import vizier.api.serialize.labels as labels


def ANNOTATION(anno):
    """Get dictionary serialization for a dataset annotation.

    Parameters
    ----------
    anno: vizier.datastore.annotation.base.DatasetAnnotation
        Dataset annotation object

    Returns
    -------
    dict
    """
    obj = {
        labels.KEY: anno.key,
        labels.VALUE: anno.value
    }
    if not anno.column_id is None:
        obj['columnId'] = anno.column_id
    if not anno.row_id is None:
        obj['rowId'] = anno.row_id
    return obj


def DATASET_ANNOTATIONS(project, dataset, annotations, urls):
    """Get dictionary serialization for dataset component annotations.

    Parameters
    ----------
    project: vizier.engine.project.base.ProjectHandle
        Handle for project containing the dataset
    dataset: vizier.datastore.dataset.DatasetDescriptor
        Dataset descriptor
    annotations: vizier.datastore.annotation.dataset.DatasetMetadata
        Set of annotations for dataset components
    urls: vizier.api.routes.base.UrlFactory, optional
        Factory for resource urls

    Returns
    -------
    dict
    """
    obj = {
        'annotations': [ANNOTATION(a) for a in annotations.annotations],
        'columns': [ANNOTATION(a) for a in annotations.columns],
        'rows': [ANNOTATION(a) for a in annotations.rows],
        'cells': [ANNOTATION(a) for a in annotations.cells]
    }
    # Add references to update annotations
    obj[labels.LINKS] = serialize.HATEOAS({
        ref.ANNOTATIONS_UPDATE: urls.update_dataset_annotations(
            project_id=project.identifier,
            dataset_id=dataset.identifier
        )
    })
    return obj


def DATASET_COLUMN(column):
    """Dictionary serialization for a dataset column.

    Parameters
    ----------
    column: vizier.datastore.dataset.DatasetColumn
        Dataset column

    Returns
    -------
    dict
    """
    return {
        labels.ID: column.identifier,
        labels.NAME: column.name,
        labels.DATATYPE: column.data_type,
    }


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
        labels.COLUMNS: [DATASET_COLUMN(col) for col in dataset.columns],
        labels.ROWCOUNT: dataset.row_count
    }
    if not name is None:
        obj[labels.NAME] = name
    # Add self reference if the project and url factory are given
    if not project is None and not urls is None:
        project_id = project.identifier
        dataset_id = dataset.identifier
        dataset_url = urls.get_dataset(
            project_id=project_id,
            dataset_id=dataset_id
        )
        obj[labels.LINKS] = serialize.HATEOAS({
            ref.SELF: dataset_url,
            ref.DATASET_FETCH_ALL: dataset_url + '?' + routes.PAGE_LIMIT + '=-1',
            ref.DATASET_DOWNLOAD: urls.download_dataset(
                project_id=project_id,
                dataset_id=dataset_id
            ),
            ref.ANNOTATIONS_GET: urls.get_dataset_annotations(
                project_id=project_id,
                dataset_id=dataset_id
            ),
            ref.ANNOTATIONS_UPDATE: urls.update_dataset_annotations(
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
    for row in rows:
        serialized_rows.append(DATASET_ROW(row))
    # Serialize the dataset schema and cells
    obj[labels.ROWS] = serialized_rows
    obj[labels.ROWCOUNT] = dataset.row_count
    obj[labels.OFFSET] = offset
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
    project_id = project.identifier
    dataset_id = dataset.identifier
    links.extend(
        serialize.HATEOAS({
            ref.PAGE_FIRST: urls.dataset_pagination(
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
                        ref.PAGE_PREV: urls.dataset_pagination(
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
                    ref.PAGE_NEXT: urls.dataset_pagination(
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
                    ref.PAGE_LAST: urls.dataset_pagination(
                        project_id=project_id,
                        dataset_id=dataset_id,
                        offset=last_offset,
                        limit=limit
                    )
                })
            )
    # Return pagination Url list
    return obj


def DATASET_IDENTIFIER(identifier, name):
    """Dictionary serialization for a dataset that is associated with a
    workflow module.

    Parameters
    ----------
    identifier: string
        Unique dataset identifier
    name : string
        User-defined dataset name

    Returns
    -------
    dict
    """
    return {
        labels.ID: identifier,
        labels.NAME: name
    }


def DATASET_ROW(row):
    """Dictionary serialization for a dataset row.

    Parameters
    ----------
    row: vizier.datastore.dataset.DatasetRow
        Dataset row

    Returns
    -------
    dict
    """
    return {
        labels.ID: row.identifier,
        labels.ROWVALUES: row.values,
        labels.ROWANNOTATIONFLAGS: row.annotations
    }
