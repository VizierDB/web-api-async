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

"""Collection of functions that convert objects in dictionary serializations
created by the serializers back into instances of the respective Python
classes.
"""

from vizier.datastore.annotation.base import DatasetAnnotation
from vizier.datastore.annotation.dataset import DatasetMetadata
from vizier.datastore.dataset import DatasetColumn, DatasetDescriptor, DatasetRow
from vizier.viztrail.module.output import ModuleOutputs, OutputObject
from vizier.viztrail.module.provenance import ModuleProvenance

import vizier.api.serialize.labels as labels


def ANNOTATION(obj):
    """Convert dictionary containing serialization for a dataset annotation into
    an instance of that class.

    Parameters
    ----------
    obj: dict
        Default serialization for dataset annotations

    Returns
    -------
    vizier.datastore.annotation.base.DatasetAnnotation
    """
    return DatasetAnnotation(
        key=obj[labels.KEY],
        value=obj[labels.VALUE],
        column_id=obj['columnId'] if 'columnId' in obj else None,
        row_id=obj['rowId'] if 'rowId' in obj else None
    )


def DATASET_ANNOTATIONS(obj):
    """Convert dictionary serialization into a dataset metadata object.

    Parameters
    ----------
    obj: dict
        Default serialization for dataset metadata

    Returns
    -------
    vizier.datastore.annotation.dataset.DatasetMetadata
    """
    return DatasetMetadata(
        columns=[ANNOTATION(a) for a in obj['columns']],
        rows=[ANNOTATION(a) for a in obj['rows']],
        cells=[ANNOTATION(a) for a in obj['cells']],
    )


def DATASET_DESCRIPTOR(obj):
    """Convert a dictionary into a dataset descriptor.

    Parameters
    ----------
    obj: list
        Default serialization for a dataset descriptors

    Returns
    -------
    vizier.datastore.dataset.DatasetDescriptor
    """
    return DatasetDescriptor(
        identifier=obj[labels.ID],
        columns=DATASET_COLUMNS(obj[labels.COLUMNS]),
        row_count=obj[labels.ROWCOUNT]
    )


def DATASET_COLUMNS(obj):
    """Convert a list of dictionaries into a list of dataset columns.

    Parameters
    ----------
    obj: list
        List of dataset columns in default serialization format

    Returns
    -------
    list
    """
    return [
        DatasetColumn(
            identifier=col[labels.ID],
            name=col[labels.NAME],
            data_type=col[labels.DATATYPE]
        ) for col in obj]


def DATASET_ROW(obj):
    """Convert dictionary into a dataset row object.

    Parameters
    ----------
    obj: dict
        Default serialization of a dataset row

    Returns
    -------
    vizier.datastore.dataset.DatasetRow
    """
    return DatasetRow(identifier=obj[labels.ID], values=obj[labels.ROWVALUES])


def HATEOAS(links):
    """Convert a list of references into a dictionary. The reference relation
    is the key for the dictionary and the reference href element the value.

    Parameters
    ----------
    links: list
        List of dictionaries that represent HATEOS links.

    Returns
    -------
    dict
    """
    result = dict()
    for ref in links:
        result[ref[labels.REL]] = ref[labels.HREF]
    return result


def OUTPUTS(obj):
    """Convert a set of module output streams from the default dictionary
    serialization into a ModuleOutputs object.

    Raises a ValueError if the given dictionary is not a proper output stream
    serialization.

    Parameters
    ----------
    obj: dict
        Default output serialization for a pair of module output streams

    Returns
    -------
    vizier.viztrail.module.output.ModuleOutputs
    """
    try:
        return ModuleOutputs(
            stdout=[
                OutputObject(
                    type=o['type'],
                    value=o['value']
                ) for o in obj['stdout']
            ],
            stderr=[
                OutputObject(
                    type=o['type'],
                    value=o['value']
                ) for o in obj['stderr']
            ]
        )
    except KeyError as ex:
        raise ValueError(ex)


def PROPERTIES(properties, allow_null=False):
    """Convert a list of properties from request format into a dictionary.

    Raises InvalidRequest if an invalid list of properties is given.

    Parameters
    ----------
    properties: list
        List of key,value pairs defining object properties
    allow_null: bool, optional
        Allow None values for properties if True

    Returns
    -------
    dict
    """
    result = dict()
    for prop in properties:
        if not isinstance(prop, dict):
            raise InvalidRequest('expected property to be a dictionary')
        name = None
        value = None
        for key in prop:
            if key  == labels.KEY:
                name = prop[key]
            elif key == labels.VALUE:
                value = prop[key]
            else:
                raise ValueError('invalid property element \'' + key + '\'')
        if name is None:
            raise ValueError('missing element \'key\' in property')
        if value is None and not allow_null:
            raise ValueError('missing property value for \'' + name + '\'')
        result[name] = value
    return result


def PROVENANCE(obj):
    """Convert the serialization of module provenance information into an
    instance of the ModuleProvenance class.

    Parameters
    ----------
    obj: dict
        Serialization of module provenance information

    Returns
    -------
    vizier.viztrail.module.provenance.ModuleProvenance
    """
    # All components are optional
    # Dictionary of datasets that were read
    read=None
    if 'read' in obj:
        read = dict()
        for ds in obj['read']:
            read[ds[labels.NAME]] = ds[labels.ID]
    # Dictionary of datasets that were written
    write=None
    if 'write' in obj:
        write = dict()
        for ds in obj['write']:
            if 'dataset' in ds:
                write[ds[labels.NAME]] = DATASET_DESCRIPTOR(ds['dataset'])
            else:
                write[ds[labels.NAME]] = None
    # names of datasets that were deleted
    delete=None
    if 'delete' in obj:
        delete = obj['delete']
    # Optional resource information (dictionary)
    resources=None
    if 'resources' in obj:
        resources = obj['resources']
    return ModuleProvenance(
        read=read,
        write=write,
        delete=delete,
        resources=resources
    )
