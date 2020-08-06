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

"""Definition of frequently used element names."""
# General object attributes
ID = 'id'
NAME = 'name'
DATA = 'data'

# Properties
KEY = 'key'
VALUE = 'value'

# HATEOAS
HREF = 'href'
LINKS = 'links'
REL = 'rel'

# Datasets
PROPERTIES = 'properties'
COLUMN = 'column'
COLUMNS = 'columns'
DATATYPE = 'type'
OFFSET = 'offset'
ROW = 'row'
ROWS = 'rows'
ROWCOUNT = 'rowCount'
ROWVALUES = 'values'
ROWCAVEATFLAGS = 'rowAnnotationFlags'
CAVEATS = 'caveats'

# Dataobjects
OBJECT_TYPE="objType"

# Annotations
COLUMN_ID = 'columnId'
ROW_ID = 'rowId'
OLD_VALUE = 'oldValue'
NEW_VALUE = 'newValue'

# Timestamps
TIMESTAMPS = 'timestamps'
CREATED_AT = 'createdAt'
FINISHED_AT = 'finishedAt'
STARTED_AT = 'startedAt'

# Module and workflow state
STATE = 'state'

# Modules
CHARTS = 'charts'
DATASETS = 'datasets'
OUTPUTS = 'outputs'
PROVENANCE = 'provenance'
ARTIFACTS = 'artifacts'

# Task state changes
RESULT = 'result'

# Commands
COMMAND = 'command'
COMMAND_PACKAGE = 'packageId'
COMMAND_ID = 'commandId'
COMMAND_ARGS = 'arguments'
CONTEXT = 'context'
CONTEXT_DATASETS = CONTEXT + 'Datasets'
CONTEXT_DATAOBJECTS = CONTEXT + 'Dataobjects'
RESOURCES = 'resources'
