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

"""Collection of reference identifiers for HATEOAS references in the links list
of serialized objects that are returned by the web service API.
"""

# General
SELF = 'self'

# API
API_DOC = 'api.doc'
API_HOME = 'api.doc'

# Branch
BRANCH_CREATE = 'branch.create'
BRANCH_DELETE = 'branch.delete'
BRANCH_HEAD = 'branch.head'
BRANCH_UPDATE = 'branch.update'

# Dataset
ANNOTATIONS_UPDATE = 'annotations.update'
ANNOTATIONS_GET = 'annotations.get'
DATASET_DOWNLOAD = 'dataset.download'
DATASET_FETCH_ALL = 'dataset.fetch'

PAGE_FIRST = 'page.first'
PAGE_LAST = 'page.last'
PAGE_NEXT = 'page.next'
PAGE_PREV = 'page.prev'

# Files
FILE_DOWNLOAD = 'file.download'
FILE_UPLOAD = 'file.upload'

# Modules
MODULE_DELETE = 'module.delete'
MODULE_INSERT = 'module.insert'
MODULE_REPLACE = 'module.replace'

# Projects
PROJECT_CREATE = 'project.create'
PROJECT_IMPORT = 'project.import'
PROJECT_LIST = 'project.list'
PROJECT_DELETE = 'project.delete'
PROJECT_UPDATE = 'project.update'

# Workflow
WORKFLOW_APPEND = 'workflow.append'
WORKFLOW_BRANCH = 'workflow.branch'
WORKFLOW_CANCEL = 'workflow.cancel'
WORKFLOW_PROJECT = 'workflow.project'
