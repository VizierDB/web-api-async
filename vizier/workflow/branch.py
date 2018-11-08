# Copyright (C) 2018 New York University
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

"""Basic objects to maintain viztrail branch information."""


class BranchProvenance(object):
    """Simple object that contains provenance information for each branch. The
    provenance of a branch includes the source branch identifier, the workflow
    version from which this branch was created, and the identifier of the module
    in the workflow version from which the branch was created.

    Attributes
    ----------
    source_branch : string
        Unique branch identifier of source branch
    workflow_version: int
        Version number of source workflow.
    module_id: int
        Identifier of module at which the new branch started. Note that this
        identifier defines the last module in the workflow of the created
        branch.
    """
    def __init__(self, source_branch, workflow_version, module_id):
        """Initialize the provenance object.

        Parameters
        ----------
        source_branch : string
            Unique branch identifier of source branch
        workflow_version: int
            Version number of source workflow.
        module_id: int
            Identifier of module at which the new branch started
        """
        self.source_branch = source_branch
        self.workflow_version = workflow_version
        self.module_id = module_id


class ViztrailBranch(object):
    """Branch in a viztrail. Each branch has a unique identifier, a set of user-
    defined properties, and a list of workflow versions that make up the history
    of the branch. The last entry in the workflow verion list references the
    current workflow (state) of the branch.

    Attributes
    ----------
    identifier: string
        Unique branch identifier
    properties: vizier.core.properties.ObjectPropertiesHandler
        Handler for user-defined properties that are associated with this
        viztrail branch
    provenance: vizier.workflow.branch.BranchProvenance
        Provenance information for this branch
    workflows: list(vizier.workflow.base.VersionDescriptor)
        List of workflow versions that define the history of this branch
    """
    def __init__(self, identifier, properties, provenance, workflows=None):
        """Initialize the viztrail branch.

        identifier: string
            Unique branch identifier
        properties: vizier.core.properties.ObjectPropertiesHandler
            Handler for user-defined properties that are associated with the
            branch
        provenance: vizier.workflow.base.ViztrailBranchProvenance
            Provenance information for this branch
        versions: list(int), optional
            List of unique workflow versions that define the history of this
            branch. The last entry in the list references the current workflow
            of the branch
        """
        self.identifier = identifier
        self.properties = properties
        self.provenance = provenance
        self.workflows = workflows if not workflows is None else list()
