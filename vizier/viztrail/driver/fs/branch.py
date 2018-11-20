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

"""Implements the branch handle for the file system based repository."""

import os

from vizier.core.annotation.fs import PersistentAnnotationSet
from vizier.viztrail.branch import BranchHandle, BranchProvenance


class FSBranchHandle(BranchHandle):
    """
    """
    def __init__(
        self, identifier, base_dir, object_store=None, provenance=None,
        properties=None, workflow=None
    ):
        """
        """
        pass
        #raise NotImplementedError

    def append_workflow(self, workflow):
        """Append the given workflow handle to the branch. The workflow becomes
        the new head of the branch.

        Parameters
        ----------
        workflow: vizier.viztrail.workflow.WorkflowHandle
            New branch head
        """
        raise NotImplementedError

    @staticmethod
    def create_branch(
        identifier, base_dir, object_store=None, provenance=None,
        properties=None, workflow=None
    ):
        """Create a new branch. If the workflow is given the new branch contains
        exactly this workflow. Otherwise, the branch is empty.

        Parameters
        ----------
        identifier: string
            Unique branch identifier
        base_dir: string
            Base directory for branch resources
        object_store: vizier.viztrail.driver.fs.io.ObjectStore, optional
            Object store implementation to read and write Json files
        provenance: vizier.viztrail.base.BranchProvenance
            Provenance information for the new branch
        properties: dict, optional
            Set of properties for the new branch
        workflow: vizier.viztrail.workflow.WorkflowHandle, optional
            Head of branch

        Returns
        -------
        vizier.viztrail.branch.BranchHandle
        """
        # Create the base directory. Raises exception if the path refers to an
        # existing file or directory
        if os.path.exists(base_dir):
            raise ValueError('viztrail branch directory \'' + str(base_dir) + '\' exists')
        os.makedirs(base_dir)
        # Make sure the object store is not None
        if object_store is None:
            object_store = DefaultObjectStore()
        # Set provenance object if not given
        if provenance is None:
            provenance = BranchProvenance()
        # Write provenance information to disk
        doc = {'createdAt': provenance.created_at.isoformat()}
        if not provenance.source_branch is None:
            # If one propery is not None all are expected to be not None
            doc['sourceBranch'] = provenance.source_branch
            doc['workflowId'] = provenance.workflow_id
            doc['moduleId'] = provenance.module_id
        object_store.write(filename=FILE_BRANCH(base_dir), content=doc)
        # Return handle for new viztrail branch
        return FSBranchHandle(
            identifier=identifier,
            base_dir=base_dir,
            object_store=object_store,
            provenance=provenance,
            properties=PersistentAnnotationSet(
                filename=FILE_PROPERTIES(base_dir),
                annotations=properties
            ),
            workflow=workflow
        )

    def delete_branch(self) :
        """Deletes the directory that contains all resoures that are associated
        with this viztrail branch.
        """
        # Delete workflows
        raise NotImplementedError
        # Delete unused modules
        raise NotImplementedError
        # Delete base directory
        shutil.rmtree(self.base_dir)


# ------------------------------------------------------------------------------
# Files and Directories
# ------------------------------------------------------------------------------


def FILE_PROPERTIES(base_dir):
    """Get the absolute path to the file containing branch annotations.

    Parameters
    ----------
    base_dir: string
        Path to the base directory for the viztrail

    Returns
    -------
    string
    """
    return os.path.join(base_dir, 'properties.json')


def FILE_BRANCH(base_dir):
    """Get the absolute path to the file containing branch metadata.

    Parameters
    ----------
    base_dir: string
        Path to the base directory for the viztrail

    Returns
    -------
    string
    """
    return os.path.join(base_dir, 'branch.json')
