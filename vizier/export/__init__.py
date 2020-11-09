# Copyright (C) 2017-2020 New York University,
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


from typing import Dict, List, IO
import shutil
from io import BytesIO
import json
from tarfile import open as taropen, TarInfo
from datetime import datetime

from vizier.engine.project.base import ProjectHandle
from vizier.engine.base import VizierEngine
from vizier.api.serialize.project import PROJECT_HANDLE
from vizier.api.serialize.module import MODULE_HANDLE
from vizier.api.serialize.files import FILE_HANDLE as FILE_HANDLE
from vizier.viztrail.base import BranchProvenance
from vizier.viztrail.module.base import ModuleHandle, MODULE_PENDING
from vizier.viztrail.module.timestamp import ModuleTimestamp
from vizier.viztrail.command import ModuleCommand
import vizier.api.serialize.labels as labels


EXPORT_VERSION = "1"
VERSION_PATH = "version.txt"
PROJECT_PATH = "project.json"
FILE_PATH    = "fs/{}"

def export_project(
    target_file: str, 
    project: ProjectHandle
  ) -> None:
  with taropen(target_file, mode = 'w:gz') as archive:

    def add_buffer(data: bytes, path: str):
      info = TarInfo(path)
      info.size = len(data)
      stream = BytesIO(data)
      archive.addfile(info, stream)

    def add_stream(stream: IO[bytes], path: str, size: int):
      info = TarInfo(path)
      info.size = size
      archive.addfile(info, stream)

    add_buffer(EXPORT_VERSION.encode(), VERSION_PATH)

    project_handle = PROJECT_HANDLE(
                        project, 
                        urls = None, 
                        deep_branch = True,
                        deep_workflow = False,
                      )


    all_modules: Dict[str, ModuleHandle] = dict()

    # Add 'module' details to workflow/branch export
    for branch_handle in project_handle["branches"]:

      branch = project.viztrail.get_branch(branch_handle["id"])
      # We just got the branch from the project... it had better not be None
      assert(branch is not None)

      for workflow_handle in branch_handle["workflows"]:
        workflow = branch.get_workflow(workflow_handle["id"])
        
        # Two goals here:
        #   1. Ensure the module is added to all_modules
        #   2. Save the module identifiers for the workflow to the workflow
        module_ids: List[str] = list()
        for module in workflow.modules:
          assert(module.identifier is not None)
          all_modules[module.identifier] = module
          module_ids += [module.identifier]

        workflow_handle["modules"] = module_ids

    # Take all the modules we found in a workflow and save them
    project_handle["modules"] = {
      module_id : MODULE_HANDLE(
        project = project, 
        branch = None, 
        module = all_modules[module_id],
        urls = None
      )
      for module_id in all_modules
    }

    # Export all of the files associated with the project.  This means:
    #   1. Add a 'files' field to the project data
    #   2. Add the files themselves
    all_files = project.filestore.list_files()
    project_handle["files"] = [
      FILE_HANDLE(file_handle, project, None)
      for file_handle in all_files
    ]

    for file_handle in all_files:
      with file_handle.open(raw = True) as f:
        add_stream(
          f, 
          FILE_PATH.format(file_handle.identifier), 
          file_handle.size(raw = True)
        )

    add_buffer(json.dumps(project_handle).encode(), PROJECT_PATH)

def import_project(
    source_file: str,
    engine: VizierEngine
  ) -> ProjectHandle:
  with taropen(source_file, mode = 'r:gz') as archive:
    def read_or_fail(path: str) -> IO:
      io = archive.extractfile(path)
      if io is None:
        raise Exception("Corrupted export (missing {})".format(path))
      else:
        return io
    with read_or_fail(VERSION_PATH) as v:
      if int(v.read()) != 1:
        raise Exception("The export is too new")
    with read_or_fail(PROJECT_PATH) as p:
      project_serialized = json.load(p)
    
    project = engine.projects.create_project(
                properties = project_serialized["properties"]
              )
    for file_serialized in project_serialized['files']:
      with project.filestore.replace_file(
          identifier = file_serialized["id"],
          file_name  = file_serialized["name"],
          mimetype   = file_serialized.get("mimetype", None),
          encoding   = file_serialized.get("encoding", None)
        ) as out_f:
        with read_or_fail(FILE_PATH.format(file_serialized["id"])) as in_f:
          shutil.copyfileobj(in_f, out_f)
      
    project.viztrail.created_at = datetime.fromisoformat(
                                    project_serialized['createdAt']
                                  )
    # project_serialized['lastModifiedAt'] ## derived from branch properties
      
    modules = project_serialized['modules']
    for module_id in modules:
      module = project.viztrail.create_module(
        command = ModuleCommand(
            package_id = modules[module_id][labels.COMMAND][labels.COMMAND_PACKAGE],
            command_id = modules[module_id][labels.COMMAND][labels.COMMAND_ID],
            arguments  = modules[module_id][labels.COMMAND][labels.COMMAND_ARGS],
            packages   = None
        ),
        external_form = modules[module_id]['text'],
        identifier = modules[module_id][labels.ID],
        state = MODULE_PENDING,
        timestamp = ModuleTimestamp(
                      created_at = datetime.fromisoformat(
                        modules[module_id][labels.TIMESTAMPS][labels.CREATED_AT]
                      )
                    )
      )
      # We need to transform the module handle into the native representation
      # for the backend


      modules[module_id] = module
    
    for branch_serialized in project_serialized["branches"]:
      branch = project.viztrail.create_branch(
        provenance = BranchProvenance(
          source_branch = branch_serialized["sourceBranch"],
          workflow_id   = branch_serialized["sourceWorkflow"],
          module_id     = branch_serialized["sourceModule"],
          created_at    = datetime.fromisoformat(
                              branch_serialized["createdAt"]
                          )
        ), 
        properties = {
          x["key"] : x["value"] 
          for x in branch_serialized["properties"]
        },
        modules = None,
        identifier = branch_serialized["id"]
      )
      for workflow_serialized in branch_serialized["workflows"]:
        branch.append_workflow(
          modules = [
            modules[module_id]
            for module_id in workflow_serialized['modules']
          ],
          action = workflow_serialized['action'],
          command = ModuleCommand(
            package_id = workflow_serialized['packageId'],
            command_id = workflow_serialized['commandId'],
            arguments = [],
            packages = None
          ),
        )

    # Replace the original "default" branch with the imported default
    original_default = (
      project.viztrail.default_branch.identifier 
        if project.viztrail.default_branch is not None else None
    )
    project.viztrail.set_default_branch(project_serialized['defaultBranch'])
    if original_default not in [ b["id"] for b in project_serialized["branches"] ]:
      # tiny bit of safety, avoid a case where the original default branch is 
      # the same as one of the imported branches
      if original_default is not None:
        project.viztrail.delete_branch(original_default)

    return project
