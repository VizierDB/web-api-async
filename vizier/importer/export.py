
import sys
import os
import json
from zipfile import ZipFile

from vizier.api.webservice.base import VizierApi
from vizier.config.app import AppConfig
from vizier.api.webservice import server

from vizier.api.serialize.project import PROJECT_HANDLE
from vizier.api.serialize.dataset import DATASET_DESCRIPTOR
from vizier.api.serialize.files import FILE_HANDLE
from vizier.engine.packages.base import DT_FILE_ID

config = AppConfig()
api = VizierApi(config, init=True)

FILE_PARAMETERS = set(
  (command.identifier, parameter['id'])
    for package in api.engine.packages.values()
    for command in package.commands.values()
    for parameter in command.parameters.values()
    if parameter['datatype'] == DT_FILE_ID
)

def export(project_id, zip_file):
  global api
  zip_file = ZipFile(file = zip_file, mode = 'w')

  #print("{}".format(project_id))
  #print("{}".format(api.projects)) # vizier.api.webservice.project.VizierProjectApi
  #print("{}".format(api.projects.projects)) # vizier.engine.project.cache.common.CommonProjectCache
  #print("{}".format(api.projects.projects.projects)) # dict
  #print("{}".format(api.projects.projects.projects[project_id])) # vizier.engine.project.base.ProjectHandle
  #print("{}".format(api.projects.projects.projects[project_id].viztrail)) # vizier.viztrail.objectstore.viztrail.OSViztrailHandle

  if project_id not in api.projects.projects.projects:
    raise("Project {} does not exist".format(project_id))

  project = api.projects.projects.projects[project_id]


  project_js = PROJECT_HANDLE(project = project, urls = api.urls)
  del project_js["links"]

  export_json(
      js = project_js,
      file = "project.json",
      zip_file = zip_file
    )

  datasets = set()
  files = set()

  for branch_handle in project.viztrail.list_branches():
    branch = api.branches.get_branch(
                project_id=project_id,
                branch_id=branch_handle.identifier
              )

    del branch["links"]
    for workflow in branch["workflows"]:
      del workflow["links"]

    export_json(
      js = branch,
      file = "branch/{}/branch.json".format(branch_handle.identifier),
      zip_file = zip_file
    )

    modules = set()

    for workflow_handle in branch["workflows"]:
      workflow = api.workflows.get_workflow(
        project_id=project_id,
        branch_id=branch_handle.identifier,
        workflow_id=workflow_handle["id"]
      )
      del workflow["links"]
      export_json(
        js = workflow,
        file = "branch/{}/workflow/{}.json".format(branch_handle.identifier, workflow_handle["id"]),
        zip_file = zip_file
      )
      for module in workflow["modules"]:
        modules.add(module["id"])

    for module_id in modules:
      module = api.workflows.get_workflow_module(
        project_id=project_id,
        branch_id=branch_handle.identifier,
        module_id=module_id
      )
      if module is not None:
        export_json(
          js = module,
          file = "branch/{}/module/{}.json".format(branch_handle.identifier, module_id),
          zip_file = zip_file
        )
        for dataset in module["datasets"]:
          datasets.add(dataset["id"])
        for dataset in module.get("dataobjects", []):
          datasets.add(dataset["id"])
        command_id = module['command']['commandId']
        for argument in module['command']["arguments"]:
          if (command_id, argument['id']) in FILE_PARAMETERS:
            files.add(argument['value']['fileid'])


  for dataset_id in datasets:
    dataset = project.datastore.get_dataset(dataset_id)
    dataset_js = DATASET_DESCRIPTOR(
      project = project,
      dataset = dataset,
      urls = api.urls
    )
    del dataset_js["links"]
    export_json(
      js = dataset_js,
      file = "dataset/{}.json".format(dataset_id),
      zip_file = zip_file
    )

  for file_id in files:
    file = api.files.get_file(project_id, file_id)

    file_js = FILE_HANDLE(project = project, f_handle = file, urls = api.urls)
    del file_js["links"]
    export_json(
      js = file_js,
      file = "file/{}.json".format(file_id),
      zip_file = zip_file
    )
    export_file(
      source = file.filepath,
      file = "file/{}.dat".format(file_id),
      zip_file = zip_file
    )


def export_json(js, file, zip_file):
  print(file)
  with zip_file.open(file, 'w') as f:
    js = json.dumps(js)
    f.write(js.encode())

def export_file(source, file, zip_file):
  print(file)
  zip_file.write(source, arcname = file)

if __name__ == "__main__":
  project_id = sys.argv[1]
  with open("{}.zip".format(project_id), "wb") as zip_file:
    export(project_id, zip_file)


