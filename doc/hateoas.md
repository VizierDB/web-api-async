# HATEOAS Links in API Resources

The web API attempts to follow the [HATEOAS](https://en.wikipedia.org/wiki/HATEOAS) (Hypermedia as the Engine of Application State) constraint of the REST application architecture. Where possible, the API includes URLs to navigate API and interact with API resources. The URLs are contained in the *links* elements for the following API responses.



## Service Descriptor

The service descriptor is returned as result for the `/ (GET)` API route.  The included references are:

- **self**: Self-reference (*GET*)
- **api.doc**: Link to the API documentation (*GET*)
- **project.create**: Create a new project (*POST*)
- **project.list**: List all available projects (*GET*)



## Project Listing

The project listing is returned by the `/projects (GET)` route. The response contains the following references:

- **self**: Self-reference (*GET*)
- **project.create**: Create a new project (*POST*)

The response also contains a list of project descriptors, each with their own set of references.



## Project Descriptor

Project descriptors are returned in responses to the `/projects (GET, POST)` and `/projects/{projectId} (PUT)` routes. Each descriptor contains the following references:

- **self**: Self-reference (*GET*)
- **api.home**: URL for API service descriptor (*GET*)
- **api.doc**: Link to the API documentation (*GET*)
- **project.delete**: Delete a given project (*DELETE*)
- **project.update**: Update project properties, e.g., the project name (*PUT*)
- **branch.create**: Create a new project branch (*POST*)
- **file.upload**: Upload a new file (*POST*)



## Project Handle

The project handle is an extended project descriptor that is returned by the response to the `/projects/{projectId} (GET)` route. The project handle contains the same set of references as the project descriptor.



## Branch Descriptor

Branch descriptors are returned as part of project handles (`/projects/{projectId} (GET)`) and in the responses to the `/projects/{projectId}/branches (POST)` and `/projects/{projectId}/branches/{branchId} (PUT)` routes. Each descriptor contains the following references:
 
- **self**: Self-reference (*GET*)
- **branch.delete**: Delete a given branch (*DELETE*)
- **branch.head**: Get the workflow at the branch head (*GET*)
- **branch.update**: Update branch properties, e.g, the branch name (*PUT*)



## Branch Handle

A branch handle is an extended branch descriptor that is returned as response to the `/projects/{projectId}/branches/{branchId} (GET)` route. The branch handle contains the same references as the branch descriptor.



## Workflow Descriptor

Workflow descriptors are included in the branch handle that is returned as response to the `/projects/{projectId}/branches/{branchId} (GET)` route. The workflow descriptor contains the following references:

- **self**: Self-reference (*GET*)
- **workflow.append**: Append a new module to the workflow (*POST*)
- **workflow.branch**: The project branch that contains the workflow (*GET*)
- **workflow.project**: The project that contains the branch and the workflow (*GET*)
- **file.upload**: Upload a file to the file store of the associated project (*POST*)



## Workflow Handle

The workflow handle is an extended workflow descriptor that contains all the workflow modules. The workflow handle is returned as response to the routes `/projects/{projectId}/branches/{branchId}/head (GET)` and `/projects/{projectId}/branches/{branchId}/workflows/{workflowId} (GET)`. The workflow handle contains the following references (in addition to those references in the workflow descriptor):

- **workflow:cancel**: Cancel all running modules for an active workflow (*POST*)

Note that the *self* reference is not included in handles for workflows that are empty. The *workflow.cancel* reference is only included for workflows with active modules.



## Workflow Update Result

The workflow update result is a reduced workflow handle that contains only those modules that were affected by an append (`/projects/{projectId}/branches/{branchId}/head (POST)`), insert (`/projects/{projectId}/branches/{branchId}/head/modules/{moduleId} (POST)`), delete (`/projects/{projectId}/branches/{branchId}/head/modules/{moduleId} (DELETE)`), replace (`/projects/{projectId}/branches/{branchId}/head/modules/{moduleId} (PUT)`) or cancel (`/projects/{projectId}/branches/{branchId}/head/cancel (POST)` ) operation. The workflow update result contains the same references as the workflow handle.



## Module Handle

Module handles are returned as part of workflow handles, workflow update results and as the result of the module state polling request (`/projects/{projectId}/branches/{branchId}/head/modules/{moduleId} (GET)`). Module handles contain the following references:

- **self**: Self-reference (*GET*)
- **module.insert**: Insert a module in the workflow before the given module (*POST*)
- **module.delete**: Delete the workflow module (*DELETE*)
- **module.replace**: Replace the workflow module with a new command (*PUT*)



## Chart View

The chart view is returned as result of the `/projects/{projectId}/branches/{branchId}/workflows/{workflowId}/modules/{moduleId}/views/{viewId} (GET)` route. The chart view contains the following references:

- **self**: Self-reference (*GET*)



## Dataset Annotations

The set of dataset annotations is returned as response to the `/projects/{projectId}/datasets/{datasetId}/annotations (GET)` route. the response contains the following references:

- **annotations.update**: Update annotations for a given dataset (*POST*)



## Dataset Descriptor

Dataset descriptors are included in workflow handles and workflow update result. Dataset descriptors are also returned as responses to create dataset request (`/projects/{projectId}/datasets (POST)`) and the get descriptor request (`/projects/{projectId}/datasets/{datasetId}/descriptor (GET)`). A dataset descriptor contains the following references:

- **self**: Self-reference (*GET*)
- **fetch.all**
- **dataset.download**: Download dataset in CSV format (*GET*)
- **dataset.fetch**: Fetch all dataset rows (*GET*)
- **annotations.get**: Get list of all annotations for dataset (*GET*)
- **annotations.update**: Update annotations for dataset (*POST*)



## Dataset Handle

The dataset handle is an extended dataset descriptor that is returned as response to the the get dataset request (`/projects/{projectId}/datasets/{datasetId} (GET)`). The dataset handle contains all references of the dataset descriptor plus the following pagination references (if applicable):

- **page.first**: Get rows for the first page in the dataset (*GET*)
- **page.last**: Get rows for the last page in the dataset (*GET*)
- **page.next**: Get rows for the next page in the dataset (*GET*)
- **page.prev**: Get rows for the previous page in the dataset (*GET*)



## File Handle

The file handle is returned as result of the file upload request (`/projects/{projectId}/files (POST)`). The file handle includes the following references:

- **self**: Self-reference (*GET*)
- **file.download**: Download the uploaded file
