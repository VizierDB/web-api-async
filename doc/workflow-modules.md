# Specification of Modules in Vizier Workflows

Modules in Vizier workflows are defined by Json objects that are part of HTTP requests. The general schema for module specifications is:

```
type: Module type
id: Type-specific identifier
arguments: Json object containing module inputs
```

The **type** element specifies the module type, i.e., the interpreter (or engine) that will execute the command. We currently support the three different module types *mimir*, *python*, and *vizual*. The **identifier** element contains a module type specific identifier that further specifies the command that is executed by the module. The **arguments** object contains the input arguments for the module.

## Module Listings

The Vizier Web API provides a listing of available modules. The elements in this listing follow the same schema as the module specifications in HTTP request with the main difference that each input argument contains a parameter definition of the following format:

```
id: The argument identifier
name: A printable argument name, e.g., for display in the UI
datatype: Definition of the argument type (see list of possible values below)
required: Boolean flag indicating whether the argument is mandatory (true) or optional (false)
values: Optional list of possible values
parent: Optional reference to parent object when having nested structure
```

The following is a list of known data types for module arguments:

```
asRow: List of components
bool: Boolean value
colid: Positive integer referencing a column in the dataset
datasetid: Name referencing a dataset
decimal: Numeric float or double value
fileid: Reference to a file on the file server
group: Indicate nesting of components
int: Integer value
pyCode: Multi-line text string (Python Code)
rowid: Positive integer referencing a row in a dataset
string: Arbitrary character sequence
```
The **fileid** argument has the following structure:

```
fileid: string
filename: string or url: string
```

If a *filename* is given this indicates that the file has been uploaded from local disk. A *url* indicates that the file was downloaded from the specified Url.

When submitting a module specification as part of a HTTP request each element in **arguments** is expected to contain pairs of argument identifier and (primitive) values. If an argument definition contains a list of elements the request is expected to submit a list of Json objects that contain one identifier and value for each object in the argument specification.


## Supported Modules
 The following is a list of supported commands showing the identifier of their expected arguments.


### Mimir

#### Key Repair Lens

```
type: 'mimir'
id: 'KEY_REPAIR'
arguments:
    dataset: 'Dataset name'
    column: 'Column index, name, or label'
    makeInputCertain: 'Flag indicating whether to make input certain'
```

#### Missing Key Lens

```
type: 'mimir'
id: 'MISSING_KEY'
arguments:
    dataset: 'Dataset name'
    column: 'Column index, name, or label'
    missingOnly: 'Optional Boolean MISSING_ONLY parameter'
    makeInputCertain: 'Flag indicating whether to make input certain'
```

#### Missing Value Lens

```
type: 'mimir'
id: 'MISSING_VALUE'
arguments:
    dataset: 'Dataset name'
    column: 'Column index, name, or label'
    constraint: 'Optional value constraint'
    makeInputCertain: 'Flag indicating whether to make input certain'
```

#### Picker Lens

```
type: 'mimir'
id: 'PICKER'
arguments:
    dataset: 'Dataset name'
    schema:
        - pickFrom: 'List of column names'
          pickAs: 'List of column names'
    makeInputCertain: 'Flag indicating whether to make input certain'
```

#### Schema Matching Lens

```
type: 'mimir'
id: 'SCHEMA_MATCHING'
arguments:
    dataset: 'Dataset name'
    schema:
        - column: 'Column index, name, or label'
          type: 'Data type name'
    makeInputCertain: 'Flag indicating whether to make input certain'
```

#### Type Inference Lens

```
type: 'mimir'
id: 'TYPE_INFERENCE'
arguments:
    dataset: 'Dataset name'
    percentConform: 'Percent that conforms'
    makeInputCertain: 'Flag indicating whether to make input certain'
```

### Python

#### Execute Python Code

```
type: 'python'
id: 'CODE'
arguments:
    source: 'Python code'
```

### VizUAL

#### Delete Column

```
type: 'vizual'
id: 'DELETE_COLUMN'
arguments:
    dataset: 'Dataset name'
    column: 'Column identifier'
```

#### Delete Row

```
type: 'vizual'
id: 'DELETE_ROW'
arguments:
    dataset: 'Dataset name'
    row: 'Row index'
```

#### Drop Dataset

```
type: 'vizual'
id: 'DROP_DATASET'
arguments:
    dataset: 'Dataset name'
```

#### Insert Column

```
type: 'vizual'
id: 'INSERT_COLUMN'
arguments:
    dataset: 'Dataset name'
    name: 'Column name'
    position: 'Index for new column'
```

#### Insert Row

```
type: 'vizual'
id: 'INSERT_ROW'
arguments:
    dataset: 'Dataset name'
    position: 'Index for new row'
```

#### Load Dataset

```
type: 'vizual'
id: 'LOAD'
arguments
    file: {fileid: 'Unique file identifier', 'filename': optional, 'url': optional}
    name: 'Dataset name'
```

#### Move Column

```
type: 'vizual'
id: 'MOVE_COLUMN'
arguments:
    dataset: 'Dataset name'
    column: 'Column identifier'
    position: 'Target index for column'
```

#### Move Row

```
type: 'vizual'
id: 'MOVE_ROW'
arguments:
    dataset: 'Dataset name'
    row: 'Row index'
    position: 'Target index for row'
```

#### Rename Column

```
type: 'vizual'
id: 'RENAME_COLUMN'
arguments:
    dataset: 'Dataset name'
    column: 'Column identifier'
    name: 'New column name'
```

#### Rename Dataset

```
type: 'vizual'
id: 'RENAME_DATASET'
arguments:
    dataset: 'Name of existing dataset'
    name: 'New dataset name'
```

#### Update Cell

```
type: 'vizual'
id: 'UPDATE_CELL'
arguments:
    dataset: 'Dataset name'
    column: 'Column identifier'
    row: 'Row index'
    value: 'New cell value'
```
