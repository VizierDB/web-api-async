Packages
========

Packages are collections of commands that can be executed by workflow modules. Each package is registered with the Vizier API using a package declaration that is represented as a single dictionary object.



Package Declaration
-------------------

Each package is a collection of commands that are supported by a package. The package declaration specifies the list of supported commands and the parameters that these commands take. The general structure of the package declaration is as follows:

```
{
    'id': string,
    'name'*: string,
    'description'*: string,
    'commands': [
        {command-declaration}
    ]
}

* = optional
```

Each package has a unique identifier and an optional name and description. The package name is a short human-readable identifier and the description a longer summary for the package. Both, name and description are used by the Vizier UI when listing commands for notebook cells. If no description is given the package name will be used as the description. If no name is given the identifier will be used as package name.



Command Declaration
-------------------

Each command declaration specifies a command that is supported by the package. The general structure of the command declaration is as follows:

```
{
    'id': string,
    'name'*: string,
    'description'*: string,
    'format'*: [list of format expressions]
    'parameters': [
        {command-declaration}
    ]
}
```

Each command has an identifier that is unique among the commands in a package.

Arguments are defined as follows:

```
{
    "id": string,
    "datatype": string,
    "name": string,
    "label"*: string,
    "enum"+: [list of values],
    "required": bool,
    "index": int,
    "hidden": bool,
    'inRow'*: string
}

* = optional
+ = only for data type string or int
```



The following is a list of known data types for module arguments:

```
bool: Boolean value
colid: Positive integer referencing a column in the dataset
dataset: Name referencing a dataset
decimal: Numeric float or double value
fileid: Reference to a file on the file server
int: Integer value
list: List of records
pyCode: Multi-line text string (Python Code)
record: Grouping of components
rowid: Positive integer referencing a row in a dataset
scalar: Either string, int or float
string: Arbitrary character sequence
```

The parameter specifications are used to render input forms in the front end. The general structure of a parameter specification is:


```
"enum": [
    {
        "isDefault": true,
        "value": "A-Z"
    },
    "Z-A"
]
```



Format Expressions
------------------

String
{'type': 'var', 'value': string, 'format': []}
