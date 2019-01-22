***THIS DOCUMENT NEEDS TO BE REVISED***

The structure of HTTP request to run a module (i.e., insert, append or replace) is as follows:


```
{
  "packageId": "string",
  "commandId": "string",
  "arguments": {}
}
```

The package and command identifier are used to uniquely identify the command that is being executes. The arguments are command specific (see below).


The general structure for arguments is as follows. Each argument is represented by a dictionary containing *id* and *value* elements. For scalar argument the value is a scalar. For nested arguments the value is a list. For records the list is a list of dictionaries. Otherwise it is a list of lists of dictionaries.

Example
-------

```
{
    "command": "chart",
    "arguments": [
        {
            "id": "series",
            "value": [
                [
                    {
                        "id": "series_label",
                        "value": "My Series"
                    },
                    {
                        "id": "series_column",
                        "value": 1
                    }
                ],
                [
                    {
                        "id": "series_column",
                        "value": 2
                    }
                ]
            ]
        },
        {
            "id": "chart",
            "value": [
                {
                    "id": "chartType",
                    "value": "bar"
                },
                {
                    "id": "chartGrouped",
                    "value": false
                }
            ]
        },
        {
            "id": "name",
            "value": "My Chart"
        },
        {
            "id": "xaxis",
            "value": [
                {
                    "id": "xaxis_range",
                    "value": "0:10"
                }
            ]
        },
        {
            "id": "dataset",
            "value": "DS"
        }
    ],
    "package": "plot"
}
```
