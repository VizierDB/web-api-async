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

"""Initialize the Python gateway to the JVM for access to Mimir datastore and
lenses.
"""
from typing import cast, Optional, List, Dict, Any, Tuple

import requests
import os
from requests.exceptions import HTTPError
from requests import Response
from vizier.datastore.annotation.base import DatasetCaveat

_mimir_url = os.environ.get('MIMIR_URL', 'http://127.0.0.1:8089/api/v2/')

class MimirError(Exception):
    def __init___(self,dErrorArguments):
        Exception.__init__(self, dErrorArguments)

PASSTHROUGH_ERRORS = set([
  "java.sql.SQLException",
  "org.apache.spark.sql.AnalysisException",
  "org.mimirdb.api.FormattedError"
])

def readResponse(resp: Response) -> Dict[str, Any]:
    json_object = None
    try:
        resp.raise_for_status()
    except HTTPError:
        raise MimirError({ 'errorMessage': "Internal Error [Mimir]: Got a {} error code.".format(resp.status_code) })
    except Exception as e: 
        raise MimirError({ 'errorMessage': "Internal Error [HTTP -> Mimir]: {}".format(e) })
    else:
        try:
            json_object = resp.json()
        except Exception as e: 
            raise MimirError({ 'errorMessage': "Internal Error [Parse Mimir Response]: {}".format(e) })
    try:
        if not json_object['errorType'] is None and not json_object['errorMessage'] is None:
            errorType = json_object.get("errorType", "Unknown")
            errorMessage = json_object.get("errorMessage", "Unknown")
            if errorType not in PASSTHROUGH_ERRORS:
                errorMessage = "Internal Error [Mimir]: {} / {}".format(errorType, errorMessage)
            json_object["errorType"] = errorType
            json_object["errorMessage"] = errorMessage
            raise MimirError(json_object)
    except KeyError: 
        pass
    return json_object

def createLens(dataset, params, type, materialize, human_readable_name = None, properties = {}):
    req_json = {
      "input": dataset,
      "params": params,
      "type": type,
      "materialize": materialize,
      "humanReadableName": human_readable_name,
      "properties" : properties
    }
    resp = readResponse(requests.post(_mimir_url + 'lens/create', json=req_json))
    return resp

def createView(dataset, query, properties = {}):
    depts = None
    if isinstance(dataset, dict):
        depts = dataset
    else:
        depts = {dataset:dataset}
    req_json = {
      "input": depts,
      "query": query,
      "properties" : properties
    }
    resp = readResponse(requests.post(_mimir_url + 'view/create', json=req_json))
    return (resp['name'], resp['dependencies'], resp['schema'], resp['properties'])

def createAdaptiveSchema(dataset, params, type):
    req_json = {
      "input": dataset,
      "params": params,
      "type": type
    } 
    resp = readResponse(requests.post(_mimir_url + 'adaptive/create', json=req_json))
    return resp['adaptiveSchemaName']
    
def vistrailsDeployWorkflowToViztool(x, name, type, users, start, end, fields, latlonfields, housenumberfield, streetfield, cityfield, statefield, orderbyfields):
    return ''
    
def loadDataSource(
      file: str, 
      infer_types: bool, 
      detect_headers: bool, 
      format: str = 'csv', 
      human_readable_name: Optional[str] = None, 
      backend_options: List[Dict[str,str]] = [], 
      dependencies: List[str] = [], 
      properties: Dict[str,Any] = {}, 
      result_name: Optional[str] = None,
      proposed_schema: List[Tuple[str,str]] = []
    ) -> Tuple[str,List[Dict[str,str]]]:
    req_json ={
      "file": file,
      "format": format,
      "inferTypes": infer_types,
      "detectHeaders": detect_headers,
      "backendOption": backend_options,
      "dependencies": dependencies,
      "properties" : properties,
      "proposedSchema" : [
        { "name" : col[0], "type" : col[1] } 
        for col in proposed_schema
      ]
    }
    if human_readable_name is not None:
      req_json["humanReadableName"] = human_readable_name
    resp = readResponse(requests.post(_mimir_url + 'dataSource/load', json=req_json))
    return (resp['name'], resp['schema'])

def loadDataInline(
      schema: Optional[List[Dict[str, str]]], 
      rows: List[List[Any]], 
      result_name: Optional[str] = None, 
      human_readable_name: Optional[str] = None, 
      dependencies: List[str] = [], 
      properties: Dict[str, Any] = {}
    ) -> Tuple[str, List[Dict[str, str]]]:
    req_json: Dict[str, Any] ={
      "schema": schema,
      "data": rows,
      "dependencies": dependencies,
      "properties" : properties,
    }
    if human_readable_name is not None:
      req_json["humanReadableName"] = human_readable_name
    if result_name is not None:
      req_json["resultName"] = result_name
    resp = readResponse(requests.post(_mimir_url + 'dataSource/inlined', json=req_json))
    return (resp['name'], resp['schema'])

    
def unloadDataSource(dataset_name, abspath, format='csv', backend_options = []):
    req_json ={
      "input":dataset_name,
      "file":abspath,
      "format": format,
      "backendOption": backend_options
    }
    resp = readResponse(requests.post(_mimir_url + 'dataSource/unload', json=req_json))
    return resp['outputFiles']
    
def repairReason(reasons, reasonIdx):
    return ''
    
def feedback(reasons, idx, ack, rvalue): 
    reason = reasons(idx)
    req_json = {
      "reason": reason,
      "idx": reason.idx,
      "ack": ack,
      "repairStr": rvalue
    } 
    resp = readResponse(requests.post(_mimir_url + 'annotations/feedback', json=req_json))
    return resp
    
#def feedbackCell(query, col, row, ack): 
    #req_json = 
    #resp = requests.post(_mimir_url + '', json=req_json)
    #return resp.json()
    
def explainRow(query: str, rowProv: Optional[str]) -> List[DatasetCaveat]: 
    req_json = {
      "query": query,
      "row": rowProv,
      "col": 0
    }
    resp = readResponse(requests.post(_mimir_url + 'annotations/cell', json=req_json))
    return [
      DatasetCaveat.from_dict(caveat)
      for caveat in resp['reasons']
    ]
    
def explainCell(query: str, col: Optional[str], rowProv: Optional[str]) -> List[DatasetCaveat]: 
    req_json = {
      "query": query,
      "row": rowProv,
      "col": col
    }
    resp = readResponse(requests.post(_mimir_url + 'annotations/cell', json=req_json))
    return [
      DatasetCaveat.from_dict(caveat)
      for caveat in resp['reasons']
    ]

def explainEverythingJson(query: str) -> List[DatasetCaveat]:
    req_json = {
      "query": query
    }
    resp = readResponse(requests.post(_mimir_url + 'annotations/all', json=req_json))
    return [
      DatasetCaveat.from_dict(caveat)
      for caveat in resp['reasons']
    ]

def vistrailsQueryMimirJson(
      query: str, 
      include_uncertainty: bool, 
      include_reasons: bool, 
      input: str = ''
    ) -> Dict[str, Any]: 
    req_json = {
      "input": input,
      "query": query,
      "includeUncertainty": include_uncertainty,
      "includeReasons": include_reasons
    } 
    resp = readResponse(requests.post(_mimir_url + 'query/data', json=req_json))
    return resp

def getTable(
      table: str, 
      columns: Optional[List[str]] = None, 
      offset: Optional[int] = None, 
      offset_to_rowid: Optional[str] = None, 
      limit: Optional[int] = None, 
      include_uncertainty: Optional[bool] = None): 
    req_json: Dict[str, Any] = { "table" : table }
    if columns is not None:
      req_json["columns"] = columns
    if offset is not None:
      if offset < 0:
        raise Exception("Invalid offset {}".format(offset))
      req_json["offset"] = offset
    if offset_to_rowid is not None:
      req_json["offset_to_rowid"] = offset_to_rowid
    if limit is not None:
      if limit < 0:
        raise Exception("Invalid offset {}".format(limit))
      req_json["limit"] = limit
    if include_uncertainty is not None:
      req_json["includeUncertainty"] = include_uncertainty

    resp = readResponse(requests.post(_mimir_url + 'query/table', json=req_json))
    return resp

def countRows(view_name: str) -> int:
    sql = 'SELECT COUNT(1) FROM ' + view_name 
    rs_count = vistrailsQueryMimirJson(sql, False, False)
    row_count = int(rs_count['data'][0][0])
    return row_count

def evalScala(inputs, source):
    req_json = {
      "input": inputs,
      "language": "scala",
      "source": source
    }
    resp = readResponse(requests.post(_mimir_url + 'eval/scala', json=req_json))
    return resp

def evalR(inputs, source):
    req_json = {
      "input": inputs,
      "language": "R",
      "source": source
    }
    resp = readResponse(requests.post(_mimir_url + 'eval/R', json=req_json))
    return resp

def getTableInfo(table: str) -> Tuple[List[Dict[str,str]], Dict[str, Any]]:
    req_json = {
      "table": table
    }
    resp = readResponse(requests.post(_mimir_url + 'tableInfo', json=req_json))
    return (
      cast(List[Dict[str,str]], resp['schema']), 
      cast(Dict[str,Any], resp['properties'])
    )

def getSchema(query):
    req_json = {
      "query": query
    }
    resp = readResponse(requests.post(_mimir_url + 'schema', json=req_json))
    return resp['schema']

def tableExists(tableName):
    req_json = {
      "query": 'SELECT * FROM ' + str(tableName)
    }
    try:
        resp = readResponse(requests.post(_mimir_url + 'schema', json=req_json))
        if resp['schema']:
            return True
        else:
            return False
    except:
        return False


def createSample(inputds, mode_config, seed = None, result_name = None, properties = {}):
  """
    Create a sample of dataset input according to the sampling mode
    configuration given in modejs.  See Mimir's mimir.algebra.sampling
    for more details.

    Parameters
    ----------
    inputds: string
        The internal name of the dataset to generate samples for
    mode_config: dictionary (see Mimir's mimir.algebra.sampling)
        The sampling process to use
    seed: long (optional)
        The seed value to use

    Returns
    -------
    string (the internal name of the generated view)
  """
  req_json = { 
    "source" : inputds,
    "samplingMode" : mode_config,
    "seed" : seed, 
    "properties" : properties,
    "resultName" : result_name
  }
  resp = readResponse(requests.post(_mimir_url + 'view/sample', json=req_json))
  return (resp['name'], resp['schema'])

def vizualScript(inputds, script, script_needs_compile = False, properties = {}):
  """
    Create a view that implements a sequence of vizual commands over a fixed input table

    Parameters
    ----------
    inputds: string
      The internal name of the dataset to apply the input script to
    script: list[dictionary] or dictionary
      The sequence of vizual commands to apply the input script to.  
      If not a list, the parameter will be assumed to be a singleton
      command and wrapped in a list.
    script_needs_compile: boolean
      Set to true if mimir should preprocess the script to provide more
      spreadsheet-like semantics (e.g., lazy evaluation of expression 
      cells)

    Returns
    -------
    dictionary of 
      - "name": The name of the created view 
      - "script": The compiled version of the script (or just script if 
                  script_needs_compile = False)
  """

  if type(script) is not list:
    script = [script]

  req_json = {
    "input" : inputds,
    "script" : script,
    # "resultName": Option[String],
    "compile": script_needs_compile, 
    "properties" : properties
  }
  # print(_mimir_url + "vizual/create")
  # print(json.dumps(req_json))
  resp = readResponse(requests.post(_mimir_url + 'vizual/create', json=req_json))
  assert("name" in resp)
  assert("script" in resp)
  return resp

def getBlob(identifier, expected_type = None):
  resp = requests.get(_mimir_url + 'blob/{}'.format(identifier))
  if resp.status_code != 200:
    raise MimirError(
      "Blob {} does not exist".format(identifier)
    )
  if expected_type is not None:
    actual_type = resp.headers.get('Content-Type', "text/plain")
    if expected_type != actual_type:
      raise MimirError(
        "Blob {} is of type {} and not {}".format(identifier, actual_type, expected_type)
      )
  return resp.content

def createBlob(identifier, blob_type, data):
  route = "blob"
  if identifier is not None:
    route += "/{}".format(identifier)
  resp = requests.put(_mimir_url + route+'?type={}'.format(blob_type), data = data)
  if resp.status_code != 200:
    raise MimirError(
      "Blob {} creation failed".format(identifier)
    )
  return resp.text
  
def getAvailableLensTypes():
    return requests.get(_mimir_url + 'lens').json()['lensTypes']
    
def getAvailableAdaptiveSchemas():
    return requests.get(_mimir_url + 'adaptive').json()['adaptiveSchemaTypes']

       
