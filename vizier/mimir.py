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

import requests
import json
import os
from requests.exceptions import HTTPError

_mimir_url = os.environ.get('MIMIR_URL', 'http://127.0.0.1:8089/api/v2/')

class MimirError(Exception):
    def __init___(self,dErrorArguments):
        Exception.__init__(self, dErrorArguments)

PASSTHROUGH_ERRORS = set([
  "java.sql.SQLException",
  "org.apache.spark.sql.AnalysisException",
  "org.mimirdb.api.FormattedError"
])

def readResponse(resp):
    json_object = None
    try:
        resp.raise_for_status()
    except HTTPError as http_e:
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
    
def loadDataSource(file, infer_types, detect_headers, format = 'csv', human_readable_name = None, backend_options = [], dependencies = [], properties = {}, result_name = None):
    req_json ={
      "file": file,
      "format": format,
      "inferTypes": infer_types,
      "detectHeaders": detect_headers,
      "backendOption": backend_options,
      "dependencies": dependencies,
      "properties" : properties
    }
    if human_readable_name != None:
      req_json["humanReadableName"] = human_readable_name
    resp = readResponse(requests.post(_mimir_url + 'dataSource/load', json=req_json))
    return (resp['name'], resp['schema'])

def loadDataInline(schema, rows, result_name = None, human_readable_name = None, dependencies = [], properties = {}):
    req_json ={
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
    
#def feedbackCell(query, col, row, ack): 
    #req_json = 
    #resp = requests.post(_mimir_url + '', json=req_json)
    #return resp.json()
    
def explainRow(query, rowProv):  
    req_json = {
      "query": query,
      "row": rowProv,
      "col": 0
    }
    resp = readResponse(requests.post(_mimir_url + 'annotations/cell', json=req_json))
    return resp['reasons']
    
def explainCell(query, col, rowProv): 
    req_json = {
      "query": query,
      "row": rowProv,
      "col": col
    }
    resp = readResponse(requests.post(_mimir_url + 'annotations/cell', json=req_json))
    return resp['reasons']

def explainEverythingJson(query):
    req_json = {
      "query": query
    }
    resp = readResponse(requests.post(_mimir_url + 'annotations/all', json=req_json))
    return resp['reasons']     
    
def vistrailsQueryMimirJson(query, include_uncertainty, include_reasons, input = ''): 
    req_json = {
      "input": input,
      "query": query,
      "includeUncertainty": include_uncertainty,
      "includeReasons": include_reasons
    } 
    resp = readResponse(requests.post(_mimir_url + 'query/data', json=req_json))
    return resp

def getTable(table, columns = None, offset = None, offset_to_rowid = None, limit = None, include_uncertainty = None): 
    req_json = { "table" : table }
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

def countRows(view_name):
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

def getTableInfo(table):
    req_json = {
      "table": table
    }
    resp = readResponse(requests.post(_mimir_url + 'tableInfo', json=req_json))
    return (resp['schema'], resp['properties'])

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
  print(_mimir_url + "vizual/create")
  print(json.dumps(req_json))
  resp = readResponse(requests.post(_mimir_url + 'vizual/create', json=req_json))
  assert("name" in resp)
  assert("script" in resp)
  return resp


  
def getAvailableLensTypes():
    return requests.get(_mimir_url + 'lens').json()['lensTypes']
    
def getAvailableAdaptiveSchemas():
    return requests.get(_mimir_url + 'adaptive').json()['adaptiveSchemaTypes']

       
