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
        Exception.__init__(self,dErrorArguments)

def readResponse(resp):
    json_object = None
    try:
        resp.raise_for_status()
    except HTTPError as http_e:
        raise MimirError({ 'errorMessage': http_e })
    except Exception as e: 
        raise MimirError({ 'errorMessage': e })
    else:
        try:
            json_object = resp.json()
        except Exception as e: 
            raise MimirError({ 'errorMessage': e })
    try:
        if not json_object['errorType'] is None and not json_object['errorMessage'] is None:
            raise MimirError(json_object)
    except KeyError: 
        pass
    return json_object

def createLens(dataset, params, type, materialize, human_readable_name = None):
    req_json = {
      "input": dataset,
      "params": params,
      "type": type,
      "materialize": materialize,
      "humanReadableName": human_readable_name
    }
    resp = readResponse(requests.post(_mimir_url + 'lens/create', json=req_json))
    return resp

def createView(dataset, query):
    req_json = {
      "input": dataset,
      "query": query
    }
    resp = readResponse(requests.post(_mimir_url + 'view/create', json=req_json))
    return (resp['viewName'], resp['dependencies'])

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
    
def loadDataSource(file, infer_types, detect_headers, format = 'csv', human_readable_name = None, backend_options = [], dependencies = []):
    req_json ={
      "file": file,
      "format": format,
      "inferTypes": infer_types,
      "detectHeaders": detect_headers,
      "backendOption": backend_options,
      "dependencies": dependencies
    }
    if human_readable_name != None:
      req_json["humanReadableName"] = human_readable_name
    resp = readResponse(requests.post(_mimir_url + 'dataSource/load', json=req_json))
    return resp['name']
    
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

def countRows(view_name):
    sql = 'SELECT COUNT(1) FROM ' + view_name + ';'
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

def getSchema(query):
    req_json = {
      "query": query
    }
    resp = readResponse(requests.post(_mimir_url + 'schema', json=req_json))
    return resp['schema']

def createSample(inputds, mode_config, seed = None):
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
    "seed" : seed
  }
  resp = readResponse(requests.post(_mimir_url + 'view/sample', json=req_json))
  return resp['viewName']

  
def getAvailableLansTypes():
    return requests.get(_mimir_url + 'lens').json()['lensTypes']
    
def getAvailableAdaptiveSchemas():
    return requests.get(_mimir_url + 'adaptive').json()['adaptiveSchemaTypes']

       
