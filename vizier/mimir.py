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

_mimir_url = os.environ.get('MIMIR_URL', 'http://127.0.0.1:8089/api/v2/')

def createLens(dataset, params, type, make_input_certain, materialize, human_readable_name = None):
    req_json = {
      "input": dataset,
      "params": params,
      "type": type,
      "materialize": materialize,
      "humanReadableName": human_readable_name
    }
    resp = requests.post(_mimir_url + 'lens/create', json=req_json)
    return resp.json()

def createView(dataset, query):
    req_json = {
      "input": dataset,
      "query": query
    }
    resp = requests.post(_mimir_url + 'view/create', json=req_json)
    return resp.json()['viewName']
        
def createAdaptiveSchema(dataset, params, type):
    req_json = {
      "input": dataset,
      "params": params,
      "type": type
    } 
    resp = requests.post(_mimir_url + 'adaptive/create', json=req_json)
    return resp.json()['adaptiveSchemaName']
    
def vistrailsDeployWorkflowToViztool(x, name, type, users, start, end, fields, latlonfields, housenumberfield, streetfield, cityfield, statefield, orderbyfields):
    return ''
    
def loadDataSource(file, infer_types, detect_headers, format = 'csv', human_readable_name = None, backend_options = []):
    req_json ={
      "file": file,
      "format": format,
      "inferTypes": infer_types,
      "detectHeaders": detect_headers,
      "backendOption": backend_options
    }
    if human_readable_name != None:
      req_json["humanReadableName"] = human_readable_name
    resp = requests.post(_mimir_url + 'dataSource/load', json=req_json)
    return resp.json()['name']
    
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
    resp = requests.post(_mimir_url + 'annotations/feedback', json=req_json)
    
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
    resp = requests.post(_mimir_url + 'annotations/cell', json=req_json)
    return resp.json()['reasons']
    
def explainCell(query, col, rowProv): 
    req_json = {
      "query": query,
      "row": rowProv,
      "col": col
    }
    resp = requests.post(_mimir_url + 'annotations/cell', json=req_json)
    return resp.json()['reasons']

def explainEverythingJson(query):
    req_json = {
      "query": query
    }
    resp = requests.post(_mimir_url + 'annotations/all', json=req_json)
    return resp.json()['reasons']     
    
def vistrailsQueryMimirJson(query, include_uncertainty, include_reasons, input = ''): 
    req_json = {
      "input": input,
      "query": query,
      "includeUncertainty": include_uncertainty,
      "includeReasons": include_reasons
    } 
    resp = requests.post(_mimir_url + 'query/data', json=req_json)
    return resp.json()

def evalScala(source):
    req_json = {
      "source": source
    }
    resp = requests.post(_mimir_url + 'eval/scala', json=req_json)
    return resp.json()

def getSchema(query):
    req_json = {
      "query": query
    }
    resp = requests.post(_mimir_url + 'schema', json=req_json)
    return resp.json()['schema']
  
def getAvailableLansTypes():
    return requests.get(_mimir_url + 'lens').json()['lensTypes']
    
def getAvailableAdaptiveSchemas():
    return requests.get(_mimir_url + 'adaptive').json()['adaptiveSchemaTypes']

       
