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

from __future__ import division

#import base
import argparse
import os

import py4j.java_gateway as jgw

from spylon.simple import SimpleJVMHelpers

"""Global variables that are initialized for Mimir."""
#_mimirCallInterface = None
_gateway = None
_mimir = None
_jvmhelper = None


class MimirCallInterface(object):

    def __init__(self, gateway):
        self.gateway = gateway

    def callToPython(self, obj):

        print("JVM Who?")
        print(obj)
        self.gateway.jvm.System.out.println("jvm in your python")
        return "spylon"

    class Java:
        implements = ["mimir.PythonMimirCallInterface"]

def jvm_gateway(bindAddr, bindPort):
    gw = jgw.JavaGateway(
        jgw.GatewayClient(
            address=bindAddr,
            port=bindPort
        ),
        auto_convert=True,
        callback_server_parameters=jgw.CallbackServerParameters()
    )
    return gw

def simple_jvm_helper(jvm_gateway):
    j = SimpleJVMHelpers(jvm_gateway)
    return j

def started(sender, **kwargs):
    server = kwargs["server"]
    print("JavaGateway Server started")

def connection_started(sender, **kwargs):
    connection = kwargs["connection"]
    print("JavaGateway Server connection_started")

def connection_stopped(sender, **kwargs):
    connection = kwargs["connection"]
    print("JavaGateway Server ")

def stopped(sender, **kwargs):
    server = kwargs["server"]
    print("JavaGateway Server stopped")

def pre_shutdown(sender, **kwargs):
    server = kwargs["server"]
    print("JavaGateway Server pre_shutdown")

def post_shutdown(sender, **kwargs):
    server = kwargs["server"]
    print("JavaGateway Server post_shutdown")



def initialize():
    jgw.server_started.connect(started)
    global _gateway
    _gateway = jvm_gateway('127.0.0.1', 33388)
    global _jvmhelper
    _jvmhelper = simple_jvm_helper(_gateway)
    global _mimir
    _mimir = _jvmhelper.gateway.entry_point
    #global _mimirCallInterface
    #_mimirCallInterface = MimirCallInterface(_gateway)
    _mimir.registerPythonMimirCallListener(MimirCallInterface(_gateway))
    # Mimir lenses
    global _mimirLenses
    _mimirLenses = []
    _mimirLenses.append(_mimir.getAvailableLenses().split (','))
    # Adaptive schemas
    global _mimirAdaptiveSchemas
    _mimirAdaptiveSchemas = []
    _mimirAdaptiveSchemas.append(_mimir.getAvailableAdaptiveSchemas().split (','))

    jgw.server_stopped.connect(
        stopped,
        sender=_gateway.get_callback_server()
    )
    jgw.server_connection_started.connect(
        connection_started,
        sender=_gateway.get_callback_server()
    )
    jgw.server_connection_stopped.connect(
        connection_stopped,
        sender=_gateway.get_callback_server()
    )
    jgw.pre_server_shutdown.connect(
        pre_shutdown,
        sender=_gateway.get_callback_server()
    )
    jgw.post_server_shutdown.connect(
        post_shutdown,
        sender=_gateway.get_callback_server()
    )

    #base._mimir = _mimir
    #base._jvmhelper = _jvmhelper


def finalize():
    print("Shutting Down Gateway...")
    #global _mimir
    #_mimir.shutdown()
    global _gateway
    _gateway.shutdown()
