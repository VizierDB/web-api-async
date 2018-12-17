###############################################################################
##
## Copyright (C) 2014-2016, New York University.
## Copyright (C) 2011-2014, NYU-Poly.
## Copyright (C) 2006-2011, University of Utah.
## All rights reserved.
## Contact: contact@vistrails.org
##
## This file is part of VisTrails.
##
## "Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions are met:
##
##  - Redistributions of source code must retain the above copyright notice,
##    this list of conditions and the following disclaimer.
##  - Redistributions in binary form must reproduce the above copyright
##    notice, this list of conditions and the following disclaimer in the
##    documentation and/or other materials provided with the distribution.
##  - Neither the name of the New York University nor the names of its
##    contributors may be used to endorse or promote products derived from
##    this software without specific prior written permission.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
## AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
## THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
## PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
## CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
## EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
## PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
## OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
## WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
## OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
## ADVISED OF THE POSSIBILITY OF SUCH DAMAGE."
##
###############################################################################
"""
Mimir package for VisTrails.
"""

from __future__ import division

from vistrails.core.packagemanager import get_package_manager

identifier = 'org.vistrails.vistrails.mimir'
name = 'Mimir'
version = '0.1.0'


def package_dependencies():
    pm = get_package_manager()
    tabledata_identifier = 'org.vistrails.vistrails.tabledata'
    if pm.has_package(tabledata_identifier):
        return [tabledata_identifier]
    else:
        return []
    
    
def package_requirements():
    from vistrails.core.requirements import require_python_module
    require_python_module('py4j')
    require_python_module('spylon')
