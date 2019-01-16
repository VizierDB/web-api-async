HATEOAS Links in API Resources
==============================

HATEOAS (Hypermedia as the Engine of Application State) is a constraint of the REST application architecture. A hypermedia-driven site provides information to navigate the site's REST interfaces dynamically by including hypermedia links with the responses.


Service Descriptor
------------------

self
doc
project:create
project:list



Project Listing
---------------

self
project:create



Project Descriptor
------------------

self
doc
home
project:delete
project:update
branch:create
file:upload



Branch Descriptor
-----------------

self
branch:delete
branch:head
branch:project
branch:update



WorkflowHandle
--------------
self
workflow:append
workflow:branch
workflow:append
workflow:cancel
file:upload


ModuleHandle
------------
self
module


FileHandle
----------

self
file:download
