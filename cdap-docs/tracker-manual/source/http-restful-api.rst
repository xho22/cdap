.. meta::
    :author: Cask Data, Inc.
    :description: Cask Tracker
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cask-tracker-http-restful-api:

=============================
Cask Tracker HTTP RESTful API
=============================

.. highlight:: console  

All Cask Tracker features are available via HTTP RESTful endpoints. It supports searching
of the *_auditLog* dataset, managing preferred tags, and querying metrics, using a set of
HTTP RESTful APIs. (See the :ref:`Reference Manual: HTTP RESTful API
<http-restful-api-introduction>` for details on the conventions used for these HTTP RESTful APIs.)

Searching the *_auditLog* Dataset
=================================

Searching Audit Log Messages
----------------------------
To search for audit log entries for a particular dataset, stream, or stream view, submit an HTTP GET request::

  GET /v3/namespaces/<namespace-id>/apps/_Tracker/services/TrackerService/methods/v1/auditlog/<type>/<name>[?startTime=<time>][&endTime=<time>][&offset=<offset>][&limit=<limit>]

where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``type``
     - One of ``dataset``, ``stream``, or ``stream_view``
   * - ``name``
     - Name of the ``entity-type``
   * - ``time`` *(optional)*
     - Time range defined by start (*startTime*, default ``0``) and end (*endTime*,
       default ``now``) times, where the times are either in milliseconds since the start of
       the Epoch, or a relative time, using ``now`` and times added to it. You can apply
       simple math, using ``now`` for the current time, ``s`` for seconds, ``m`` for
       minutes, ``h`` for hours and ``d`` for days. For example: ``now-5d-12h`` is 5 days
       and 12 hours ago.
   * - ``offset`` *(optional)*
     - The offset to start the results at for paging; default is ``0``
   * - ``limit`` *(optional)*
     - The maximum number of results to return in the results; default is ``10``
     
A successful query will return with the results as a field along with a count of the total
results available, plus the offset used for the set of results returned. This is to allow
for pagination through the results. Results are sorted so that the most recent audit event
in the time range is returned first.

.. highlight:: json  

If there are no results, an empty set of results will be returned (pretty-printed here for
display)::

  {
    "totalResults": 0,
    "results": [],
    "offset": 0
  }

Example:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X GET "http://localhost:10000/v3/namespaces/default/apps/_Tracker/services/TrackerService/methods/v1/auditlog/stream/who?limit=1&startTime=now-5d-12h&endTime=now-12h"

.. highlight:: json-ellipsis

Results (reformatted for display)::

  {
    "totalResults": 5,
    "results": [
      {
        "version": 1,
        "time": 1461266805472,
        "entityId": {
          "namespace": "default",
          "stream": "who",
          "entity": "STREAM"
        },
        "user": "unknown",
        "type": "METADATA_CHANGE",
        "payload": {
          "previous": {
            "SYSTEM": {
              "properties": {
                "creation-time": "1461266804916",
                "ttl": "9223372036854775807"
              },
              "tags": [
                "who"
              ]
            }
          },
          "additions": {
            "SYSTEM": {
              "properties": {
                "schema": "{\"type\":\"record\",\"name\":\"stringBody\",\"fields\":[{\"name\":\"body\",\"type\":\"string\"}]}"
              },
              "tags": []
            }
          },
          "deletions": {
            "SYSTEM": {
              "properties": {},
              "tags": []
            }
          }
        }
      },
      ...
      {
        "version": 1,
        "time": 1461266805404,
        "entityId": {
          "namespace": "default",
          "stream": "who",
          "entity": "STREAM"
        },
        "user": "unknown",
        "type": "CREATE",
        "payload": {}
      }
    ],
    "offset": 0
  }

.. highlight:: console  

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Returns the audit log entries requested in the body of the response
   * - ``400 BAD REQUEST``
     - Returned if the input values are invalid, such as an incorrect date format, negative
       offsets or limits, or an invalid range. The response will include an appropriate error
       message.
   * - ``500 SERVER ERROR``
     - Unknown server error


Managing Preferred Tags
=======================
You can use the Cask Tracker HTTP Restful APIs for managing *preferred tags*: you can add,
remove, promote, and demote *user tags* as needed.

Retrieve Tags
-------------
To retrieve a list of all tags in the system, submit an HTTP GET request::

  GET /v3/namespaces/<namespace-id>/apps/_Tracker/services/TrackerService/methods/v1/tags[?type=<type>][&prefix=<prefix>]

where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``type`` *(optional)*
     - The type of tag to return, either ``user`` or ``preferred``
   * - ``prefix`` *(optional)*
     - Each tag returned will start with this prefix

A successful query will return a 200 response with the total number of each type of tag
matching the options as well as a list of the tags and the number of entities they are
attached to.

Example:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X GET "http://localhost:10000/v3/namespaces/default/apps/_Tracker/services/TrackerService/methods/v1/tags"

.. highlight:: json-ellipsis

Results (reformatted for display)::

  {
    "preferred": 2,
    "user": 2,
    "preferredTags": {
      "preferredTag1" : 5,
      "preferredTag2" : 1
    },
    "userTags": {
      "tag1": 1,
      "tag2": 3
    }
  }

.. highlight:: console  

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Returns the tags requested in the body of the response
   * - ``404 NOT FOUND``
     - Returned if the tag does not exist as a preferred tag
   * - ``500 SERVER ERROR``
     - Unknown server error

Validate Tags
-------------
To validate a list of tags (confirm that the tags conform to the CDAP :ref:`alphanumeric
extra extended character set <supported-characters>`) before adding them, submit an HTTP
POST request::

  POST /v3/namespaces/<namespace-id>/apps/_Tracker/services/TrackerService/methods/v1/tags/validate

where the payload is a JSON array of tags to validate:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``payload``
     - A JSON-formatted array of tags to validate

A successful query will return a 200 response with a message as to which tags are valid
and which are invalid.

Example:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X POST "http://localhost:10000/v3/namespaces/default/apps/_Tracker/services/TrackerService/methods/v1/tags/validate" \
  -d '["newtag","new Tag","inval!d"]'

.. highlight:: json-ellipsis

Results (reformatted for display)::

  {
    "valid": 1,
    "invalid": 2,
    "validTags": [
      "newtag"
    ],
    "invalidTags": [
      "new Tag",
      "inval!d"
    ]
  }

.. highlight:: console  

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Returns the validation of the tags requested in the body of the response
   * - ``500 SERVER ERROR``
     - Unknown server error

Promote or Demote Tags
----------------------
Use these endpoints to promote a user tag to a preferred tag (or demote back to a user tag)::

  POST /v3/namespaces/<namespace-id>/apps/_Tracker/services/TrackerService/methods/v1/tags/promote
  POST /v3/namespaces/<namespace-id>/apps/_Tracker/services/TrackerService/methods/v1/tags/demote

where the payload is a JSON array of tags to promote or demote:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``payload``
     - A JSON formatted array of tags to promote

A successful query will return a 200 response with a message telling you know which
tags are valid and promoted/demoted and which are invalid.

Example:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X POST "http://localhost:10000/v3/namespaces/default/apps/_Tracker/services/TrackerService/methods/v1/tags/promote" \
  -d '["newtag","new Tag"]'

.. highlight:: json-ellipsis

Results (reformatted for display)::

  {
    "valid": 1,
    "invalid": 1,
    "validTags": [
      "newtag"
    ],
    "invalidTags": [
      "new Tag"
    ]
  }

.. highlight:: console  

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Returns the result of the action requested in the body of the response
   * - ``500 SERVER ERROR``
     - Unknown server error

Delete a Preferred Tag
----------------------
To delete a preferred tag from the system, submit an HTTP DELETE request::

  DELETE /v3/namespaces/<namespace-id>/apps/_Tracker/services/TrackerService/methods/v1/tags/preferred?tag=<tag>

where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``tag``
     - The preferred tag you would like to  delete

A successful query will return a 200 response with an empty body

Example:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X DELETE "http://localhost:10000/v3/namespaces/default/apps/_Tracker/services/TrackerService/methods/v1/tags/preferred?tag=example"

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Returns the audit log entries requested in the body of the response
   * - ``404 NOT FOUND``
     - Returned if the tag does not exist as a preferred tag
   * - ``500 SERVER ERROR``
     - Unknown server error

Retrieve Tags For a Specific Entity
-----------------------------------
To retrieve the tags for a specific dataset, stream, or stream view, submit an HTTP GET request::

  GET /v3/namespaces/<namespace-id>/apps/_Tracker/services/TrackerService/methods/v1/tags/<entity-type>/<entity-name>

where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``entity-type``
     - One of ``dataset``, ``stream``, or ``stream_view``
   * - ``entity-name``
     - The name of the entity to list the tags for

A successful query will return a 200 response with a body containing a list of tags.

Example:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X GET "http://localhost:10000/v3/namespaces/default/apps/_Tracker/services/TrackerService/methods/v1/tags/stream/exampleStream"
  
.. highlight:: json-ellipsis

Results (reformatted for display)::

  {
    "preferred": 1,
    "user": 1,
    "preferredTags": {
      "preferredTag": 1
    },
    "userTags": {
      "prod": 2
    }
  }

.. highlight:: console  

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Returns the tags for the requested entity in the body of the response
   * - ``404 NOT FOUND``
     - Returned if the entity does not exist
   * - ``500 SERVER ERROR``
     - Unknown server error

Add Tags to a Specific Entity
-----------------------------
To add tags to a specific dataset, stream, or stream view, submit an HTTP POST request::

  POST /v3/namespaces/<namespace-id>/apps/_Tracker/services/TrackerService/methods/v1/tags/promote/<entity-type>/<entity-name>

where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``entity-type``
     - One of ``dataset``, ``stream``, or ``stream_view``
   * - ``entity-name``
     - The name of the entity to add the tags to
   * - ``payload``
     - The list of tags to add to the entity

A successful query will return a 200 response with no body.

Example:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X POST "http://localhost:10000/v3/namespaces/default/apps/_Tracker/services/TrackerService/methods/v1/tags/stream/exampleStream" \
  -d '["tag1","tag2"]'

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The tags were added successfully
   * - ``404 NOT FOUND``
     - Returned if the entity does not exist
   * - ``500 SERVER ERROR``
     - Unknown server error

Remove a Tag from a Specific Entity
-----------------------------------
To remove a specific tag from a specific dataset, stream, or stream view, submit an HTTP DELETE request::

  DELETE /v3/namespaces/<namespace-id>/apps/_Tracker/services/TrackerService/methods/v1/tags/delete/<entity-type>/<entity-name>?tagname=<tag>

where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``entity-type``
     - One of ``dataset``, ``stream``, or ``stream_view``
   * - ``entity-name``
     - The name of the entity
   * - ``tag``
     - The tag to remove from the entity


A successful query will return a 200 response with no body.

Example:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X POST "http://localhost:10000/v3/namespaces/default/apps/_Tracker/services/TrackerService/methods/v1/tags/stream/exampleStream?tagname=tag1"

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The tag was removed successfully
   * - ``404 NOT FOUND``
     - Returned if the entity does not exist
   * - ``500 SERVER ERROR``
     - Unknown server error


Querying Metrics
================

Retrieve the Top Entities Graph Data
------------------------------------
To retrieve the list of top entities accessing a dataset or all datasets, submit an HTTP
GET request::

  GET /v3/namespaces/<namespace-id>/apps/_Tracker/services/TrackerService/methods/v1/auditmetrics
    /top-entities/{type}[?limit=<limit>][&entityType=<entity-type>][&entityName=<entity-name>][&startTime=<start-time>][&endTime=<end-time>]

where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``type``
     - One of ``datasets``, ``programs``, or ``applications``
   * - ``limit`` *(optional)*
     - The number of results to return; default is 5
   * - ``entity-type`` *(optional)*
     - One of ``dataset``, ``stream``, or ``stream_view``
   * - ``entity-name`` *(optional)*
     - The name of the entity to retrieve the list for
   * - ``start-time`` *(optional)* and ``end-time`` *(optional)*
     - Time range defined by start (*startTime*, default ``0``) and end (*endTime*,
       default ``now``) times, where the times are either in milliseconds since the start of
       the Epoch, or a relative time, using ``now`` and times added to it. You can apply
       simple math, using ``now`` for the current time, ``s`` for seconds, ``m`` for
       minutes, ``h`` for hours and ``d`` for days. For example: ``now-5d-12h`` is 5 days
       and 12 hours ago.

A successful query will return a 200 response with a body containing the entities and
their values, suitable for displaying in a graph.

Example:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X GET "http://localhost:10000/v3/namespaces/default/apps/_Tracker/services/TrackerService/methods/v1/auditmetrics/top-entities/applications?end=now&limit=5&start=now-7d"

.. highlight:: json-ellipsis

Results (reformatted for display)::

  [
    {
      "entityName": "Application_1",
      "value": 20
    },
    {
      "entityName": "Application_2",
      "value": 12
    },
    {
      "entityName": "Application_3",
      "value": 10
    },
    {
      "entityName": "Application_4",
      "value": 9
    },
    {
      "entityName": "Application_5",
      "value": 8
    }
  ]

.. highlight:: console  


.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Returns the results in the body of the response
   * - ``404 NOT FOUND``
     - Returned if the entity does not exist
   * - ``500 SERVER ERROR``
     - Unknown server error

Retrieve "Time Since" Data
--------------------------
To retrieve a list of the "times since" that the last audit message of a type was
received, submit an HTTP GET request::

  GET /v3/namespaces/<namespace-id>/apps/_Tracker/services/TrackerService/methods/v1/auditmetrics
    /time-since?entityType=<entity-type>&entityName=<entity-name>

where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``entity-type``
     - One of ``dataset``, ``stream``, or ``stream_view``
   * - ``entity-name``
     - The name of the entity to list the times for

A successful query will return a 200 response with a body containing the audit message
types and the last time they were received, suitable for displaying in a table.

Example:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X GET "http://localhost:10000/v3/namespaces/default/apps/_Tracker/services/TrackerService/methods/v1/auditmetrics/time-since?entityType=stream&entityName=events"

.. highlight:: json

Results (reformatted for display)::

  {
    "truncate": 44,
    "read": 1247103,
    "metadata_change": 1247718
  }

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Returns the results in the body of the response
   * - ``404 NOT FOUND``
     - Returned if the entity does not exist
   * - ``500 SERVER ERROR``
     - Unknown server error

Retrieve the Audit Log Histogram Data
-------------------------------------
To retrieve the histogram data for audit logs, submit an HTTP GET request::

  GET /v3/namespaces/<namespace-id>/apps/_Tracker/services/TrackerService/methods/v1/auditmetrics
    /audit-histogram/?entityType=<entity-type>&entityName=<entity-name>[&startTime=<start-time>][&endTime=<end-time>]

where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``entity-type``
     - One of ``dataset``, ``stream``, or ``stream_view``
   * - ``entity-name``
     - The name of the entity to retrieve the data for
   * - ``start-time`` *(optional)* and ``end-time`` *(optional)*
     - Time range defined by start (*startTime*, default ``0``) and end (*endTime*,
       default ``now``) times, where the times are either in milliseconds since the start of
       the Epoch, or a relative time, using ``now`` and times added to it. You can apply
       simple math, using ``now`` for the current time, ``s`` for seconds, ``m`` for
       minutes, ``h`` for hours and ``d`` for days. For example: ``now-5d-12h`` is 5 days
       and 12 hours ago.

A successful query will return a 200 response with a body containing the audit log
histogram data, suitable for displaying in a graph.

Example:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X GET "http://localhost:10000/v3/namespaces/default/apps/_Tracker/services/TrackerService/methods/v1/auditmetrics/audit-histogram?entityType=stream&entityName=events"

.. highlight:: json

Results (reformatted for display)::

  {
    "results": [
      {
        "timestamp": 1471910400,
        "value": 6
      },
      {
        "timestamp": 1472083200,
        "value": 1
      }
    ],
    "bucketInterval": "DAY"
  }

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Returns the results in the body of the response
   * - ``404 NOT FOUND``
     - Returned if the entity does not exist
   * - ``500 SERVER ERROR``
     - Unknown server error

Retrieve Tracker Meter Data
---------------------------
To retrieve the tracker meter scores for a list of datasets and streams, submit an HTTP POST request::

  POST /v3/namespaces/<namespace-id>/apps/_Tracker/services/TrackerService/methods/v1/tracker-meter

where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``payload``
     - A JSON map of string to array where the keys are either ``streams`` or
       ``datasets`` and the values are arrays of the names of each type

A successful query will return a 200 response with a body containing the Tracker scores
for each entity requested.

Example:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X POST "http://localhost:10000/v3/namespaces/default/apps/_Tracker/services/TrackerService/methods/v1/auditmetrics/tracker-meter" \
  -d '{"datasets":["ds1","ds2","ds3","ds4"],"streams":["strm1","strm2","strm3","strm4"]}'

.. highlight:: json

Results (reformatted for display)::

  {
    "datasets": [
      {
        "name": "ds1",
        "value": 80
      }
    ],
    "streams": [
      {
        "name": "strm1",
        "value": 80
      },
      {
        "name": "strm2",
        "value": 90
      }
    ]
  }

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Returns the results in the body of the response
   * - ``404 NOT FOUND``
     - Returned if the entity does not exist
   * - ``500 SERVER ERROR``
     - Unknown server error
