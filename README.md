# OpenDXL Ruby Client

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://travis-ci.org/opendxl-community/opendxl-client-ruby.png?branch=master)](https://travis-ci.org/opendxl-community/opendxl-client-ruby)

## Overview

The OpenDXL Ruby Client enables the development of applications that connect to
the
[McAfee Data Exchange Layer](http://www.mcafee.com/us/solutions/data-exchange-layer.aspx)
messaging fabric for the purposes of sending/receiving events and
invoking/providing services.

## Tested Ruby Versions

The code may work on all Ruby MRI (CRuby) 2.x versions but has only been
tested with Ruby MRI (CRuby) versions 2.3.x and 2.4.x.

The code is known to encounter problems with [JRuby](https://www.jruby.org) 9k
(last tested with JRuby 9.2.0.0).

* The client can intermittently hang on attempts to receive incoming DXL
  messages.

  This has been seen most frequently when a significant number of incoming and
  outgoing DXL messages are in process. An issue has been raised for this to
  the njh/ruby-mqtt project [here](https://github.com/njh/ruby-mqtt/pull/104).

* Socket-level exceptions may be raised and not handled properly when
  connections are closed while DXL messages are still in flight from another
  thread.

## Bugs / Limitations

No releases (i.e., to [RubyGems](https://rubygems.org/)) have been done for
this project yet. In addition to the issues seen when running under JRuby 9k,
here is a list of known current limitations:

* Certificate path settings in the dxlclient.config (BrokerCertChain, CertFile,
  and PrivateKey) currently require a fully-qualified path.

* When a service id is included in a request message, the request message is
  inappropriately routed to request callbacks which match the request topic
  but do not match the service id.

* No support exists yet for client configuration provisioning via a command
  line tool, as can be done with the
  [OpenDXL Python Client](https://opendxl.github.io/opendxl-client-python/pydoc/basiccliprovisioning.html).

* The [add_topics](https://opendxl.github.io/opendxl-client-python/pydoc/dxlclient.service.html#dxlclient.service.ServiceRegistrationInfo.add_topics)
  method from the OpenDXL Python client has not been implemented yet.

## Other To-Dos

* Produce API-level (probably [YARD-based](https://yardoc.org/)) docs

* Port some additional test suites over from the Python/JavaScript OpenDXL
  test suites, including:

  * Message serialization unit tests (e.g., binary/non-ASCII and
    'other fields')

  * Broker service registry tests

* Add Rakefile and tasks for running tests, building docs, building packages
  per the typical OpenDXL client packaging approach, etc.

## LICENSE

Copyright 2018

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
