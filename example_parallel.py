#!/usr/bin/env python
#
# (C) Copyright 2012-2013 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from ecmwfapi import ECMWFExtendedDataServer

# To run this example, you need an API key 
# available from https://api.ecmwf.int/v1/key/

server = ECMWFExtendedDataServer()

server.retrieve_parallel(
    [
        {
            "class": "s2",
            "dataset": "s2s",
            "date": "2015-01-01",
            "expver": "prod",
            "levtype": "sfc",
            "origin": "ecmf",
            "param": "165",
            "step": "0/to/1104/by/24",
            "stream": "enfo",
            "target": "test",
            "time": "00",
            "type": "cf",
        },
        {
            "class": "s2",
            "dataset": "s2s",
            "date": "2015-01-02",
            "expver": "prod",
            "levtype": "sfc",
            "origin": "ecmf",
            "param": "165",
            "step": "0/to/1104/by/24",
            "stream": "enfo",
            "target": "test",
            "time": "00",
            "type": "cf",
        },{
            "class": "s2",
            "dataset": "s2s",
            "date": "2015-01-03",
            "expver": "prod",
            "levtype": "sfc",
            "origin": "ecmf",
            "param": "165",
            "step": "0/to/1104/by/24",
            "stream": "enfo",
            "target": "test",
            "time": "00",
            "type": "cf",
        },{
            "class": "s2",
            "dataset": "s2s",
            "date": "2015-01-04",
            "expver": "prod",
            "levtype": "sfc",
            "origin": "ecmf",
            "param": "165",
            "step": "0/to/1104/by/24",
            "stream": "enfo",
            "target": "test",
            "time": "00",
            "type": "cf",
        },{
            "class": "s2",
            "dataset": "s2s",
            "date": "2015-01-05",
            "expver": "prod",
            "levtype": "sfc",
            "origin": "ecmf",
            "param": "165",
            "step": "0/to/1104/by/24",
            "stream": "enfo",
            "target": "test",
            "time": "00",
            "type": "cf",
        },
    ]
)
