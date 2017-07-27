"""Python client for ECMWF web services API."""
#
# (C) Copyright 2012-2013 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from .ECMWFDataServer import ECMWFDataServer
from .ECMWFService import ECMWFService
from .ECMWFExtendedDataServer import ECMWFExtendedDataServer
from .log import Log
from .exceptions import DataServerError

__version__ = '1.4.2'
