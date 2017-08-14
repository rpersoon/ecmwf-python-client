#
# (C) Copyright 2012-2013 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#
# (C) Copyright 2017 Ricardo Persoon.


from .exceptions import *

import json
import os

from .api_connection import *
from .config import *
from .log import *
from .ECMWFDataServer import ECMWFDataServer


class ECMWFService(ECMWFDataServer):

    def __init__(self, service, api_url=None, api_key=None, api_email=None, verbose=False, custom_log=None,
                 custom_log_level=False):
        """
        :param api_url: ECMWF API url
        :param api_key: authentication API key
        :param api_email: e-mail address used to register for the API
        :param verbose: not used, but here for backwards compatibility
        :param custom_log: custom logging function. If not specified, build-in logging class will be used
        :param custom_log_level: whether the provided custom logging function accepts a 2nd parameter with the log
            call that specifies the logging level. If not, only 1 parameter string is passed for logging
        """

        ECMWFDataServer.__init__(self, api_url, api_key, api_email, verbose, custom_log, custom_log_level)

        self.service = service

    def execute(self, request_data, target):
        """
        Retrieve a service with the given parameters

        :param request_data: parameter list
        :param target: Target of the request
        """

        if isinstance(request_data, dict):
            request_data = [request_data]

        elif not isinstance(request_data, list):
            self.log("The request data object should be a dictionary with the parameters or a list with multiple"
                     "dictionaries for multiple transfers", 'error')
            return

        if len(request_data) == 0:
            self.log("No requests were given", 'warning')
            return

        try:
            disable_ssl_validation = config.get_boolean('disable_ssl_validation', 'network')

        except ConfigError:
            disable_ssl_validation = False

        for [index, request] in enumerate(request_data):

            if len(request_data) == 1:
                self.log("Starting request", 'info')

            else:
                self.log("Starting request %i of %i" % (index + 1, len(request_data)), 'info')

            try:
                connection = ApiConnection(self.api_url, "services/%s" % self.service, self.api_email,
                                           self.api_key, self.log, disable_ssl_validation=disable_ssl_validation)
                connection.transfer_request(request, target)

            except ApiConnectionError as e:
                self.log("API connection error: %s" % e, 'error')

        self.log("ECMWFService done", 'info')

    def retrieve(self, request_data):
        """
        Placeholder to make sure that the retrieve method is not called on the ECMWFService
        """

        print("Please use the ECMWFDataServer object for the retrieve method")
