#
# (C) Copyright 2012-2013 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from .exceptions import APIKeyFetchError

import json
import os

from .api_connection import *
from .log import *


__version__ = '1.4.2'


class ECMWFDataServer:

    def __init__(self, url=None, key=None, email=None, verbose=False, custom_log=None, custom_log_level=False):

        if url is None or key is None or email is None:
            try:
                [key, url, email] = self._get_api_key_values()

            except APIKeyFetchError as e:
                self.log("Failed to retrieve ECMWF API key: %s" % e, 'error')

        self.url = url
        self.key = key
        self.email = email
        self.verbose = verbose

        if custom_log is None:
            self.log_level = True
            self.log_method = Log().log
        else:
            self.log_level = custom_log_level
            self.log_method = custom_log

        self.log("ECMWF API python library %s initialised" % __version__, 'info')

    def log(self, message, level):
        """
        Passed to the transfer classes to provide logging, uses available logging method to log messages. This method is
        required for backwards compatibility and to support different custom logging functions, either with one (message
        only) or two (message and log level) parameters

        :param message: message to log
        :param level: log level, in [info, warning, error] for the default logging module
        """

        if self.log_level:
            self.log_method(message, level)
        else:
            self.log_method("[%s] %s" % (level, message))

    def retrieve(self, request_data):
        """
        Retrieve a dataset with the given parameters

        :param request_data: parameter list
        """

        try:
            connection = ApiConnection(self.url, "datasets/%s" % request_data['dataset'], self.email, self.key,
                                       self.log, verbose=self.verbose)
            connection.transfer_request(request_data, request_data['target'])

        except ApiConnectionError as e:
            self.log("API connection error: %s" % e, 'info')

        self.log("ECMWFDataServer done", 'info')

    def _get_api_key_values(self):
        """
        Get the API key from the environment or the '.ecmwfapirc' file. The environment is looked at first. Raises
        APIKeyFetchError when unable to get the API key from either the environment or the ecmwfapirc file

        :return: tuple with the key, url, and email forming our API key
        """
        try:
            key_values = self._get_api_key_from_environ()

        except APIKeyFetchError:
            try:
                key_values = self._get_api_key_from_rcfile()
            except APIKeyFetchError:
                raise

        return key_values

    @staticmethod
    def _get_api_key_from_environ():
        try:
            key = os.environ["ECMWF_API_KEY"]
            url = os.environ["ECMWF_API_URL"]
            email = os.environ["ECMWF_API_EMAIL"]
            return key, url, email
        except KeyError:
            raise APIKeyFetchError("ERROR: Could not get the API key from the environment")

    @staticmethod
    def _get_api_key_from_rcfile():
        rc = os.path.normpath(os.path.expanduser("~/.ecmwfapirc"))

        try:
            with open(rc) as f:
                config = json.load(f)

        except IOError as e:  # Failed reading from file
            raise APIKeyFetchError(str(e))
        except ValueError:  # JSON decoding failed
            raise APIKeyFetchError("ERROR: Missing or malformed API key in '%s'" % rc)
        except Exception as e:  # Unexpected error
            raise APIKeyFetchError(str(e))

        try:
            key = config["key"]
            url = config["url"]
            email = config["email"]
            return key, url, email

        except:
            raise APIKeyFetchError("ERROR: Missing or malformed API key in '%s'" % rc)
