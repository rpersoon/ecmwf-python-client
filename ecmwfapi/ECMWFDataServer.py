#
# (C) Copyright 2012-2013 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from .exceptions import *

import json
import os

from .api_connection import *
from .config import *
from .log import *


class ECMWFDataServer:

    def __init__(self, url=None, key=None, email=None, verbose=False, custom_log=None, custom_log_level=False):

        # Load the configuration file
        try:
            config.load(os.path.join(os.path.dirname(__file__), 'config.ini'))

        except ConfigError as e:
            raise DataServerError("Failed to load configuration file config.ini: %s" % e)

        # Initialise the logging
        if custom_log is None:
            self.log_level = True

            display_info_messages = config.get_boolean('display_info_messages', 'log')
            display_warning_messages = config.get_boolean('display_warning_messages', 'log')
            display_error_messages = config.get_boolean('display_error_messages', 'log')

            self.log_method = Log(display_info_messages, display_warning_messages, display_error_messages).log

        else:
            self.log_level = custom_log_level
            self.log_method = custom_log

        # If API credentials are not passed, try to retrieve the API credentials from the configuration file first
        if url is None or key is None or email is None:

            try:
                url = config.get('url', 'api')
                key = config.get('key', 'api')
                email = config.get('email', 'api')

            except ConfigError:
                pass

        # If API credentials where not in the configuration file either, retreive them from the ~/.ecmwfapirc file or
        # environment
        if url is None or key is None or email is None or url == 'none' or key == 'none' or email == 'none':

            try:
                [key, url, email] = self._get_api_key_values()

            except APIKeyFetchError as e:
                self.log("Failed to retrieve ECMWF API key: %s" % e, 'error')
                raise DataServerError("Failed to retrieve ECMWF API key from all sources: %s" % e)

        self.url = url
        self.key = key
        self.email = email

        self.log("ECMWF API python library %s initialised" % config.get('version', 'client'), 'info')

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

        if isinstance(request_data, dict):
            request_data = [request_data]

        elif not isinstance(request_data, list):
            self.log("The request data object should be a dictionary with the parameters or a list with multiple"
                     "dictionaries for multiple transfers", 'error')
            return

        if len(request_data) == 0:
            self.log("No requests were given", 'warning')
            return

        for [index, request] in enumerate(request_data):

            if len(request_data) == 1:
                self.log("Starting request", 'info')

            else:
                self.log("Starting request %i of %i" % (index + 1, len(request_data)), 'info')

            try:
                connection = ApiConnection(self.url, "datasets/%s" % request['dataset'], self.email, self.key,
                                           self.log)
                connection.transfer_request(request, request['target'])

            except ApiConnectionError as e:
                self.log("API connection error: %s" % e, 'error')

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
