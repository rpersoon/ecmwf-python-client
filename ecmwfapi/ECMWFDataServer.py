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
import queue
import threading
import time

from .api_connection import *
from .config import *
from .log import *


class ECMWFDataServer:

    def __init__(self, api_url=None, api_key=None, api_email=None, verbose=False, custom_log=None,
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
        if api_url is None or api_key is None or api_email is None:

            try:
                api_url = config.get('url', 'api')
                api_key = config.get('key', 'api')
                api_email = config.get('email', 'api')

            except ConfigError:
                pass

        # If API credentials where not in the configuration file either, retreive them from the ~/.ecmwfapirc file or
        # environment
        if api_url is None or api_key is None or api_email is None or api_url == 'none' or api_key == 'none' \
                or api_email == 'none':

            try:
                [api_key, api_url, api_email] = self._get_api_key_values()

            except APIKeyFetchError as e:
                self.log("Failed to retrieve ECMWF API key: %s" % e, 'error')
                raise DataServerError("Failed to retrieve ECMWF API key from all sources: %s" % e)

        self.api_url = api_url
        self.api_key = api_key
        self.api_email = api_email

        self.transfer_queue = None

        self.log("ECMWF API python library %s initialised" % config.get('version', 'client'), 'info')

    def log(self, message, level, request_id=None):
        """
        Passed to the transfer classes to provide logging, uses available logging method to log messages. This method is
        required for backwards compatibility and to support different custom logging functions, either with one (message
        only) or two (message and log level) parameters

        :param message: message to log
        :param level: log level, in [info, warning, error] for the default logging module
        :param request_id: optional request id to add to log messages
        """

        # Add the request id to the message if it is specified
        if isinstance(request_id, int):
            message = '(Request %s) %s' % (request_id, message)

        if self.log_level:
            self.log_method(message, level)
        else:
            self.log_method("[%s] %s" % (level, message))

    def retrieve(self, request_data):
        """
        Retrieve a dataset with the given parameters

        :param request_data: parameter list for transfer, or list of multiple parameter lists
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

            if len(request_data) > 1:
                self._process_request(request, index + 1)

            else:
                self._process_request(request, 1)

        self.log("ECMWFDataServer completed all requests", 'info')

    def retrieve_parallel(self, request_data, parallel_count=None):
        """
        Retrieve the given datasets in parallel - the different transfers are ran in parallel, but each individual
        dataset is downloaded sequentially

        :param request_data: parameter list for transfer, or list of multiple parameter lists
        :param parallel_count: maximum number of parallel / concurrent transfers
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

        # Determine parallel count
        if not isinstance(parallel_count, int):
            try:
                parallel_count = config.get_int('parallel_count', 'network')

            except ConfigError:
                self.log("No parallel count given and not set in configuration file either", 'error')

        self.transfer_queue = queue.Queue()

        # Launch the desired number of threads to process the requests
        self.log("Launching %s threads to process transfers" % parallel_count, 'info')

        threads = []
        for i in range(parallel_count):
            t = threading.Thread(target=self._parallel_worker)
            t.daemon = True
            t.start()
            threads.append(t)

        # Insert all transfers in the queue
        request_id = 1
        for transfer in request_data:
            self.transfer_queue.put([transfer, request_id])
            request_id += 1

            # Wait 3 seconds to not make too many API calls
            time.sleep(3)

        # Add stop indicators to the queue, 1 for each thread
        for i in range(parallel_count):
            self.transfer_queue.put(None)

        # Wait for all threads to complete
        for i in range(0, parallel_count):
            threads[i].join()

        self.log("ECMWFDataServer completed all requests in parallel", 'info')

    def _process_request(self, request_data, request_id):
        """
        Process the dataset transfer request. Used in both normal and parallel requests.

        :param request_data: parameter list for transfer
        :param request_id: identification of requests, used when multiple or parallel requests are initialised to inform
                           the user of the progress and which request is currently processed
        """

        try:
            disable_ssl_validation = config.get_boolean('disable_ssl_validation', 'network')

        except ConfigError:
            disable_ssl_validation = False

        if request_id is not None:
            self.log("Starting request %i" % request_id, 'info', request_id)

        else:
            self.log("Starting request", 'info', request_id)

        try:
            connection = ApiConnection(self.api_url, "datasets/%s" % request_data['dataset'], self.api_email,
                                       self.api_key, self.log, disable_ssl_validation=disable_ssl_validation,
                                       request_id=request_id)
            connection.transfer_request(request_data, request_data['target'])

        except ApiConnectionError as e:
            self.log("API connection error: %s" % e, 'error', request_id)

    def _parallel_worker(self):
        """
        Worker function to process parallel transfers, multiple instances launched in threads
        """

        if self.transfer_queue is None:
            self.log("No transfer queue specified for parallel transfer", 'error')

        while True:
            item = self.transfer_queue.get()

            # Non item indicates we have to stop
            if item is None:
                break

            elif not isinstance(item, list) or len(item) != 2:
                self.log("Invalid transfer item in queue", 'warning')
                continue

            self._process_request(item[0], item[1])

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
        """
        Obtain the API key from the environment

        :return: tuple with API key, url and e-mail
        """

        try:
            api_key = os.environ['ECMWF_API_KEY']
            api_url = os.environ['ECMWF_API_URL']
            api_email = os.environ['ECMWF_API_EMAIL']

        except KeyError:
            raise APIKeyFetchError("ERROR: Could not get the API key from the environment")

        return api_key, api_url, api_email

    @staticmethod
    def _get_api_key_from_rcfile():
        """
        Obtain the API key from the file ~/.ecmwfapirc

        :return: tuple with API key, url and e-mail
        """

        rc = os.path.normpath(os.path.expanduser("~/.ecmwfapirc"))

        try:
            with open(rc) as f:
                api_config = json.load(f)

        # Failed reading from file
        except IOError as e:
            raise APIKeyFetchError(str(e))

        # JSON decoding failed
        except ValueError:
            raise APIKeyFetchError("Missing or malformed API key in '%s'" % rc)

        # Unexpected error
        except Exception as e:
            raise APIKeyFetchError(str(e))

        try:
            key = api_config['key']
            url = api_config['url']
            email = api_config['email']

        except:
            raise APIKeyFetchError("Missing or malformed API key in '%s'" % rc)

        return key, url, email
