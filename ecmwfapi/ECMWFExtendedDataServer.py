#
# (C) Copyright 2012-2013 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from .exceptions import *

import queue
import threading

from .ECMWFDataServer import *


class ECMWFExtendedDataServer(ECMWFDataServer):

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

        self.transfer_queue = None

        # Call super class init
        ECMWFDataServer.__init__(self, api_url, api_key, api_email, verbose, custom_log, custom_log_level)

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

        # Insert all transfers in the queue
        request_id = 1
        for transfer in request_data:
            self.transfer_queue.put([transfer, request_id])
            request_id += 1

        # Launch the desired number of threads to process the requests
        self.log("Launching %s threads to process transfers" % parallel_count, 'info')

        threads = []
        for i in range(parallel_count):
            t = threading.Thread(target=self._parallel_worker)
            t.daemon = True
            t.start()
            threads.append(t)

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
