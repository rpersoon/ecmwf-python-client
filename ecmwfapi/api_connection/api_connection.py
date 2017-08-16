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


from .exceptions import ApiConnectionError

import json
import sys
import time

from ecmwfapi import custom_http


class ApiConnection(object):

    def __init__(self, api_url, api_service, api_email, api_key, log, report_news=True, disable_ssl_validation=False,
                 request_id=None):
        """
        :param api_url: ECMWF API url
        :param api_service: the service that is called at the API
        :param api_email: e-mail address used to register for the API
        :param api_key: authentication API key
        :param log: the logging method used. Should accept 2 parameters: the message itself and the logging level, which
            can be one of [info, warning, error]
        :param report_news: whether to output news messages from the API
        """

        self.api_url = api_url
        self.api_email = api_email
        self.api_key = api_key
        self.api_service = api_service
        self.log = log
        self.retry = 5
        self.location = None
        self.done = False
        self.value = True
        self.message_offset = 0
        self.status = None
        self.disable_ssl_validation = disable_ssl_validation
        self.request_id = request_id

        self.log("Connecting to ECMWF API at %s" % self.api_url, 'info', self.request_id)

        # Retrieve user details
        user = self._api_request('%s/who-am-i' % self.api_url)[1]
        self.log("Registered as %s" % user['full_name'] or "user '%s'" % user['uid'], 'info', self.request_id)

        # Display the news if requested and if available
        if report_news:
            news = self._api_request('%s/%s/news' % (self.api_url, self.api_service))[1]
            for item in news['news'].split("\n"):
                if len(item) > 0:
                    self.log("News: %s" % item, 'info', self.request_id)

    def transfer_request(self, request, target=None):
        """
        Transfer a dataset

        :param request: dictionary with request data
        :param target: location to write data to. Outputs to stdout if target == None
        """

        status = None

        content = self._api_request('%s/%s/requests' % (self.api_url, self.api_service), 'POST', request)[1]
        self.log("Request submitted", 'info', self.request_id)
        self.log("Request id: %s" % content['name'], 'info', self.request_id)

        if content['status'] != status:
            status = content['status']
            self.log("Request is %s" % status, 'info', self.request_id)

        while not self.done:
            if content['status'] != status:
                status = content['status']
                self.log("Request is %s" % status, 'info', self.request_id)

            time.sleep(self.retry)

            content = self._api_request(self.location, 'GET')[1]
            if content['status'] == 'complete':
                self.done = True

        if self.status != status:
            status = self.status
            self.log("Request is %s" % status, 'info', self.request_id)

        result = content

        if target:
            if target:
                file = open(target, "wb")
            else:
                file = sys.stdout

            time_start = time.time()

            # Transfer the dataset using the robust file transfer
            transfer_size = custom_http.robust_get_file(result['href'], file,
                                                        disable_ssl_validation=self.disable_ssl_validation)

            time_end = time.time()

            if time_end > time_start:
                self.log("Transfer rate %s/s" % self._bytename(transfer_size / (time_end - time_start)), 'info',
                         self.request_id)

            file.flush()
            file.close()

        # Try to delete the file at the API. Ignore exceptions as it does not have any impact.
        try:
            self._api_request(self.location, 'DELETE')

        except ApiConnectionError:
            pass

    def _api_request(self, url, request_type='GET', payload=None):
        """
        Make a request at the ECMWF API. Retries in case of errors.

        :param url: URL to call
        :param request_type: request type, one of [GET, POST, DELETE]
        :param payload: request payload (only applicable to POST requests)
        :return: tuple with response headers and content
        """

        request_tries = 0
        request_succeeded = False
        headers = None
        content = None

        request_headers = {
            'Accept': "application/json",
            'From': self.api_email,
            'X-ECMWF-KEY': self.api_key
        }

        # Construct API request URL
        url = "%s/?offset=%d&limit=500" % (url, self.message_offset)

        while request_tries < 7 and not request_succeeded:
            try:
                if request_type == 'GET':
                    [headers, content] = custom_http.get_request(url, request_headers, timeout=30,
                                                                 disable_ssl_validation=self.disable_ssl_validation)

                elif request_type == 'POST':

                    # Verify that a payload was given
                    if not payload:
                        raise ApiConnectionError("No payload given with POST request to %s" % url)

                    data = json.dumps(payload).encode('utf-8')
                    [headers, content] = custom_http.post_request(url, data, request_headers, timeout=30,
                                                                  disable_ssl_validation=self.disable_ssl_validation)

                elif request_type == 'DELETE':
                    [headers, content] = custom_http.delete_request(url, request_headers, timeout=30,
                                                                    disable_ssl_validation=self.disable_ssl_validation)

                else:
                    raise ApiConnectionError("Unknown API request type %s" % request_type)

                request_succeeded = True

            except custom_http.CustomHttpError as e:
                self.log("Api request failed: %s" % e, 'warning', self.request_id)
                request_tries += 1

                # Wait at least a second before the next try
                time.sleep(1)

        if not request_succeeded:
            raise ApiConnectionError("Failed to complete API request")

        # Decode the response
        try:
            content_raw = content.decode('utf-8')
            content = json.loads(content_raw)

        except (LookupError, ValueError, Exception) as e:
            raise ApiConnectionError("Failed to decode result: %s" % str(e))

        # Check for any errors in the response
        if 'error' in content:
            raise ApiConnectionError("API reported error: %s" % content['error'])

        # Print any new messages reported by the API
        if 'messages' in content:
            for message in content['messages']:
                self.log("API message: %s" % message, 'info', self.request_id)
                self.message_offset += 1

        # Update the retry period if specified
        try:
            self.retry = int(headers['retry-after'])

        except KeyError:
            pass

        if headers.status in (201, 202):
            self.location = headers['location']

        return [headers, content]

    @staticmethod
    def _bytename(size):
        """
        Convert bytes to printable value

        :param size: size in bytes
        :return: printable value corresponding to given bytes
        """

        prefix = {'': 'K', 'K': 'M', 'M': 'G', 'G': 'T', 'T': 'P', 'P': 'E'}
        l = ''
        size *= 1.0

        while 1024 < size:
            l = prefix[l]
            size /= 1024
        s = ''
        if size > 1:
            s = 's'
        return "%g %sbyte%s" % (size, l, s)
