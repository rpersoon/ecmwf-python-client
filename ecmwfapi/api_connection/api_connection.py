#
# (C) Copyright 2012-2013 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from .exceptions import ApiConnectionError

import json
import time

from urllib.request import urlopen

from ecmwfapi import http


class ApiConnection(object):

    def __init__(self, url, service, email, key, log, quiet=False, verbose=False, report_news=True):
        self.api_url = url
        self.api_email = email
        self.api_key = key
        self.service = service
        self.log = log
        self.quiet = quiet
        self.verbose = verbose
        self.retry = 5
        self.location = None
        self.done = False
        self.value = True
        self.message_offset = 0
        self.status = None

        self.log("Connecting to ECMWF API at %s" % self.api_url)

        # Retrieve user details
        user = self._api_request('%s/who-am-i' % self.api_url)[1]
        self.log("Registered as %s" % user['full_name'] or "user '%s'" % user['uid'])

        # Display the news if requested and if available
        if report_news:
            news = self._api_request('%s/%s/news' % (self.api_url, self.service))[1]
            for item in news['news'].split("\n"):
                if len(item) > 0:
                    self.log('News: ' + item)

    def transfer_request(self, request, target=None):

        status = None

        content = self._api_request('%s/%s/requests' % (self.api_url, self.service), 'POST', request)[1]
        self.log('Request submitted')
        self.log('Request id: ' + content['name'])

        if content['status'] != status:
            status = content['status']
            self.log("Request is %s" % status)

        while not self.done:
            if content['status'] != status:
                status = content['status']
                self.log("Request is %s" % status)

            self.log("Sleeping %s second(s)" % self.retry)
            time.sleep(self.retry)

            content = self._api_request(self.location, 'GET')[1]
            if content['status'] == 'complete':
                self.done = True

        if self.status != status:
            status = self.status
            self.log("Request is %s" % status)

        result = content

        if target:
            file = open(target, "wb")

            time_start = time.time()

            # Transfer the dataset using the robust file transfer
            transfer_size = http.robust_get_file(result['href'], file)

            time_end = time.time()

            if time_end > time_start:
                self.log("Transfer rate %s/s" % self._bytename(transfer_size / (time_end - time_start)))

            file.flush()
            file.close()

        # Try to delete the file at the API. Ignore exceptions as it does not have any impact.
        try:
            self._api_request(self.location, 'DELETE')

        except ApiConnectionError:
            pass

    def _api_request(self, url, request_type='GET', payload=None):

        headers = {"Accept": "application/json", "From": self.api_email, "X-ECMWF-KEY": self.api_key}

        # Construct API request URL
        url = '%s/?offset=%d&limit=500' % (url, self.message_offset)

        if request_type == 'GET':
            [headers, content] = http.get_request(url, headers)

        elif request_type == 'POST':

            # Verify that a payload was given
            if not payload:
                raise ApiConnectionError("No payload given with POST request to %s" % url)

            data = json.dumps(payload).encode('utf-8')
            [headers, content] = http.post_request(url, data, headers)

        elif request_type == 'DELETE':
            [headers, content] = http.delete_request(url, headers)

        else:
            raise ApiConnectionError("Unknown API request type %s" % request_type)

        # Decode the response
        try:
            content_raw = content.decode('utf-8')
            content = json.loads(content_raw)

        except (LookupError, ValueError, json.decoder.JSONDecodeError) as e:
            raise ApiConnectionError("Failed to decode result: %s" % str(e))

        # Check for any errors in the response
        if 'error' in content:
            raise ApiConnectionError("ECMWF API reported error: %s" % content['error'])

        # Print any new messages reported by the API
        if 'messages' in content:
            for message in content['messages']:
                print('API message: %s' % message)
                self.message_offset += 1

        # Update the retry period if specified
        try:
            self.retry = int(headers['retry-after'])

        except KeyError:
            pass

        if headers.status in (201, 202):
            self.location = headers['location']

        return [headers, content]

    def _transfer(self, url, path, size):
        self.log("Transferring %s into %s" % (self._bytename(size), path))
        self.log("From %s" % (url, ))

        start = time.time()

        http = urlopen(url)
        f = open(path, "wb")

        total = 0
        block = 1024 * 1024
        while True:
            chunk = http.read(block)
            if not chunk:
                break
            f.write(chunk)
            total += len(chunk)

        f.flush()
        f.close()

        end = time.time()

        header = http.info()
        length = header.get("Content-Length")
        if length is None:
            self.log("Warning: Content-Length missing from HTTP header")
        if end > start:
            self.log("Transfer rate %s/s" % self._bytename(total / (end - start)), )

        return total

    @staticmethod
    def _bytename(size):
        prefix = {'': 'K', 'K': 'M', 'M': 'G', 'G': 'T', 'T': 'P', 'P': 'E'}
        l = ''
        size *= 1.0
        while 1024 < size:
            l = prefix[l]
            size /= 1024
        s = ""
        if size > 1:
            s = "s"
        return "%g %sbyte%s" % (size, l, s)
