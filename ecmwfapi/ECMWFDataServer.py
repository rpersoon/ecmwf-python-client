#
# (C) Copyright 2012-2013 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import json
import os
import time

from .api_connection import ApiConnection
from .exceptions import APIKeyFetchError


__version__ = '1.4.2'


class ECMWFDataServer:

    def __init__(self, url=None, key=None, email=None, verbose=False, log=None):
        if url is None or key is None or email is None:
            key, url, email = self._get_api_key_values()

        self.url = url
        self.key = key
        self.email = email
        self.verbose = verbose
        self.log = log

        self.trace("ECMWF API python library %s initialised" % __version__)

    def trace(self, m):
        if self.log:
            self.log(m)
        else:
            print("%s %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), m))

    def retrieve(self, request_data):
        connection = ApiConnection(self.url, "datasets/%s" % request_data['dataset'], self.email, self.key,
                                   self.trace, verbose=self.verbose)
        connection.transfer_request(request_data, request_data['target'])
        self.trace("Done.")

    def _get_api_key_values(self):
        """Get the API key from the environment or the '.ecmwfapirc' file. The environment is looked at first.

        Returns tuple with the key, url, and email forming our API key.
        Raises APIKeyFetchError when unable to get the API key from either the environment or the ecmwfapirc file
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
