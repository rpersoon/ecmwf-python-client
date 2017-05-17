#
# (C) Copyright 2012-2013 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import json
import time

from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen, addinfourl


class APIException(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class Ignore303(HTTPRedirectHandler):

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if code in [301, 302]:
            # We want the posts to work even if we are redirected
            if code == 301:
                print()
                print("*** ECMWF API has moved to %s" % newurl)
                print("*** Please update your ~/.ecmwfapirc file")
                print()

            try:
                # Python < 3.4
                data = req.get_data()
            except AttributeError:
                # Python >= 3.4
                data = req.data

            try:
                # Python < 3.4
                origin_req_host = req.get_origin_req_host()
            except AttributeError:
                # Python >= 3.4
                origin_req_host = req.origin_req_host

            return Request(newurl,
                           data=data,
                           headers=req.headers,
                           origin_req_host=origin_req_host,
                           unverifiable=True)
        return None

    @staticmethod
    def http_error_303(req, fp, code, msg, headers):
        infourl = addinfourl(fp, headers, req.get_full_url())
        infourl.status = code
        infourl.code = code
        return infourl


class ApiConnection(object):

    def __init__(self, url, service, email, key, log, quiet=False, verbose=False, news=True):
        self.url = url
        self.email = email
        self.key = key
        self.service = service
        self.log = log
        self.quiet = quiet
        self.verbose = verbose
        self.retry = 5
        self.location = None
        self.done = False
        self.value = True
        self.offset = 0
        self.status = None

        self.log("ECMWF API at %s" % (self.url,))
        user = self.call("%s/%s" % (self.url, "who-am-i"))
        self.log("Welcome %s" % (user["full_name"] or "user '%s'" % user["uid"],))
        if news:
            try:
                news = self.call("%s/%s/%s" % (self.url, self.service, "news"))
                for n in news["news"].split("\n"):
                    self.log(n)
            except:
                pass

    def _bytename(self, size):
        prefix = {'': 'K', 'K': 'M', 'M': 'G', 'G': 'T', 'T': 'P', 'P': 'E'}
        l = ''
        size = size * 1.0
        while 1024 < size:
            l = prefix[l]
            size = size / 1024
        s = ""
        if size > 1:
            s = "s"
        return "%g %sbyte%s" % (size, l, s)

    def _transfer(self, url, path, size):
        self.log("Transfering %s into %s" % (self._bytename(size), path))
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

    def request(self, request, target=None):

        status = None

        self.submit("%s/%s/requests" % (self.url, self.service), request)
        self.log('Request submitted')
        self.log('Request id: ' + self.last['name'])
        if self.status != status:
            status = self.status
            self.log("Request is %s" % (status, ))

        while not self.ready():
            if self.status != status:
                status = self.status
                self.log("Request is %s" % (status, ))
            self.wait()

        if self.status != status:
            status = self.status
            self.log("Request is %s" % (status, ))

        result = self.result()
        if target:
            size = -1
            tries = 0
            while size != result["size"] and tries < 10:
                size = self._transfer(result["href"], target, result["size"])
                if size != result["size"] and tries < 10:
                    tries += 1
                    self.log("Transfer interrupted, retrying...")
                    time.sleep(60)
                else:
                    break

            assert size == result["size"]

        self.cleanup()

        return result

    def call(self, url, payload=None, method="GET"):

        if self.verbose:
            print(method, url)

        headers = {"Accept": "application/json", "From": self.email, "X-ECMWF-KEY": self.key}

        opener = build_opener(Ignore303)

        data = None
        if payload is not None:
            data = json.dumps(payload).encode('utf-8')
            headers["Content-Type"] = "application/json"

        url = "%s?offset=%d&limit=500" % (url, self.offset)
        req = Request(url=url, data=data, headers=headers)
        if method:
            req.get_method = lambda: method

        error = False

        try:
            res = opener.open(req)
        except HTTPError as e:
            # It seems that some version of urllib2 are buggy
            if e.code <= 299:
                res = e
            else:
                raise

        self.retry = int(res.headers.get("Retry-After", self.retry))
        code = res.code
        if code in [201, 202]:
            self.location = res.headers.get("Location", self.location)

        if self.verbose:
            print("Code", code)
            print("Content-Type", res.headers.get("Content-Type"))
            print("Content-Length", res.headers.get("Content-Length"))
            print("Location", res.headers.get("Location"))

        body = res.read().decode("utf-8")
        res.close()

        if code in [204]:
            self.last = None
            return None
        else:
            try:
                self.last = json.loads(body)
            except Exception as e:
                self.last = {"error": "%s: %s" % (e, body)}
                error = True

        if self.verbose:
            print(json.dumps(self.last, indent=4))

        self.status = self.last.get("status", self.status)

        if self.verbose:
            print("Status", self.status)

        if "messages" in self.last:
            for n in self.last["messages"]:
                if not self.quiet:
                    print(n)
                self.offset += 1

        if code == 200 and self.status == "complete":
            self.value = self.last
            self.done = True
            if isinstance(self.value, dict) and "result" in self.value:
                self.value = self.value["result"]

        if code in [303]:
            self.value = self.last
            self.done = True

        if "error" in self.last:
            raise APIException("ecmwf.API error 1: %s" % (self.last["error"],))

        if error:
            raise APIException("ecmwf.API error 2: %s" % (res, ))

        return self.last

    def submit(self, url, payload):
        self.call(url, payload, "POST")

    def POST(self, url, payload):
        return self.call(url, payload, "POST")

    def GET(self, url):
        return self.call(url, None, "GET")

    def wait(self):
        if self.verbose:
            print("Sleeping %s second(s)" % (self.retry))
        time.sleep(self.retry)
        self.call(self.location, None, "GET")

    def ready(self):
        return self.done

    def result(self):
        return self.value

    def cleanup(self):
        try:
            if self.location:
                self.call(self.location, None, "DELETE")

        except:
            pass
