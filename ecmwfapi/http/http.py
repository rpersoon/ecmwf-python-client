from .exceptions import *

import httplib2
import socket


def get_request(url, headers=None, timeout=30):
    """
    Retrieve contents of a page, passing any exceptions. Does not follow redirects.

    :param url: URL to retrieve
    :param headers: request headers
    :param timeout: timeout of request in seconds
    :return: page data
    """

    try:
        h = httplib2.Http(timeout=timeout)
        h.follow_redirects = False
        resp, content = h.request(url, 'GET', '', headers=headers)

    except (httplib2.ServerNotFoundError, ConnectionRefusedError) as e:
        raise HttpError("Could not retrieve URL %s. Additional info: %s" % (url, e))
    except socket.timeout:
        raise HttpError("Request timed out after specified timeout period of %s seconds" % timeout)

    return [resp, content]


def post_request(url, data, headers=None, timeout=30):
    """
    Retrieve contents of a page with a POST request and one payload data object, passing any exceptions.  Does not
    follow redirects.

    :param url: URL to retrieve
    :param data: Payload data object
    :param headers: request headers
    :param timeout: timeout of request in seconds
    :return: page data
    """

    # Prepare headers even if there are none, as we're doing a post request
    if headers is None:
        headers = {}
    headers['Content-type'] = 'application/x-www-form-urlencoded'

    try:
        h = httplib2.Http(timeout=timeout)
        h.follow_redirects = False
        resp, content = h.request(url, 'POST', data, headers=headers)

    except (httplib2.ServerNotFoundError, ConnectionRefusedError) as e:
        raise HttpError("Could not retrieve URL %s. Additional info: %s" % (url, e))
    except socket.timeout:
        raise HttpError("Request timed out after specified timeout period of %s seconds" % timeout)

    return [resp, content]


def delete_request(url, headers=None, timeout=30):
    """
    Retrieve contents of a page, passing any exceptions. Does not follow redirects.

    :param url: URL to retrieve
    :param headers: request headers
    :param timeout: timeout of request in seconds
    :return: page data
    """

    try:
        h = httplib2.Http(timeout=timeout)
        h.follow_redirects = False
        resp, content = h.request(url, 'DELETE', '', headers=headers)

    except (httplib2.ServerNotFoundError, ConnectionRefusedError) as e:
        raise HttpError("Could not retrieve URL %s. Additional info: %s" % (url, e))
    except socket.timeout:
        raise HttpError("Request timed out after specified timeout period of %s seconds" % timeout)

    return [resp, content]
