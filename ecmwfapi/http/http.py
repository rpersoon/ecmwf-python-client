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


def robust_get_file(url, file_handle, block_size=1048576, timeout=15):
    """
    Download an object in a robust way using HTTP partial downloading

    :param url: URL to download
    :param file: open pointer to file to store download in, data is appended
    :param block_size: size of individual download chunks during partial downloading
    :param timeout: timeout in seconds till individual block downloads are failed
    :return: None
    """

    # Retrieve header first in order to determine file size
    try:
        h = httplib2.Http(timeout=timeout)
        resp, content = h.request(url, 'HEAD', '', headers={})

    except httplib2.ServerNotFoundError as e:
        raise HttpError("Could not retrieve URL %s. Additional info: %s" % (str(url), str(e)))

    try:
        content_length = int(resp['content-length'])

    except KeyError:
        raise HttpError("Content length not set")

    block_start = 0
    block_end = block_size

    if block_end > content_length:
        block_end = content_length - 1

    while content_length > block_start and block_end != block_start:
        headers = {
            'Range': 'bytes=%s-%s' % (block_start, block_end)
        }

        completed = False
        try_count = 0

        while not completed and try_count < 5:

            try:
                resp, content = h.request(url, 'GET', '', headers)
                completed = True

            except Exception:
                try_count += 1

        file_handle.write(content)

        block_end_old = block_end
        block_start = block_end + 1
        block_end = block_end_old + block_size

        if block_end >= content_length:
            block_end = content_length - 1

    return content_length
