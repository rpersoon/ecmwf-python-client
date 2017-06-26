from .exceptions import *

import httplib2
import socket


def get_request(url, headers=None, timeout=30, disable_ssl_validation=False):
    """
    Retrieve contents of a page, passing any exceptions. Does not follow redirects.

    :param url: URL to retrieve
    :param headers: request headers
    :param timeout: timeout of request in seconds
    :param disable_ssl_validation: whether to disable SSL validation in httplib2
    :return: page data
    """

    try:
        h = httplib2.Http(timeout=timeout, disable_ssl_certificate_validation=disable_ssl_validation)
        h.follow_redirects = False
        resp, content = h.request(url, 'GET', '', headers=headers)

    except (httplib2.ServerNotFoundError, ConnectionRefusedError) as e:
        raise HttpError("Could not retrieve URL %s. Additional info: %s" % (url, e))
    except socket.timeout:
        raise HttpError("Request timed out after specified timeout period of %s seconds" % timeout)

    return [resp, content]


def post_request(url, data, headers=None, timeout=30, disable_ssl_validation=False):
    """
    Retrieve contents of a page with a POST request and one payload data object, passing any exceptions.  Does not
    follow redirects.

    :param url: URL to retrieve
    :param data: Payload data object
    :param headers: request headers
    :param timeout: timeout of request in seconds
    :param disable_ssl_validation: whether to disable SSL validation in httplib2
    :return: page data
    """

    # Prepare headers even if there are none, as we're doing a post request
    if headers is None:
        headers = {}
    headers['Content-type'] = 'application/x-www-form-urlencoded'

    try:
        h = httplib2.Http(timeout=timeout, disable_ssl_certificate_validation=disable_ssl_validation)
        h.follow_redirects = False
        resp, content = h.request(url, 'POST', data, headers=headers)

    except (httplib2.ServerNotFoundError, ConnectionRefusedError) as e:
        raise HttpError("Could not retrieve URL %s. Additional info: %s" % (url, e))
    except socket.timeout:
        raise HttpError("Request timed out after specified timeout period of %s seconds" % timeout)

    return [resp, content]


def delete_request(url, headers=None, timeout=30, disable_ssl_validation=False):
    """
    Retrieve contents of a page, passing any exceptions. Does not follow redirects.

    :param url: URL to retrieve
    :param headers: request headers
    :param timeout: timeout of request in seconds
    :param disable_ssl_validation: whether to disable SSL validation in httplib2
    :return: page data
    """

    try:
        h = httplib2.Http(timeout=timeout, disable_ssl_certificate_validation=disable_ssl_validation)
        h.follow_redirects = False
        resp, content = h.request(url, 'DELETE', '', headers=headers)

    except (httplib2.ServerNotFoundError, ConnectionRefusedError) as e:
        raise HttpError("Could not retrieve URL %s. Additional info: %s" % (url, e))
    except socket.timeout:
        raise HttpError("Request timed out after specified timeout period of %s seconds" % timeout)

    return [resp, content]


def robust_get_file(url, file_handle, block_size=1048576, timeout=20, disable_ssl_validation=False):
    """
    Download an object in a robust way using HTTP partial downloading

    :param url: URL to download
    :param file_handle: open pointer to file to store download in, data is appended
    :param block_size: size of individual download chunks during partial downloading
    :param timeout: timeout in seconds till individual block downloads are failed
    :param disable_ssl_validation: whether to disable SSL validation in httplib2
    :return: None
    """

    # Verify block size parameter
    if not isinstance(block_size, int):
        raise HttpError("The block size should be an integer")
    elif block_size < 512:
        raise HttpError("The block size should be at least 512 bytes")
    elif block_size > 268435456:
        raise HttpError("The block size can not be more than 256 megabytes")

    # Verify timeout parameter
    if not isinstance(timeout, int):
        raise HttpError("The timeout should be an integer")
    elif timeout < 1:
        raise HttpError("The timeout should be at least 1 second")
    elif timeout > 86400:
        raise HttpError("The timeout can not be more than 86400 seconds")

    # Retrieve header first in order to determine file size
    try:
        h = httplib2.Http(timeout=timeout, disable_ssl_certificate_validation=disable_ssl_validation)
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

        while not completed and try_count < 7:

            try:
                resp, content = h.request(url, 'GET', '', headers)
                completed = True

            except Exception:
                print("Failed a block, retrying")
                try_count += 1

        file_handle.write(content)

        block_end_old = block_end
        block_start = block_end + 1
        block_end = block_end_old + block_size

        if block_end >= content_length:
            block_end = content_length - 1

    return content_length
