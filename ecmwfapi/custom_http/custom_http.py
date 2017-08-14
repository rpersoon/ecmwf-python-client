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


from .exceptions import CustomHttpError

import concurrent.futures
import httplib2
import queue
import socket
import time


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

    except (httplib2.ServerNotFoundError, ConnectionResetError, ConnectionAbortedError, ConnectionRefusedError,
            ConnectionError) as e:
        raise CustomHttpError("Could not retrieve URL %s. Additional info: %s" % (url, e))
    except socket.timeout:
        raise CustomHttpError("Request timed out after specified timeout period of %s seconds" % timeout)
    except Exception as e:
        raise CustomHttpError("Other unknown exception: %s" % e)

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

    except (httplib2.ServerNotFoundError, ConnectionResetError, ConnectionAbortedError, ConnectionRefusedError,
            ConnectionError) as e:
        raise CustomHttpError("Could not retrieve URL %s. Additional info: %s" % (url, e))
    except socket.timeout:
        raise CustomHttpError("Request timed out after specified timeout period of %s seconds" % timeout)
    except Exception as e:
        raise CustomHttpError("Other unknown exception: %s" % e)

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

    except (httplib2.ServerNotFoundError, ConnectionResetError, ConnectionAbortedError, ConnectionRefusedError,
            ConnectionError) as e:
        raise CustomHttpError("Could not retrieve URL %s. Additional info: %s" % (url, e))
    except socket.timeout:
        raise CustomHttpError("Request timed out after specified timeout period of %s seconds" % timeout)
    except Exception as e:
        raise CustomHttpError("Other unknown exception: %s" % e)

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
        raise CustomHttpError("The block size should be an integer")
    elif block_size < 512:
        raise CustomHttpError("The block size should be at least 512 bytes")
    elif block_size > 268435456:
        raise CustomHttpError("The block size can not be more than 256 megabytes")

    # Verify timeout parameter
    if not isinstance(timeout, int):
        raise CustomHttpError("The timeout should be an integer")
    elif timeout < 1:
        raise CustomHttpError("The timeout should be at least 1 second")
    elif timeout > 86400:
        raise CustomHttpError("The timeout can not be more than 86400 seconds")

    # Define HTTP handler
    http_handle = httplib2.Http(timeout=timeout, disable_ssl_certificate_validation=disable_ssl_validation)

    # Retrieve header first in order to determine file size
    connected = False
    connection_retries = 0
    headers = None

    while not connected:
        if connection_retries > 0:
            print("Failed to retrieve header information, retry %s of 5" % connection_retries)

        try:
            headers, _ = http_handle.request(url, 'HEAD', '', headers={})
            connected = True

        except httplib2.ServerNotFoundError as e:
            connection_retries += 1
            if connection_retries > 5:
                raise CustomHttpError("The IP address of %s could not be determined. Additional info: %s" % (url, e))

        except socket.timeout:
            connection_retries += 1
            if connection_retries > 5:
                raise CustomHttpError("The connection with %s timed out while retrieving header information" % url)

    try:
        content_length = int(headers['content-length'])

    except KeyError:
        raise CustomHttpError("Content length not set")

    block_start = 0
    block_end = block_size

    if block_end > content_length:
        block_end = content_length - 1

    while content_length > block_start and block_end != block_start:

        file_handle.write(_get_block(http_handle, url, block_start, block_end))
        block_start = block_end + 1
        block_end += block_size

        if block_end >= content_length:
            block_end = content_length - 1

    return content_length


def robust_get_file_parallel(url, file_handle, block_size=1048576, timeout=20, disable_ssl_validation=False, threads=5):
    """
    Download an object in a robust way using HTTP partial downloading, and process multiple blocks in parallel

    :param url: URL to download
    :param file_handle: open pointer to file to store download in, data is appended
    :param block_size: size of individual download chunks during partial downloading
    :param timeout: timeout in seconds till individual block downloads are failed
    :param disable_ssl_validation: whether to disable SSL validation in httplib2
    :param threads: number of threads to download blocks
    :return: None
    """

    # Verify block size parameter
    if not isinstance(block_size, int):
        raise CustomHttpError("The block size should be an integer")
    elif block_size < 512:
        raise CustomHttpError("The block size should be at least 512 bytes")
    elif block_size > 268435456:
        raise CustomHttpError("The block size can not be more than 256 megabytes")

    # Verify timeout parameter
    if not isinstance(timeout, int):
        raise CustomHttpError("The timeout should be an integer")
    elif timeout < 1:
        raise CustomHttpError("The timeout should be at least 1 second")
    elif timeout > 86400:
        raise CustomHttpError("The timeout can not be more than 86400 seconds")

    # Define block result storage
    result_blocks = {}

    # Define work queue
    work_queue = queue.Queue()

    # Launch worker threads
    thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=threads)
    for i in range(threads):
        thread_pool.submit(_thread_download, work_queue, result_blocks, url, timeout, disable_ssl_validation)

    # Define HTTP handler
    http_handle = httplib2.Http(timeout=timeout, disable_ssl_certificate_validation=disable_ssl_validation)

    # Retrieve header first in order to determine file size
    connected = False
    connection_retries = 0
    headers = None

    while not connected:
        if connection_retries > 0:
            print("Failed to retrieve header information, retry %s of 5" % connection_retries)

        try:
            headers, _ = http_handle.request(url, 'HEAD', '', headers={})
            connected = True

        except httplib2.ServerNotFoundError as e:
            connection_retries += 1
            if connection_retries > 5:
                raise CustomHttpError("The IP address of %s could not be determined. Additional info: %s" % (url, e))

        except socket.timeout:
            connection_retries += 1
            if connection_retries > 5:
                raise CustomHttpError("The connection with %s timed out while retrieving header information" % url)

    try:
        content_length = int(headers['content-length'])

    except KeyError:
        raise CustomHttpError("Content length not set")

    block_start = 0
    block_end = block_size

    if block_end > content_length:
        block_end = content_length - 1

    block_id = 0
    while content_length > block_start and block_end != block_start:

        work_queue.put([block_id, block_start, block_end])

        block_start = block_end + 1
        block_end += block_size

        if block_end >= content_length:
            block_end = content_length - 1

        block_id += 1

    # Insert poison pills in queue
    for _ in range(threads):
        work_queue.put(None)

    # Write all result blocks to the result file
    written_block_id = 0
    while written_block_id < block_id:

        written = False
        while not written:

            try:
                file_handle.write(result_blocks[written_block_id])
                written = True
                result_blocks.pop(written_block_id)
                written_block_id += 1

            except KeyError:
                time.sleep(0.1)

    return content_length


def _get_block(http_handle, url, block_start, block_end):
    headers = {
        'Range': 'bytes=%s-%s' % (block_start, block_end)
    }

    content = None
    completed = False
    try_count = 0

    while not completed and try_count < 7:
        try:
            resp, content = http_handle.request(url, 'GET', '', headers)
            completed = True

        except Exception as e:
            print("Failed a block, retrying (%s)" % e)
            try_count += 1

    if not completed:
        raise CustomHttpError("Downloading of block failed after 7 retries")

    return content


def _thread_download(work_queue, result_blocks, url, timeout, disable_ssl_validation):
    """
    Download a block and save result in provided dictionary. Called by the thread pool.

    :param work_queue: queue object to obtain work items from
    :param result_blocks: dictionary to store block in
    :param url: url to download from
    :param timeout: http timeout
    :param disable_ssl_validation: whether to disable SSL validation in httplib2
    """

    # Initialise HTTP handle
    http_handle = httplib2.Http(timeout=timeout, disable_ssl_certificate_validation=disable_ssl_validation)

    while True:
        work = work_queue.get()

        # Stop if receiving poison pill
        if work is None:
            return

        result_blocks[work[0]] = _get_block(http_handle, url, work[1], work[2])
