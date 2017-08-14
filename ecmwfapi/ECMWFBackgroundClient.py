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


import queue
import signal

from background_client_cli.connection_handler import ConnectionHandler, ConnectionHandlerError
from background_client_cli.socket_communication import SocketServer, SocketServerError
from background_client_cli.transfer_handler import TransferHandler, TransferHandlerError
from log import *

server_instance = None


# Define an exit handler to properly shutdown if required
def exit_handler(exit_signal, frame):

    # Unused arguments
    del exit_signal, frame

    # Stop the server instance
    server_instance.stop()

signal.signal(signal.SIGINT, exit_handler)


def stop():

    # Stop the server instance
    server_instance.stop()


def main():

    global server_instance

    # Start the logger
    try:
        log_handle = Log()

    except LogError as e:
        print("Failed to start log: %s" % e)
        return

    # Define the task storage / administration
    active_task_storage = {}
    completed_task_storage = {}

    # Create a task queue
    task_queue = queue.Queue(1000)

    # Create a queue containing new connections, maximum size 25
    connection_queue = queue.Queue(25)

    allowed_ips = ['127.0.0.1']

    # Start 8 threads to handle connections
    connection_threads = []
    for i in range(8):
        try:
            thread = ConnectionHandler(log_handle, connection_queue, allowed_ips, active_task_storage,
                                       completed_task_storage, task_queue, stop)
            thread.start()
            connection_threads.append(thread)

        except ConnectionHandlerError as e:
            log_handle.error("Failed to start connection handler: %s" % e)
            exit(-1)

    # Start 5 threads to process transfers
    process_threads = []
    for i in range(5):
        try:
            thread = TransferHandler(log_handle, active_task_storage, completed_task_storage, task_queue)
            thread.start()
            process_threads.append(thread)

        except TransferHandlerError as e:
            log_handle.error("Failed to start transfer handler: %s" % e)
            exit(-1)

    # Start the server instance
    try:
        server_instance = SocketServer('', 54500)
        server_instance.run(connection_queue)

    except SocketServerError as e:
        log_handle.error("Error in socket server: %s" % str(e))
        exit(-1)

    # Stop the server when running completes
    server_instance.shutdown()

    # Send poison pills to connection handler threads
    for _ in range(8):
        connection_queue.put(None)

    for i in range(8):
        connection_threads[i].join()

    # Send poison pills to transfer processing threads
    for _ in range(5):
        task_queue.put(None)

    for i in range(5):
        process_threads[i].join()

if __name__ == "__main__":
    main()
