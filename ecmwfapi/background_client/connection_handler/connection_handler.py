import datetime
import json
import queue
import random
import string
import threading

from background_client.socket_communication import SocketConnectionError
from .exceptions import ConnectionHandlerError


class ConnectionHandler(threading.Thread):

    def __init__(self, log, connection_queue, allowed_ips, active_task_storage, completed_task_storage, task_queue,
                 stop):
        """
        Initialise connection handler

        :param log: logging handler
        :param connection_queue: queue to retrieve connections from
        :param allowed_ips: ips allowed to connect
        :param active_task_storage: storage of the active tasks
        :param completed_task_storage: storage of the completed tasks
        :param task_queue: work queue with new tasks
        :param stop: method to call when the stop command is received
        """

        # Initialise the thread
        threading.Thread.__init__(self)

        if not isinstance(connection_queue, queue.Queue):
            raise ConnectionHandlerError("No valid queue object passed as connection queue")
        if not isinstance(allowed_ips, list):
            raise ConnectionHandlerError("No valid list object passed as allowed ips")

        self.allowed_ips = allowed_ips
        self.connection_queue = connection_queue
        self.log = log
        self.stop = stop

        self.active_task_storage = active_task_storage
        self.completed_task_storage = completed_task_storage
        self.task_queue = task_queue

    def run(self):
        """
        Main run function of the connection handler. Loops over connections in the queue and places data objects in
        the data queue
        """

        # Run until poison pill received
        while True:

            # Get a new connection from the queue
            connection = self.connection_queue.get()

            # Poison pill send through None object, end the loop if we get one
            if connection is None:
                break

            if connection.get_remote_host() not in self.allowed_ips:
                self.log.warning("Unauthorized connection from %s" % connection.get_remote_host())
                continue

            try:
                message = connection.receive()

            except SocketConnectionError as e:
                self.log.warning("Error while receiving message: %s" % str(e))
                continue

            try:
                message = json.loads(message)

            except (json.decoder.JSONDecodeError, TypeError) as e:
                self.log.error("Invalid JSON message: %s. Error: %s" % (message, e))
                exit(-1)

            response = None
            command_type = None
            command_data = None

            try:
                command_type = message['command']
                command_data = message['data']

            except KeyError as e:
                response = {
                    'status': 'error',
                    'error_message': "Invalid request, no command and / or data passed (%s)" % e
                }

            if response is None:

                if command_type == 'list_active_transfers':

                    data = self.list_transfers()

                    response = {
                        'status': 'ok',
                        'data': data
                    }

                elif command_type == 'list_completed_transfers':

                    data = self.list_transfers(True)

                    response = {
                        'status': 'ok',
                        'data': data
                    }

                elif command_type == 'add_transfer':

                    try:
                        task_id = self.add_transfer(command_data)

                        response = {
                            'status': 'ok',
                            'data': {
                                'task_id': task_id
                            }
                        }

                    except ConnectionHandlerError as e:
                        self.log.error("Failed to add transfer: %s" % e)

                        response = {
                            'status': 'error',
                            'error_message': "Failed to add the transfer"
                        }

                elif command_type == 'heartbeat':
                    response = {
                        'status': 'ok',
                        'data': {}
                    }

                elif command_type == 'stop':

                    self.stop()

                    response = {
                        'status': 'ok',
                        'data': {}
                    }

                else:
                    response = {
                        'status': 'error',
                        'error_message': "Invalid command %s" % command_type
                    }

            # Json encode the response
            response = json.dumps(response)

            # Send the response and terminate connection
            connection.send(response)
            connection.close()

            self.connection_queue.task_done()

    def list_transfers(self, completed=False):
        """
        List the currently active or completed transfers
        :param completed: whether to list completed or active transfers
        :return: dictionary with transfers
        """

        data = []

        if completed:
            storage = self.completed_task_storage
        else:
            storage = self.active_task_storage

        for task_id, item in storage.items():
            data.append({
                'task_id': task_id,
                'task_added': item['task_added'],
                'task_status': item['task_status'],
            })

        return data

    def add_transfer(self, data):
        """
        Add a transfer to the queue

        :param data: transfer data
        :return task_id: id of the newly created task
        """

        task_id = ''.join(random.choice(string.ascii_lowercase) for _ in range(32))

        self.active_task_storage[task_id] = {
            'task_added': datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
            'task_status': 'queued',
            'task_data': data,
        }

        # Add the task to the queue for processing
        self.task_queue.put(task_id)

        return task_id
