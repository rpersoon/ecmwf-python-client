from .exceptions import SocketServerError
from .socket_connection import SocketConnection

import socket


class SocketServer:
    """
    Server instance of a Python socket application
    """

    def __init__(self, host, port):
        """
        Initialise the socket server and open the socket

        :param host: host to connect to
        :param port: port to connect to
        """

        self.running = False

        self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.connection.bind((host, port))
        self.connection.listen(10)

    def run(self, connection_queue):
        """
        Run the server, which loops, accepts connections and adds them to the socket queue

        :param connection_queue: queue object in which new connections are placed
        """

        self.running = True
        self.connection.settimeout(0.1)

        while self.running:
            try:
                [connection, remote] = self.connection.accept()
                connection.settimeout(None)

                socket_connection = SocketConnection(remote[0], remote[1], connection)
                connection_queue.put(socket_connection)

            except socket.timeout:
                pass
            except ConnectionResetError:
                pass

    def stop(self):
        """
        Stop listening and end the server
        """

        self.running = False

    def shutdown(self):
        """
        Close the socket connection
        """

        self.connection.close()
