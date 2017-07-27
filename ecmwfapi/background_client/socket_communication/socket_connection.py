from .exceptions import SocketConnectionError

import socket
import struct


class SocketConnection:

    def __init__(self, remote_host, remote_port, connection=None, timeout=15):

        self.connection = None

        if connection is None:
            self._connect(remote_host, remote_port, timeout)
        else:
            self.connection = connection
            self.set_timeout(timeout)
            self.connected = True

        self.remote_host = remote_host
        self.remote_port = remote_port

    def send(self, message):
        """
        Send a message through the socket

        :param message: the message
        """

        if not self.connected:
            raise SocketConnectionError("Not connected to a socket")

        # Prefix each message with a 4-byte length
        message = struct.pack('>I', len(message)) + bytes(message, 'utf-8')

        try:
            self.connection.sendall(message)
        except socket.timeout:
            raise SocketConnectionError("Sending timed out")
        except ConnectionResetError:
            raise SocketConnectionError("Failed to send message: connection was reset")

    def receive(self):
        """
        Receive a message through the socket

        :return: the received message
        """

        # Read message length and unpack it into an integer
        raw_message_length = self._receive_all(4)

        #print('hi?')

        #print(raw_message_length)
        #print(len(raw_message_length))

        if not raw_message_length:
            print(raw_message_length)
            raise SocketConnectionError("Message didn't contain message length")

        message_length = struct.unpack('>I', raw_message_length)[0]

        #print(message_length)

        # Read the message data
        data = self._receive_all(message_length)

        if data is None:
            return None

        return data.decode('utf-8')

    def get_remote_host(self):
        """
        Get name of remote host at socket

        :return: remote host
        """
        return self.remote_host

    def get_remote_port(self):
        """
        Get port of remote host at socket

        :return: port
        """
        return self.remote_port

    def set_timeout(self, seconds):
        """
        Set the timeout for a the existing connection

        :param seconds: timeout in seconds
        """

        self.connection.settimeout(seconds)

    def close(self):
        """
        Close the socket connection
        """

        self.connection.close()
        self.connected = False

    def _connect(self, host, port, timeout):
        """
        Open a new connection

        :param host: host to connect to
        :param port: port to connect to
        """

        if self.connection is not None:
            raise SocketConnectionError("This instance already started a server or client")

        try:
            self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.set_timeout(timeout)

            # Connect to server and send data
            self.connection.connect((host, port))

        except ConnectionRefusedError:
            raise SocketConnectionError("Connection refused")

        except socket.timeout:
            raise SocketConnectionError("Connection timed out")

        self.connected = True

    def _receive_all(self, n):
        """
        Receive a given number of bytes from the socket connection

        :param n: number of bytes
        """

        data = b''
        while len(data) < n:
            try:
                packet = self.connection.recv(n - len(data))
            except socket.timeout:
                raise SocketConnectionError("Receiving timed out")
            except ConnectionResetError:
                raise SocketConnectionError("Failed to receive: connection was reset")

            if not packet:
                return None
            data += packet
        return data
