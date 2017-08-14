#!/usr/bin/env python3
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


import json
import os
import subprocess
import sys

from ecmwfapi.background_client.socket_communication import SocketConnection, SocketConnectionError


def main():

    if len(sys.argv) <= 1:
        print("No command specified. Use 'background_client_cli.py help' for usage instructions.")

    else:
        if sys.argv[1] == 'status':
            status()

        elif sys.argv[1] == 'start':
            start_background_client()

        elif sys.argv[1] == 'stop':
            stop_background_client()

        elif sys.argv[1] == 'list_active_transfers':
            list_transfers()

        elif sys.argv[1] == 'list_completed_transfers':
            list_transfers(True)

        elif sys.argv[1] == 'add_transfer':
            if len(sys.argv) > 2:
                add_transfer(sys.argv[2])

            else:
                print("No transfer data specified. Use 'background_client_cli.py help' for usage instructions.")

        elif sys.argv[1] == 'cancel_transfer':
            if len(sys.argv) > 2:
                cancel_transfer(sys.argv[2])

            else:
                print("No transfer ID specified. Use 'background_client_cli.py help' for usage instructions.")

        elif sys.argv[1] == 'help':
            print_help()

        else:
            print("Unknown command. Use 'background_client_cli.py help' for usage instructions.")


def list_transfers(completed=False):
    """
    Lists the currently active transfers in the background client
    """

    if completed:
        command_response = send_command('list_completed_transfers')

    else:
        command_response = send_command('list_active_transfers')

    if command_response['status'] == 'ok':

        if len(command_response['data']) > 0:
            print('----------------------------------------------------------------------')
            print('Task added             Task status    Task ID')
            print('----------------------------------------------------------------------')
            for item in command_response['data']:
                print('%s    ' % item['task_added'], end='')
                print(item['task_status'], end='')
                for i in range(0, 15 - len(item['task_status'])):
                    print(' ', end='')
                print(item['task_id'])

        else:
            if completed:
                print("No transfers completed")
            else:
                print("No transfers currently active")

    else:
        print("An error occurred while listing transfers: %s" % command_response['error_message'])


def add_transfer(transfer_data):
    """
    Add a transfer

    :param transfer_data: parameters for the transfer
    """

    # Dictionary for the transfer parameters
    transfer_parameters = {}

    # Remove all whitespace from the transfer data
    transfer_data = ''.join(transfer_data.split())

    # If there is a comma in the transfer data, the data is passed as the argument itself
    if ':' in transfer_data:
        items = transfer_data.split(',')

        for item in items:

            parts = item.split(':')

            if len(parts) != 2:
                print("Incorrect transfer data given, please call 'background_client_cli.py help' for the syntax")
                return

            transfer_parameters[parts[0]] = parts[1]

    # If there is no comma in the transfer data, it should be a reference to a file with the transfer data
    else:
        try:
            with open(transfer_data) as file:
                content = file.readlines()

            # Strip whitespace
            content = [item.strip() for item in content]

            for item in content:
                parts = item.split(':')

                if len(parts) != 2:
                    print("Incorrect transfer data in file, please call 'background_client_cli.py help' for the syntax")
                    return

                transfer_parameters[parts[0]] = parts[1]

        except FileNotFoundError:
            print("File '%s' not found" % transfer_data)
            return

    command_response = send_command('add_transfer', transfer_parameters)

    if command_response['status'] == 'ok':
        print("The transfer was successfully added with task_id %s" % command_response['data']['task_id'])

    else:
        print("An error occurred while adding the transfer: %s" % command_response['error_message'])


def cancel_transfer(task_id):
    """
    Cancel a transfer

    :param task_id: task_id of the transfer to be cancelled. Only queued transfers can be cancelled
    """

    command_response = send_command('cancel_transfer', {'task_id': task_id})

    if command_response['status'] == 'ok':
        print("The transfer was successfully cancelled")

    else:
        print("An error occurred while cancelling the transfer: %s" % command_response['error_message'])


def start_background_client():
    """
    Start the background client. Checks whether it is already running first.
    """

    if send_command('heartbeat')['status'] == 'ok':
        print("The background client is already running")
        return

    subprocess.Popen(["python3", "ecmwfapi/ECMWFBackgroundClient.py"], preexec_fn=os.setpgrp,
                     stdout=open(os.devnull, 'w'), stderr=open(os.devnull, 'w'))
    print("The background client has been started")


def stop_background_client():
    """
    Stop the background client. Checks whether it is actually running. Any active transfers will be finished by the
    client, but the port will be freed. So it can start again immediately.
    """

    command_response = send_command('stop')

    if command_response['status'] == 'ok':
        print("The background client has been stopped. Any active transfers will be finished.")

    else:
        print("The background client was not active")


def status():
    """
    Send a heartbeat to the client to test the connection
    """

    command_response = send_command('heartbeat')

    if command_response['status'] == 'ok':
        print("The background client is active")

    else:
        print("The background client is not running")


def send_command(command_type, command_data=None):
    """
    Send a command to the background client

    :param command_type: type of the command
    :param command_data: optional data associated with the command
    :return dict: response to the command by the background client
    """

    # Try to connect to the background daemon
    try:
        connection = SocketConnection('127.0.0.1', 54500)

        if command_data is None:
            command_data = {}

        data = {
            'command': command_type,
            'data': command_data
        }

        connection.send(json.dumps(data))
        response = connection.receive()
        connection.close()

    except (SocketConnectionError, ValueError):
        return {
            'status': "error",
            'error_message': "API communication failure"
        }

    return json.loads(response)


def print_help():
    """
    Prints the help information
    """

    print("Available commands:")
    print("./background_client_cli.py.py status                    - Check whether the background client is running")
    print("./background_client_cli.py.py start                     - Start the background client")
    print("./background_client_cli.py.py stop                      - Stop the background client")
    print("./background_client_cli.py.py list_active_transfers     - List the currently active transfers")
    print("./background_client_cli.py.py list_completed_transfers  - List the completed transfers since the background "
          "client was started")
    print("./background_client_cli.py.py add_transfer <parameters> - Start a new transfer")
    print("./background_client_cli.py.py cancel_transfer <task id> - Cancel a transfer")
    print()
    print("Parameter format for a new transfer")
    print("-----------------------------------")
    print("The parameters of new transfers can either be specified on the command line directly, or entered in a file. "
          "When using the command line, different parameters are separated by comma's and each of the key-value pairs "
          "are separated by colons. When using a file, different parameters are separated by new lines and each of the "
          "key-value pairs are separated by colons.")
    print()
    print("The following examples are both valid ways to start the same transfer:")
    print()
    print("./background_client.py add_transfer class:s2,dataset:s2s,date:2015-01-01,expver:prod,levtype:sfc,"
          "origin:ecmf,param:165,step:0/to/1104/by/24,stream:enfo,target:test,time:00,type:cf")
    print()
    print('    or')
    print()
    print("./background_client_cli.py.py add_transfer transfer_data.txt")
    print()
    print("Where the file 'transfer_data.txt' would contain:")
    print()
    print("class: s2")
    print("dataset: s2s")
    print("date: 2015-01-01")
    print("expver: prod")
    print("levtype: sfc")
    print("origin: ecmf")
    print("param: 165")
    print("step: 0/to/1104/by/24")
    print("stream: enfo")
    print("target: test")
    print("time: 00")
    print("type: cf")


if __name__ == "__main__":
    main()
