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


from .exceptions import TransferHandlerError

import os
import queue
import threading
import sys

# Required to import the ECMWFDataServer
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/../../..')

from ecmwfapi.ECMWFDataServer import ECMWFDataServer


class TransferHandler(threading.Thread):

    def __init__(self, log, active_task_storage, completed_task_storage, task_queue):
        """
        Initialise connection handler

        :param log: logging handler
        :param active_task_storage: storage of the active tasks
        :param completed_task_storage: storage of the completed tasks
        :param task_queue: work queue with new tasks
        """

        # Initialise the thread
        threading.Thread.__init__(self)

        if not isinstance(active_task_storage, dict):
            raise TransferHandlerError("The active task storage object should be a dictionary")
        if not isinstance(completed_task_storage, dict):
            raise TransferHandlerError("The active task storage object should be a dictionary")
        if not isinstance(task_queue, queue.Queue):
            raise TransferHandlerError("The task queue object should be a queue")

        self.log = log
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
            task = self.task_queue.get()

            # Poison pill send through None object, end the loop if we get one
            if task is None:
                break

            self.active_task_storage[task]['task_status'] = 'active'

            # Process the transfer
            server = ECMWFDataServer()
            server.retrieve(self.active_task_storage[task]['task_data'])

            self.completed_task_storage[task] = {
                'task_added': self.active_task_storage[task]['task_added'],
                'task_status': 'completed',
            }

            del self.active_task_storage[task]

            self.task_queue.task_done()
