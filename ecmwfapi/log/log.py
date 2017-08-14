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


import time


class Log:

    def __init__(self, display_info_messages=True, display_warning_messages=True, display_error_messages=True):

        self.info_messages = []
        self.warning_messages = []
        self.error_messages = []

        self.display_info_messages = display_info_messages
        self.display_warning_messages = display_warning_messages
        self.display_error_messages = display_error_messages

    def log(self, message, log_type='info'):
        """
        General logging function that supports all three logging options: info, warning and error. Can be passed as a
        logging handle to classes

        :param message: the message to log
        :param log_type:
        :return: None
        """
        if log_type == 'error':
            self.error(message)
        elif log_type == 'warning':
            self.warning(message)
        else:
            self.info(message)

    def info(self, message):
        """
        Logs an info message and outputs as specified in configuration

        :param message: the message to log
        :return: none
        """

        if self.display_info_messages:
            print('[Info]     %s - %s' % (time.strftime("%d-%m-%Y %H:%M:%S"), message))

        self.info_messages.append(message)

    def warning(self, message):
        """
        Logs a warning message and outputs as specified in configuration

        :param message: The message to log
        :return: none
        """

        if self.display_warning_messages:
            print('[Warning]  %s - %s' % (time.strftime("%d-%m-%Y %H:%M:%S"), message))

        self.warning_messages.append(message)

    def error(self, message):
        """
        Logs an error message and outputs as specified in configuration

        :param message: The message to log
        :return: none
        """

        if self.display_error_messages:
            print('[Error]    %s - %s' % (time.strftime("%d-%m-%Y %H:%M:%S"), message))

        self.error_messages.append(message)

    def get_info_messages(self):
        """
        Obtain the list of info messages logged so far

        :return: list with all logged info messages
        """

        return self.info_messages

    def get_warning_messages(self):
        """
        Obtain the list of warning messages logged so far

        :return: list with all logged info messages
        """

        return self.warning_messages

    def get_error_messages(self):
        """
        Obtain the list of error messages logged so far

        :return: list with all logged info messages
        """

        return self.error_messages
