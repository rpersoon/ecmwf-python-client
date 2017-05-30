import configparser

from ecmwfapi.config.exceptions import *


class Config:

    def __init__(self):

        self._config = None

    def load(self, config_path):
        """
        Load a given configuration file in the ConfigParser

        :param config_path: path to the config file, relative to application root
        """

        parser = configparser.ConfigParser()

        if not parser.read(config_path):
            raise ConfigError('Configuration file not found: %s' % config_path)
        self._config = parser

    def _item(self, item_name, section_name=None, item_type='str'):
        """
        Return a configuration item

        :param item_name: name of the item in the configuration file
        :param section_name: section where the item is located in the configuration file
        :param item_type: datatype of item, in [none, boolean, int, float]
        :return: the requested configuration value
        """

        try:
            if item_type is 'none':
                return self._config.get(section_name, item_name)
            elif item_type is 'boolean':
                return self._config.getboolean(section_name, item_name)
            elif item_type is 'int':
                return self._config.getint(section_name, item_name)
            elif item_type is 'float':
                return self._config.getfloat(section_name, item_name)
            else:
                raise ConfigError("Unknown data type %s" % item_type)

        except (configparser.NoOptionError, configparser.NoSectionError):
            raise ConfigError('Configuration option %s not found' % item_name)

    def get(self, item_name, section_name=None):
        """
        Get a config item, without validating data type

        :param item_name: name of the item in the configuration file
        :param section_name: section where the item is located in the configuration file
        :return: the requested configuration value
        """

        return self._item(item_name, section_name, 'none')

    def get_boolean(self, item_name, section_name=None):
        """
        Get a config item, while validating that it's a boolean

        :param item_name: name of the item in the configuration file
        :param section_name: section where the item is located in the configuration file
        :return: the requested configuration value
        """

        return self._item(item_name, section_name, 'boolean')

    def get_int(self, item_name, section_name=None):
        """
        Get a config item, while validating that it's an integer

        :param item_name: name of the item in the configuration file
        :param section_name: section where the item is located in the configuration file
        :return: the requested configuration value
        """

        return self._item(item_name, section_name, 'int')

    def get_float(self, item_name, section_name=None):
        """
        Get a config item, while validating that it's a float

        :param item_name: name of the item in the configuration file
        :param section_name: section where the item is located in the configuration file
        :return: the requested configuration value
        """

        return self._item(item_name, section_name, 'float')
