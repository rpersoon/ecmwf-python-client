from .config import Config
from .exceptions import ConfigError

# Initialise config-class to use the same instance in different imports
config = Config()
