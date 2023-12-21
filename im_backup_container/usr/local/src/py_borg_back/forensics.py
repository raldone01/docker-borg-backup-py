import logging
import sys

from config_validation import Validation
from config_defaults import repo_general_defaults

class HashDeep:
  """
  The HashDeep class computes the hash of all matching files and stores the following information in a file:
  Timestamp, Filepath, Hash, Filesize
  """
  def __init__(self, config, config_args):
    # add logger with name
    self.logger = logging.getLogger("hash_deep")
    self._load_extract_config(config, config_args)

  def _load_extract_config(self, config, config_args):
    self.logger.debug("Loading config for hash_deep")
    validation = Validation(self.logger)

    self.logger.setLevel(self._load_config_key(config, 'log_level', Validation.validate_log_level(self.logger), config_args=config_args).upper())


    self.enabled = self._load_config_key(config, 'enabled', validation.validate_bool, config_args=config_args)

    self.files_include = self._load_config_key(config, 'files_include', lambda x: validation.validate_list_of(x, validation.validate_string))
    self.files_exclude = self._load_config_key(config, 'files_exclude', lambda x: validation.validate_list_of(x, validation.validate_string))

    self.hostname = self._load_config_key(config, 'hostname', validation.validate_string)

    supported_algorithms = ["sha512", "sha256", "sha1", "md5"]
    def validate_algorithm(algorithm):
      if not validation.validate_string(algorithm):
        return False
      if algorithm not in supported_algorithms:
        self.logger.error(f"Algorithm \"{algorithm}\" is not supported")
        return False
      return True
    self.algorithm = self._load_config_key(config, 'algorithm', validate_algorithm)
    self.output_folder = self._load_config_key(config, 'output_folder', validation.validate_string)

  def _load_config_key(self, config, key, validator=None, default=None, config_args=None):
    hash_deep_config = config['hash_deep']

    config_value = None
    if config_args is not None and hasattr(config_args, key):
      config_value = getattr(config_args, key)
    elif key in hash_deep_config:
      config_value = hash_deep_config[key]
    elif key in repo_general_defaults:
      config_value = repo_general_defaults[key]
    elif default is not None:
      config_value = default
    else:
      self.logger.error(f"Failed to find config key \"{key}\" in repo \"{self.name}\"")
      sys.exit(1)

    if validator is not None:
      if not validator(config_value):
        self.logger.error(f"Config key \"{key}\" in hash_deep failed validation")
        sys.exit(1)
    return config_value

  def _not_enabled(self):
    if not self.enabled:
      self.logger.debug(f"Skipping hash_deep. It is disabled.")
      return True
    return False

  def _make_filepath(self):
    filename = f"hash_deep_{self.hostname}.txt"
    return f"{self.output_folder}/{filename}"

  # Similar to https://github.com/borgbackup/borg/blob/master/src/borg/archiver/create_cmd.py
  def _rec_walk(
        self,
        *,
        path,
        name,
        fso,
        cache,
        matcher,
        exclude_caches,
        exclude_if_present,
        keep_exclude_tags,
    ):
      """
      Process *path* (or, preferably, parent_fd/name) recursively according to the various parameters.

      This should only raise on critical errors. Per-item errors must be handled within this method.
      """
      pass

  async def run_hash_deep_now(self):
    if self._not_enabled():
      return 0
    self.logger.info(f"Starting hash_deep")

    pass
