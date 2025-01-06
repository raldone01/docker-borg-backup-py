from croniter import croniter
import os
import stat
import logging


class Validation:
  def __init__(self, logger):
    self.logger = logger

  def validate_cron_interval(self, cron_interval):
    if cron_interval == False:
      return True
    if not croniter.is_valid(cron_interval):
      self.logger.error(f"Cron interval \"{cron_interval}\" is not valid")
      return False
    return True

  def validate_int_positive(self, keep_int):
    if not isinstance(keep_int, int):
      self.logger.error(f"Keep value is not an int")
      return False
    if keep_int < 0:
      self.logger.error(f"Keep value is less than 0")
      return False
    return True

  def validate_bool(self, bool_value):
    if not isinstance(bool_value, bool):
      self.logger.error(f"Value is not a bool")
      return False
    return True

  def validate_pass_file(self, file_path):
    # check if file exists and is readable
    if not os.path.isfile(file_path):
      self.logger.error(f"File \"{file_path}\" does not exist")
      return False
    if not os.access(file_path, os.R_OK):
      self.logger.error(f"File \"{file_path}\" is not readable")
      return False
    return True

  def validate_ssh_key(self, file_path):
    if not self.validate_pass_file(file_path):
      return False
    # check if the file (600) and folder (700) permissions are correct

    # Get file and folder permissions
    file_permissions = os.stat(file_path).st_mode
    folder_permissions = os.stat(os.path.dirname(file_path)).st_mode

    # Check if file permissions are 600
    if not bool(file_permissions & stat.S_IRUSR) or not bool(file_permissions & stat.S_IWUSR):
      self.logger.error(f"File \"{file_path}\" permissions are not 600")
      return False

    # Check if folder permissions are 700
    if not bool(folder_permissions & stat.S_IRUSR) or not bool(folder_permissions & stat.S_IWUSR) or not bool(folder_permissions & stat.S_IXUSR):
      self.logger.error(f"Folder \"{os.path.dirname(
        file_path)}\" permissions are not 700")
      return False
    return True

  def validate_string(self, string):
    if not isinstance(string, str):
      self.logger.error(f"Value is not a string")
      return False
    return True

  def validate_list_of(self, liste, item_validator):
    if not isinstance(liste, list):
      self.logger.error(f"Value is not a list")
      return False
    for item in liste:
      if not item_validator(item):
        return False
    return True

  def validate_log_level(logger):
    def validate_log_level_inner(log_level):
      if not isinstance(log_level, str):
        logger.error(f"Log level is not a string")
        return False
      if isinstance(logging.getLevelName(log_level.upper()), str):
        logger.error(f"Log level \"{log_level}\" is not valid")
        return False
      return True
    return validate_log_level_inner
