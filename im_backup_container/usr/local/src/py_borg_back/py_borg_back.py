import subprocess
import logging
import toml
import sys
import os
import argparse
from datetime import datetime
from croniter import croniter
import time
import unittest.mock as mock
import stat
from icecream import ic
import asyncio
import signal

repo_general_defaults = {
  'cron_interval': 'R 0 * * *',
  'keep_daily': 7,
  'keep_weekly': 4,
  'keep_monthly': 2,
  'keep_yearly': 1,
  'compact': True,
  'prune': True,
  'dry_run': False,
}

borg_path = '/usr/bin/borg'

async def read_stream(stream, cb):
  while True:
    line = await stream.readline()
    if line:
      cb(line)
    else:
      break

# https://stackoverflow.com/a/77461729/4479969
def td_format(td, pad=True):
    try:
        _days, parsed = str(td).split(",")
    except ValueError:
        hours, minutes, seconds = str(td).split(":")
    else:
        days = _days.split(" ")[0]
        _hours, minutes, seconds = parsed.split(":")
        hours = int(_hours) + int(days) * 24

    if pad:
        hours = hours.zfill(2)

    return f"{hours}:{minutes}:{seconds}"

class Repo:
  def __init__(self, name, config, config_args):
    self.name = name
    # add logger with name
    self.logger = logging.getLogger(f"repo.{name}")
    self._load_extract_config(config, config_args)
    self.current_subprocess = None

  def _load_extract_config(self, config, config_args):
    self.logger.info(f"Loading config for repo \"{self.name}\"")

    # cron str
    self.cron_interval_str = self._load_config_key(config, 'cron_interval', croniter.is_valid)
    self.next_run = croniter(self.cron_interval_str, datetime.now()).get_next(datetime)

    def validate_keep_int(keep_int):
      if not isinstance(keep_int, int):
        self.logger.error(f"Keep value is not an int")
        return False
      if keep_int < 0:
        self.logger.error(f"Keep value is less than 0")
        return False
      return True
    self.keep_daily = self._load_config_key(config, 'keep_daily', validate_keep_int)
    self.keep_weekly = self._load_config_key(config, 'keep_weekly', validate_keep_int)
    self.keep_monthly = self._load_config_key(config, 'keep_monthly', validate_keep_int)
    self.keep_yearly = self._load_config_key(config, 'keep_yearly', validate_keep_int)

    def validate_bool(bool_value):
      if not isinstance(bool_value, bool):
        self.logger.error(f"Value is not a bool")
        return False
      return True
    self.prune = self._load_config_key(config, 'prune', validate_bool)
    self.compact = self._load_config_key(config, 'compact', validate_bool)

    def validate_pass_file(file_path):
      # check if file exists and is readable
      if not os.path.isfile(file_path):
        self.logger.error(f"File \"{file_path}\" does not exist")
        return False
      if not os.access(file_path, os.R_OK):
        self.logger.error(f"File \"{file_path}\" is not readable")
        return False
      return True
    borg_pass_file = self._load_config_key(config, 'borg_pass_file', validate_pass_file)
    with open(borg_pass_file, 'r') as f:
      self.borg_passphrase = f.read().strip()

    self.enabled = self._load_config_key(config, 'enabled', validate_bool, True)

    def validate_ssh_key(file_path):
      if not validate_pass_file(file_path):
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
        self.logger.error(f"Folder \"{os.path.dirname(file_path)}\" permissions are not 700")
        return False
      return True
    self.ssh_key_file = self._load_config_key(config, 'ssh_key_file', validate_ssh_key, None)

    def validate_string(string):
      if not isinstance(string, str):
        self.logger.error(f"Value is not a string")
        return False
      return True
    self.repo_url = self._load_config_key(config, 'repo_url', validate_string)

    def validate_list_of(liste, item_validator):
      if not isinstance(liste, list):
        self.logger.error(f"Value is not a list")
        return False
      for item in liste:
        if not item_validator(item):
          return False
      return True
    self.files_include = self._load_config_key(config, 'files_include', lambda x: validate_list_of(x, validate_string))
    self.files_exclude = self._load_config_key(config, 'files_exclude', lambda x: validate_list_of(x, validate_string))

    self.hostname = self._load_config_key(config, 'hostname', validate_string)

    self.dry_run = self._load_config_key(config, 'dry_run', validate_bool)
    if config_args.dry_run:
      self.dry_run = True
    if self.dry_run:
      self.logger.info(f"Dry run enabled")

  def _load_config_key(self, config, key, validator=None, default=None):
    general_config = config['repo_general']
    repo_config = config['repo'][self.name]

    config_value = None
    if key in repo_config:
      config_value = repo_config[key]
    elif key in general_config:
      config_value = general_config[key]
    elif key in repo_general_defaults:
      config_value = repo_general_defaults[key]
    elif default is not None:
      config_value = default
    else:
      self.logger.error(f"Failed to find config key \"{key}\" in repo \"{self.name}\"")
      sys.exit(1)

    if validator is not None:
      if not validator(config_value):
        self.logger.error(f"Config key \"{key}\" in repo \"{self.name}\" failed validation")
        sys.exit(1)
    return config_value

  def _not_enabled(self):
    if not self.enabled:
      self.logger.info(f"Skipping repo \"{self.name}\" is not enabled.")
      return True
    return False

  def _create_borg_env(self):
    # create _Env object for subprocess
    env = os.environ.copy()
    # https://borgbackup.readthedocs.io/en/stable/quickstart.html
    env['BORG_PASSPHRASE'] = self.borg_passphrase
    # https://borgbackup.readthedocs.io/en/stable/deployment/automated-local.html
    # No one can answer if Borg asks these questions, it is better to just fail quickly
    # instead of hanging.
    env['BORG_RELOCATED_REPO_ACCESS_IS_OK'] = 'no'
    env['BORG_UNKNOWN_UNENCRYPTED_REPO_ACCESS_IS_OK'] = 'no'

    env['BORG_RSH'] = 'ssh -oBatchMode=yes -i ' + self.ssh_key_file
    env['BORG_REPO'] = self.repo_url
    return env

  async def _run_async_subprocess(self, cmd, log_prefix):
    """
    Run an async subprocess command and handle logging.

    Args:
        cmd (list[str]): The command and its arguments to be executed.

    Returns:
        int: The return code of the subprocess.
    """
    env = self._create_borg_env()
    borg_logger = logging.getLogger(f"repo.{self.name}.borg.{log_prefix}")
    now = datetime.now()

    return_code = 1
    try:
      process = await asyncio.subprocess.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
        cwd="/host",
      )

      await asyncio.gather(
        read_stream(process.stdout, lambda line: borg_logger.info(line.decode())),
        read_stream(process.stderr, lambda line: borg_logger.error(line.decode())),
      )

      self.current_subprocess = process
      await process.wait()
      elapsed = datetime.now() - now
      elapsed_str = td_format(elapsed)

      if process.returncode != 0:
        borg_logger.error(f"Failed to run \"{log_prefix}\" in {elapsed_str}. Exit: {process.returncode}")
      else:
        borg_logger.info(f"\"{log_prefix}\" finished in {elapsed_str}")
      return_code = process.returncode
    except Exception as e:
      elapsed = datetime.now() - now
      elapsed_str = td_format(elapsed)
      borg_logger.exception(f"Failed to run \"{log_prefix}\" in {elapsed_str}")

    self.current_subprocess = None
    return return_code

  async def run_backup(self):
    if self._not_enabled():
      return 0

    if datetime.now() < self.next_run:
      self.logger.debug(f"Skipping backup for repo \"{self.name}\". Cron interval not met. Next run: {self.next_run.strftime('%Y/%m/%d %H:%M:%S')}")
      return 0

    ret = await self.run_backup_now()
    self.next_run = croniter(self.cron_interval_str, datetime.now()).get_next(datetime)
    return ret

  async def run_backup_now(self):
    ret_create = await self.run_backup_create()
    ret_prune = await self.run_backup_prune()
    ret_compact = await self.run_backup_compact()
    return ret_create + ret_prune + ret_compact

  async def stop_subprocess(self):
    if self.current_subprocess is not None:
      self.logger.info(f"Stopping subprocess for repo \"{self.name}\"")
      self.current_subprocess.terminate()
      await self.current_subprocess.wait()

  async def run_backup_create(self):
    if self._not_enabled():
      return 0
    self.logger.info(f"Running backup create for repo \"{self.name}\"")

    cmd = [
      borg_path,
      'create',
      '--filter', 'AMEds',
      '--list',
      '--stats',
      '--show-rc',
      '--compression', 'zstd',
      '--exclude-caches',
    ]
    if self.dry_run:
      cmd.append('--dry-run')
    if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
      cmd.append('--verbose')
    for file_exclude in self.files_exclude:
      cmd.append('--exclude')
      cmd.append(file_exclude)
    cmd.append(f'::{self.hostname}-{{now}}')
    for file_include in self.files_include:
      cmd.append(file_include)

    self.logger.debug(f"Running command: \"{' '.join(cmd)}\"")
    return await self._run_async_subprocess(cmd, 'create')

  async def run_backup_prune(self):
    if self._not_enabled():
      return 0
    if not self.prune:
      self.logger.info(f"Skipping backup prune for repo \"{self.name}\"")
      return 0
    self.logger.info(f"Running backup prune for repo \"{self.name}\"")

    cmd = [
      borg_path,
      'prune',
      '--list',
      '--glob-archives', f'{self.hostname}-*',
      '--show-rc',
      '--keep-daily', str(self.keep_daily),
      '--keep-weekly', str(self.keep_weekly),
      '--keep-monthly', str(self.keep_monthly),
      '--keep-yearly', str(self.keep_yearly),
    ]
    if self.dry_run:
      cmd.append('--dry-run')
    if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
      cmd.append('--verbose')

    self.logger.debug(f"Running command: \"{' '.join(cmd)}\"")
    return await self._run_async_subprocess(cmd, 'prune')

  async def run_backup_compact(self):
    if self._not_enabled():
      return 0
    if not self.compact:
      self.logger.info(f"Skipping backup compact for repo \"{self.name}\"")
      return 0
    self.logger.info(f"Running backup compact for repo \"{self.name}\"")

    cmd = [
      borg_path,
      'compact',
    ]
    if self.dry_run:
      cmd.append('--dry-run')
    if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
      cmd.append('--verbose')

    self.logger.debug(f"Running command: \"{' '.join(cmd)}\"")
    return await self._run_async_subprocess(cmd, 'compact')

def setup_logging(config_args) -> None:
  log_level = logging.getLevelName(config_args.log_level.upper())
  logging.basicConfig(level=log_level, format='%(asctime)s %(levelname)s %(name)s %(message)s')

def log_borg_version() -> None:
  logging.debug("Running \"borg --version\"")
  try:
    cmd_version_result = subprocess.run(['borg', '--version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if cmd_version_result.returncode != 0:
      logging.error("Failed to run \"borg --version\".\n" + ic.format({"exit": cmd_version_result.returncode, "stderr": cmd_version_result.stderr.decode(), "stdout": cmd_version_result.stdout.decode()}))
      sys.exit(1)
    logging.info(f"Borg version: {cmd_version_result.stdout.decode().strip()}")
  except Exception as e:
    logging.exception(f"Failed to run \"borg --version\". Is it installed?")
    sys.exit(1)

class BackupManager:
  def __init__(self, config_args):
    self.config_args = config_args
    self.load_config_file()
    self.repos = []
    for repo_name in self.get_repo_names():
      self.repos.append(Repo(repo_name, self.config, config_args))
  async def run(self):
    while True:
      for repo in self.repos:
        await repo.run_backup()
      await asyncio.sleep(60)
  async def run_backup(self):
    for repo in self.repos:
      await repo.run_backup_now()
  def load_config_file(self) -> None:
    try:
      logging.info(f"Reading config file: \"{self.config_args.config_file}\"")
      with open(self.config_args.config_file, 'r') as f:
        self.config = toml.load(f)
      logging.debug(f"Config parsed")
    except Exception as e:
      logging.exception(f"Failed to read config file \"{self.config_args.config_file}\"")
      sys.exit(1)
  def get_repo_names(self) -> list[str]:
    return self.config['repo'].keys()
  async def shutdown(self):
    # Gracefully stop running tasks and subprocesses
    for repo in self.repos:
        await repo.stop_subprocess()
    asyncio.get_event_loop().stop()

def signal_handler(signum, frame):
    if signum == signal.SIGTERM or signum == signal.SIGINT or signum == signal.SIGQUIT:
      logging.info(f"Received signal {signal.strsignal(signum)}, initiating graceful shutdown...")
      asyncio.create_task(backup_manager.shutdown())

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Backup Manager')
  parser.add_argument('-c', '--config', help='Path to config file', required=True)
  parser.add_argument('--dry-run', help='Dry run', action='store_true')
  parser.add_argument('--log-level', help='Log level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO')
  parser.add_argument('--single-run-now', help='Run backup once. Begin immediately.', action='store_true')
  args = parser.parse_args()

  config_args = mock.Mock()
  config_args.config_file = args.config
  config_args.dry_run = args.dry_run
  config_args.log_level = args.log_level

  setup_logging(config_args)

  log_borg_version()

  global backup_manager
  backup_manager = BackupManager(config_args)

  signal.signal(signal.SIGTERM, signal_handler)
  signal.signal(signal.SIGINT, signal_handler)
  signal.signal(signal.SIGQUIT, signal_handler)

  if args.single_run_now:
    logging.info("Running single run now")
    asyncio.run(backup_manager.run_backup())
  else:
    logging.info("Running as foreground daemon")
    asyncio.run(backup_manager.run())
  logging.debug("__main__ finished")
