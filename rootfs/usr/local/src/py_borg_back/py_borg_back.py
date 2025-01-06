import subprocess
import logging
import toml
import sys
import os
import argparse
from datetime import datetime, timedelta, timezone
from croniter import croniter
import unittest.mock as mock
from icecream import ic
import asyncio
import signal

from utils import read_stream, td_format
from config_validation import Validation
from config_defaults import repo_general_defaults
from setup_logging import setup_logging

borg_path = "/usr/bin/borg"
should_exit = False


class Repo:
  def __init__(self, name, config, config_args):
    self.name = name
    # add logger with name
    self.logger = logging.getLogger(f"repo.{name}")
    self._load_extract_config(config, config_args)
    self.current_subprocess = None
    self.next_run_message_time = datetime(1970, 1, 1, tzinfo=timezone.utc)

  def _load_extract_config(self, config, config_args):
    self.logger.debug(f'Loading config for repo "{self.name}"')
    validation = Validation(self.logger)

    self.logger.setLevel(
        self._load_config_key(
            config,
            "log_level",
            Validation.validate_log_level(self.logger),
            config_args=config_args,
        ).upper()
    )

    # cron str
    self.cron_interval_str = self._load_config_key(
        config, "cron_interval", validation.validate_cron_interval
    )
    if self.cron_interval_str != False:
      self.next_run = croniter(self.cron_interval_str, datetime.now()).get_next(
          datetime
      )

    self.keep_daily = self._load_config_key(
        config, "keep_daily", validation.validate_int_positive
    )
    self.keep_weekly = self._load_config_key(
        config, "keep_weekly", validation.validate_int_positive
    )
    self.keep_monthly = self._load_config_key(
        config, "keep_monthly", validation.validate_int_positive
    )
    self.keep_yearly = self._load_config_key(
        config, "keep_yearly", validation.validate_int_positive
    )

    self.prune = self._load_config_key(
        config, "prune", validation.validate_bool)
    self.compact = self._load_config_key(
        config, "compact", validation.validate_bool
    )

    borg_pass_file = self._load_config_key(
        config, "borg_pass_file", validation.validate_pass_file
    )
    with open(borg_pass_file, "r") as f:
      self.borg_passphrase = f.read().strip()

    self.enabled = self._load_config_key(
        config, "enabled", validation.validate_bool
    )

    self.ssh_key_file = self._load_config_key(
        config, "ssh_key_file", validation.validate_ssh_key, None
    )

    self.repo_url = self._load_config_key(
        config, "repo_url", validation.validate_string
    )

    self.files_include = self._load_config_key(
        config,
        "files_include",
        lambda x: validation.validate_list_of(
            x, validation.validate_string),
    )
    self.files_exclude = self._load_config_key(
        config,
        "files_exclude",
        lambda x: validation.validate_list_of(
            x, validation.validate_string),
    )

    self.hostname = self._load_config_key(
        config, "hostname", validation.validate_string
    )

    self.dry_run = self._load_config_key(
        config, "dry_run", validation.validate_bool
    )
    if config_args.dry_run:
      self.dry_run = True
    if self.dry_run:
      self.logger.info(f"Dry run enabled")

  def _load_config_key(
      self, config, key, validator=None, default=None, config_args=None
  ):
    general_config = config["repo_general"]
    repo_config = config["repo"][self.name]

    config_value = None
    if config_args is not None and hasattr(config_args, key) and getattr(self.config_args, key) is not None:
      # logging.debug(f'Using config arg "{key}"')
      config_value = getattr(config_args, key)
    elif key in repo_config:
      # logging.debug(f'Using repo config key "{key}"')
      config_value = repo_config[key]
    elif key in general_config:
      # logging.debug(f'Using general config key "{key}"')
      config_value = general_config[key]
    elif key in repo_general_defaults:
      # logging.debug(f'Using general defaults key "{key}"')
      config_value = repo_general_defaults[key]
    elif default is not None:
      # logging.debug(f'Using default value for "{key}"')
      config_value = default
    else:
      self.logger.error(
          f'Failed to find config key "{key}" in repo "{self.name}"'
      )
      sys.exit(1)

    if validator is not None:
      if not validator(config_value):
        self.logger.error(
            f'Config key "{key}" in repo "{
                self.name}" failed validation'
        )
        sys.exit(1)
    return config_value

  def _not_enabled(self):
    if not self.enabled:
      self.logger.debug(f'Skipping repo "{self.name}" because it is disabled.')
      return True
    return False

  def _create_borg_env(self):
    # create _Env object for subprocess
    env = os.environ.copy()
    # https://borgbackup.readthedocs.io/en/stable/quickstart.html
    env["BORG_PASSPHRASE"] = self.borg_passphrase
    # https://borgbackup.readthedocs.io/en/stable/deployment/automated-local.html
    # No one can answer if Borg asks these questions, it is better to just fail quickly
    # instead of hanging.
    env["BORG_RELOCATED_REPO_ACCESS_IS_OK"] = "no"
    env["BORG_UNKNOWN_UNENCRYPTED_REPO_ACCESS_IS_OK"] = "no"

    env["BORG_RSH"] = "ssh -oBatchMode=yes -i " + self.ssh_key_file
    env["BORG_REPO"] = self.repo_url
    return env

  async def _run_async_subprocess(self, cmd, log_prefix, stderr_is_stdout=True):
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

      def handle_line(cb):
        return lambda line: cb(line.decode("utf-8").strip())

      await asyncio.gather(
          read_stream(process.stdout, handle_line(borg_logger.info)),
          read_stream(
              process.stderr,
              handle_line(
                  borg_logger.info if stderr_is_stdout else borg_logger.error
              ),
          ),
      )

      self.current_subprocess = process
      await process.wait()
      elapsed = datetime.now() - now
      elapsed_str = td_format(elapsed)

      if process.returncode != 0:
        borg_logger.error(
            f'Failed to run "{log_prefix}" in {
                elapsed_str}. Exit: {process.returncode}'
        )
      else:
        borg_logger.info(f'"{log_prefix}" finished in {elapsed_str}')
      return_code = process.returncode
    except Exception as e:
      elapsed = datetime.now() - now
      elapsed_str = td_format(elapsed)
      borg_logger.exception(
          f'Failed to run "{log_prefix}" in {elapsed_str}')

    self.current_subprocess = None
    return return_code

  async def try_run_backup(self):
    if self._not_enabled():
      return 0
    if self.cron_interval_str == False:
      self.logger.debug(
        f'No cron interval for "{self.name}", skipping')
      return 0

    now = datetime.now()
    if now < self.next_run and self.next_run_message_time + timedelta(minutes=10) < now:
      self.logger.debug(
        f'Not time to run "{self.name}" yet (next run: "{self.next_run.strftime('%Y/%m/%d %H:%M:%S')}"), skipping'  # nopep8
      )
      self.next_run_message_time = now
      return 0

    ret = await self.run_backup_now()
    self.next_run = croniter(self.cron_interval_str, datetime.now()).get_next(
        datetime
    )
    return ret

  async def run_backup_now(self, recreate=False):
    global should_exit
    if should_exit:
      return 0
    ret_create = await self.run_backup_create(recreate)
    if should_exit:
      return ret_create
    ret_prune = await self.run_backup_prune()
    if should_exit:
      return ret_create + ret_prune
    ret_compact = await self.run_backup_compact()
    return ret_create + ret_prune + ret_compact

  async def stop_subprocess(self):
    if self.current_subprocess is not None:
      self.logger.info(f'Stopping subprocess for repo "{self.name}"')
      self.current_subprocess.terminate()
      await self.current_subprocess.wait()

  async def run_backup_create(self, recreate=False):
    if self._not_enabled():
      return 0
    self.logger.info(f'Running backup create for repo "{self.name}"')

    cmd = [
        borg_path,
        recreate and "recreate" or "create",
        "--filter",
        "AMEds",
        "--list",
        "--stats",
        "--show-rc",
        "--compression",
        "zstd",
        "--exclude-caches",
    ]
    if self.dry_run:
      cmd.append("--dry-run")
    if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
      cmd.append("--verbose")
    for file_exclude in self.files_exclude:
      cmd.append("--exclude")
      cmd.append(file_exclude)
    cmd.append(f"::{self.hostname}-{{now}}")
    for file_include in self.files_include:
      cmd.append(file_include)

    self.logger.debug(f"Running command: \"{' '.join(cmd)}\"")
    return await self._run_async_subprocess(cmd, "create")

  async def run_backup_prune(self):
    if self._not_enabled():
      return 0
    if not self.prune:
      self.logger.info(f'Skipping backup prune for repo "{self.name}"')
      return 0
    self.logger.info(f'Running backup prune for repo "{self.name}"')

    cmd = [
        borg_path,
        "prune",
        "--list",
        "--glob-archives",
        f"{self.hostname}-*",
        "--show-rc",
        "--keep-daily",
        str(self.keep_daily),
        "--keep-weekly",
        str(self.keep_weekly),
        "--keep-monthly",
        str(self.keep_monthly),
        "--keep-yearly",
        str(self.keep_yearly),
    ]
    if self.dry_run:
      cmd.append("--dry-run")
    if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
      cmd.append("--verbose")

    self.logger.debug(f"Running command: \"{' '.join(cmd)}\"")
    return await self._run_async_subprocess(cmd, "prune")

  async def run_backup_compact(self):
    if self._not_enabled():
      return 0
    if not self.compact:
      self.logger.info(f'Skipping backup compact for repo "{self.name}"')
      return 0
    self.logger.info(f'Running backup compact for repo "{self.name}"')

    cmd = [
        borg_path,
        "compact",
    ]
    if self.dry_run:
      cmd.append("--dry-run")
    if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
      cmd.append("--verbose")

    self.logger.debug(f"Running command: \"{' '.join(cmd)}\"")
    return await self._run_async_subprocess(cmd, "compact")

  async def run_cmd(self, cmd):
    self.logger.info(f'Running custom command for repo "{self.name}"')

    self.logger.debug(f"Running command: \"{' '.join(cmd)}\"")
    return await self._run_async_subprocess(cmd, "custom", False)

  async def break_locks(self):
    self.logger.warning(f'Breaking locks for repo "{self.name}"')

    cmd = [
        borg_path,
        "break-lock",
    ]
    if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
      cmd.append("--verbose")

    self.logger.debug(f"Running command: \"{' '.join(cmd)}\"")
    return await self._run_async_subprocess(cmd, "break-lock")


def log_borg_version() -> None:
  logging.debug('Running "borg --version"')
  try:
    cmd_version_result = subprocess.run(
        ["borg", "--version"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if cmd_version_result.returncode != 0:
      logging.error(
          'Failed to run "borg --version".\n'
          + ic.format(
              {
                  "exit": cmd_version_result.returncode,
                  "stderr": cmd_version_result.stderr.decode("utf-8"),
                  "stdout": cmd_version_result.stdout.decode("utf-8"),
              }
          )
      )
      sys.exit(1)
    logging.info(
        f"Borg version: {
            cmd_version_result.stdout.decode('utf-8').strip()}"
    )
  except Exception as e:
    logging.exception(f'Failed to run "borg --version". Is it installed?')
    sys.exit(1)


class BackupManager:
  def __init__(self, config_args):
    self.config_args = config_args
    self._load_config_file()
    self._load_extract_config(config_args)
    self.repos = []
    for repo_name in self.get_repo_names():
      self.repos.append(Repo(repo_name, self.config, config_args))

  async def run(self):
    global should_exit
    while not should_exit:
      for repo in self.repos:
        await repo.try_run_backup()
      await asyncio.sleep(3)

  async def run_backup_now(self, recreate=False):
    for repo in self.repos:
      await repo.run_backup_now(recreate)

  async def run_cmd(self, cmd):
    for repo in self.repos:
      await repo.run_cmd(cmd)

  async def break_locks(self):
    for repo in self.repos:
      if repo.enabled:
        await repo.break_locks()

  def _load_config_file(self) -> None:
    try:
      logging.info(
        f'Reading config file: "{self.config_args.config_file}"'
      )
      with open(self.config_args.config_file, "r") as f:
        self.config = toml.load(f)
      logging.debug(f"Config parsed")
    except Exception as e:
      logging.exception(
          f'Failed to read config file "{self.config_args.config_file}"'
      )
      sys.exit(1)

  def _load_extract_config(self, config_args) -> None:
    logger = logging.getLogger()
    logger.setLevel(
        self._load_config_key(
            "log_level", Validation.validate_log_level(logger)
        ).upper()
    )

  def _load_config_key(self, key, validator=None, default=None):
    general_config = self.config["repo_general"]

    config_value = None
    if hasattr(self.config_args, key) and getattr(self.config_args, key) is not None:
      # logging.debug(f'Using config arg "{key}"')
      config_value = getattr(self.config_args, key)
    elif key in general_config:
      # logging.debug(f'Using general config key "{key}"')
      config_value = general_config[key]
    elif key in repo_general_defaults:
      # logging.debug(f'Using general defaults key "{key}"')
      config_value = repo_general_defaults[key]
    elif default is not None:
      # logging.debug(f'Using default value for "{key}"')
      config_value = default
    else:
      logging.error(f'Failed to find config key "{key}"')
      sys.exit(1)

    if validator is not None:
      if not validator(config_value):
        logging.error(f'Config key "{key}" failed validation')
        sys.exit(1)
    return config_value

  def get_repo_names(self) -> list[str]:
    return self.config["repo"].keys()

  async def shutdown(self):
    logging.info("Shutting down Backup Manager...")
    # Gracefully stop running tasks and subprocesses
    for repo in self.repos:
      await repo.stop_subprocess()
    global should_exit
    should_exit = True


def signal_handler(signum, frame):
  if signum == signal.SIGTERM or signum == signal.SIGINT or signum == signal.SIGQUIT:
    logging.info(
        f"Received signal {signal.strsignal(
            signum)}, initiating graceful shutdown..."
    )
    asyncio.create_task(backup_manager.shutdown())


async def main():
  parser = argparse.ArgumentParser(description="Backup Manager")
  parser.add_argument(
      "-c", "--config", help="Path to config file", required=True)
  parser.add_argument("--dry-run", help="Dry run", action="store_true")
  parser.add_argument(
      "--log-level",
      help="Log level",
      choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
  )

  run_group = parser.add_mutually_exclusive_group()
  run_group.add_argument("--run-break-locks",
                         help="Break locks", action="store_true")
  run_group.add_argument(
      "--run-single-cmd-now",
      help="Run a custom borg command once. Begin immediately.",
      nargs=argparse.REMAINDER,
  )
  run_group.add_argument(
      "--run-backup-once",
      help="Run backup once. Begin immediately.",
      action="store_true",
  )
  run_group.add_argument(
      "--run-recreate-once",
      help="Run backup recreate once. Begin immediately.",
      action="store_true",
  )
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

  if args.run_break_locks:
    logging.warning("Breaking locks on all repos")
    await backup_manager.break_locks()
  if args.run_single_cmd_now:
    logging.info("Running single cmd on all repos")
    await backup_manager.run_cmd(args.run_single_cmd_now)
  elif args.run_backup_once:
    logging.info("Running single backup on all repos")
    await backup_manager.run_backup_now()
  elif args.run_recreate_once:
    logging.info("Running single recreate on all repos")
    await backup_manager.run_backup_now(True)
  else:
    logging.info("Running as foreground daemon")
    await backup_manager.run()
  logging.debug("__main__ finished")


if __name__ == "__main__":
  asyncio.run(main())
