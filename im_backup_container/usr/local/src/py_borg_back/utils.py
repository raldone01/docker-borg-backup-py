from datetime import timedelta
import logging

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
