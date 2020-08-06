import os

def debug_is_on() -> bool:
  return str(os.environ.get("DEBUG", "")) != ""

