import os

def debug_is_on():
  return str(os.environ.get("DEBUG", "")) != ""

