
from glob import glob
import json
import os

## Replace vizual.load and vizual.unload with data.*
for module in glob('vizier/vt/*/modules/*'):
  if not module.endswith(".bak"):
    try:
      with open(module) as f:
        data = json.load(f)

      package = data["command"]["packageId"]
      command = data["command"]["commandId"]
      if package == "vizual":
        if command == "load" or command == "unload":      
          print("Upgrading Module {} (vizual.{} -> data.{})".format(module, command, command))
          os.rename(module, module+".bak")
          data["command"]["packageId"] = "data"
          with open(module, "w") as f:
            json.dump(data, f)
    except Exception as e:
      print("Error processing Module {}".format(module))
      print(e)
