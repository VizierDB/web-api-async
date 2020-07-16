
from glob import glob
import json
import os

packages = set()

## Replace vizual.load and vizual.unload with data.*
for module in glob('vizier-data/.vizierdb/vt/*/modules/*'):
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
      elif package == "python":
        if command == "code":
          if "format" not in set(arg["id"] for arg in data["command"]["args"]):
            print("Upgrading Module {} (python.{}'s format -> text/html)".format(module, command, command))
            data["command"]["args"].append({
                "id" : "format",
                "value" : "text/html"
              })
            os.rename(module, module+".bak")
            with open(module, "w") as f:
              print(module)
              json.dump(data, f)
      packages.add(package)
    except Exception as e:
      print("Error processing Module {}".format(module))
      print(e)

# print(packages)