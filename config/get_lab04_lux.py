import requests
from requests.auth import HTTPBasicAuth
import json
import yaml

# Load credentials from secrets.yaml file
with open("secrets.yaml", "r") as file:
    secrets = yaml.safe_load(file)

username = secrets["DSlab04_username"] # Load username from secrets.yaml
password = secrets["DSlab_password"] # Load password from secrets.yaml

url = "http://x.x.x.x/status" # (Local static ip - hidden) Replace url with yours device local ip and the /status

response = requests.get(url, auth=HTTPBasicAuth(username, password))

data = response.json()

filtered = {
    "lux": data["lux"]["value"],
    "illumination": data["lux"]["illumination"],
    "temp": data["tmp"]["value"],
    "unixtime": data["unixtime"], 
    "time": data["time"]
    }

print(json.dumps(filtered, indent=1))
