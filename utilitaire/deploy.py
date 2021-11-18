# modules génériques
import argparse
import pandas as pd
import numpy as np
import re
from datetime import datetime

# modules custom
from modules import route_postgre


print(" - Deploiement ...")
param_config = route_postgre.read_config_database("config/config.json", server="LOCAL SERVER")
route_postgre.deploy_database(host=param_config["host"], user=param_config["username"], password=param_config["password"])
route_postgre.init_empty_schema(host=param_config["host"], user=param_config["username"], password=param_config["password"], verbose = True)
print(" - Deploiement termine.")