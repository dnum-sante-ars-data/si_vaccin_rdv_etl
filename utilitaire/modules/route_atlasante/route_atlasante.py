import pandas as pd
import json
from datetime import datetime, timedelta
from datetime import date
import requests
import logging


# retourne la config d'une ressource au format dictionnaire
def read_config_ressource(path_in, ressource_name) :
    with open(path_in) as f:
        dict_ret = json.load(f)
    L_ret = dict_ret["atlasante"]
    ressource_config = {}
    for ressource in L_ret :
        if ressource["ressource"] == ressource_name :
            ressource_config = ressource.copy()
    logging.info("Lecture config Atlasante " + path_in + ".")
    return ressource_config

def get_file(url, path_save, verbose=True) :
    logging.info("Recuperation depuis " + str(url) + " vers " + path_save)
    if verbose :
        print("Recuperation depuis " + str(url) + " vers " + path_save + "...")
    r = requests.get(url, allow_redirects=True)
    open(path_save, 'wb').write(r.content)
    if verbose :
        print(path_save + " enrgistre.")
    return

def save_file_atlasante(ressource, date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    path_out = "data/allocation/atlasante/" + date + " - covid_vaccin_allocation_src.csv"
    get_file(ressource["url"], path_out, verbose=verbose)
    return

