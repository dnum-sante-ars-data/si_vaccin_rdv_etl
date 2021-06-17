import json
import pandas as pd
import requests

from datetime import datetime
from datetime import date

#
# FONCTION GENERIQUE
#

# retourne la config d'un serveur au format dictionnaire
def read_config_api(path_in, api_name) :
    with open(path_in) as f:
        dict_ret = json.load(f)
    L_ret = dict_ret["data_gouv"]
    api_config = {}
    for api in L_ret :
        if api["api"] == api_name :
            api_config = api.copy()
    return api_config

def api_url(path, api_config):
    return (api_config["url"] + path)

def publish_file(path_in, api_config, id_res, verbose=True) :
    url = api_url('/datasets/{}/resources/{}/upload/'.format(api_config["dataset_id"], id_res), api_config)
    headers = {
    'X-API-KEY': api_config["api_key"]
    }
    response = requests.post(url, files={
        'file': open(path_in, 'rb'),
        }, headers=headers)
    if verbose :
        print(" - - - Chargement sur " + api_config["url"] + " de " + path_in + " ...")
        print(" - - - Code retour API : " + str(response.status_code))
        print(" - - - Message API : " + str(response.content))
    return

def publish_rdvs(api_config, date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    # national
    path_national = "data/agenda/opendata/" + str(date) + " - prise_rdv_national.csv"
    publish_file(path_national, api_config, api_config["ressource_id"]["prise-rdv-national"], verbose=verbose)
    # region
    path_reg = "data/agenda/opendata/" + str(date) + " - prise_rdv_par_reg.csv"
    publish_file(path_reg, api_config, api_config["ressource_id"]["prise-rdv-reg"], verbose=verbose)
    # dep
    path_dep = "data/agenda/opendata/" + str(date) + " - prise_rdv_par_dep.csv"
    publish_file(path_dep, api_config, api_config["ressource_id"]["prise-rdv-dep"], verbose=verbose)
    # centre
    path_centre = "data/agenda/opendata/" + str(date) + " - prise_rdv_par_centre.csv"
    publish_file(path_centre, api_config, api_config["ressource_id"]["prise-rdv-centre"], verbose=verbose)
    return