# modules génériques
import argparse
import pandas as pd
import numpy as np
import re
from datetime import datetime
from tqdm import tqdm
import logging

# modules custom
from modules import route_sftp, agenda, route_data_gouv, allocation, route_atlasante, route_postgre, stock, creneaux



def __main__(args) :
    if args.domaine not in ["agenda", "creneaux", "alloc","stock_fluide","stock_dispostock"] :
        print(" - - - Erreur : commande inconnue. Veuillez sélectionner une commande existante.")
        return
    if args.verbose :
        print(" - - Verbose active")
    if args.date :
        pattern_date = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$"
        if bool(re.match(pattern_date, args.date)) :
            print(" - - Date : " + args.date)
        else :
            print(" - - - Erreur : Format date incorrect.")
            return
    # domaine agenda
    if args.domaine == "agenda" :
        if args.commande == "import" :
            import_agenda_sftp(date= args.date, config=args.config, verbose=args.verbose)
        elif args.commande == "import_wget":
            import_wget_agenda_sftp(date=args.date, config=args.config, verbose=args.verbose)
        elif args.commande == "process" :
            generate_agenda_raw(date= args.date, verbose=args.verbose)
        elif args.commande == "process_OD" :
            generate_OD(date= args.date, verbose=args.verbose)
        elif args.commande == "process_OD_VM" :
            generate_OD_VM(date=args.date, verbose=args.verbose)
        elif args.commande == "control" :
            control_agenda(date= args.date, verbose=args.verbose)
        elif args.commande == "publish_sftp_ars" :
            publish_agenda_sftp_ars(date= args.date, config=args.config, verbose=args.verbose)
        elif args.commande == "publish_ftplib_sftp_ars":
            publish_agenda_ftplib_sftp_ars(date=args.date, config=args.config, verbose=args.verbose)
        elif args.commande == "publish_sftp_alloc" :
            publish_agenda_sftp_alloc(date= args.date, config=args.config, verbose=args.verbose)
        elif args.commande == "publish_opendata" :
            publish_agenda_opendata(date= args.date, config=args.config, env_publication=args.datagouv, verbose=args.verbose)
        elif args.commande == "clean_sftp" :
            clean_sftp(date= args.date, config=args.config, verbose=args.verbose)
        else :
            print(" - - - Erreur : commande inconnue pour le domaine agenda. Veuillez sélectionner une commande existante.")
    #creneaux d agenda
    if args.domaine == "creneaux" :
        if args.commande == "import" :
            import_creneaux_sftp(date= args.date, config=args.config, verbose=args.verbose)
        elif args.commande == "process" :
            generate_creneaux(date= args.date, verbose=args.verbose)
        else :
            print(" - - - Erreur : commande inconnue pour le domaine creneaux. Veuillez sélectionner une commande existante.")
    # domaine alloc
    elif args.domaine == "alloc" :
        if args.commande == "import" :
            import_alloc(date= args.date, config=args.config, verbose=args.verbose)
        elif args.commande == "process" :
            generate_rapp_alloc(date= args.date, verbose=args.verbose)
        else :
            print(" - - - Erreur : commande inconnue pour le domaine alloc. Veuillez sélectionner une commande existante.")
    elif args.domaine == "stock_fluide" :
        if args.commande == "import" :
            import_fluide(date= args.date, config=args.config, verbose=args.verbose)
        if args.commande == "process" :
            generate_stock_fluide_tot(date= args.date, config=args.config, verbose=args.verbose)
        else :
            print(" - - - Erreur : commande inconnue pour le domaine stock_fluide. Veuillez sélectionner une commande existante.")
    elif args.domaine == "stock_dispostock" :
        if args.commande == "import" :
            import_dispostock(date= args.date, config=args.config, verbose=args.verbose)
        if args.commande == "process" :
            generate_stock_dispostock_tot(date= args.date, config=args.config, verbose=args.verbose)
        else :
            print(" - - - Erreur : commande inconnue pour le domaine stock_dispostock. Veuillez sélectionner une commande existante.")
    return


#
# __ AGENDA ___
#

# import des donnees

def import_agenda_sftp(date= datetime.today().strftime("%Y-%m-%d"), config="config/config.json", verbose=True) :
    # recuperation des agenda sur le serveur sftp distant
    server_in_sftp = route_sftp.read_config_sftp(config,"ATLASANTE SFTP INPUT")
    for operateur in ["maiia","keldoc","doctolib"] :
        route_sftp.save_agenda_op_sftp(operateur, server_in_sftp, date=date, verbose=verbose)
    return
    
def import_wget_agenda_sftp(date=datetime.today().strftime("%Y-%m-%d"), config="config.config.json", verbose=True) :
    # recuperation des agenda sur le serveur sftp distant
    server_in_sftp = route_sftp.read_config_sftp(config,"ATLASANTE SFTP INPUT")
    for operateur in ["maiia","keldoc","doctolib"] :
        route_sftp.save_wget_agenda_op_sftp(operateur, server_in_sftp, date=date, verbose=verbose)
    return

def generate_agenda_raw(date = datetime.today().strftime("%Y-%m-%d"), config="config/config.json", verbose=True) :
    param = agenda.read_config_agenda(config)
    # recuperation et normalisation des daframes
    process_agenda_raw("maiia", date=date, verbose=verbose)
    process_agenda_raw("keldoc", date=date, verbose=verbose)
    process_agenda_raw("doctolib", date=date, verbose=verbose)
    return

def process_agenda_raw(operateur, date = datetime.today().strftime("%Y-%m-%d"), config="config/config.json", verbose=True) :
    param = agenda.read_config_agenda(config)
    reader = agenda.load_agenda_op(operateur, 1000000, date=date)
    # initialisation constante err
    data_err = {}
    for chunk in reader :
        df_op_chunk_filter, data_err = agenda.filter_err_agenda(chunk,
            operateur=operateur,
            treshold_control=param["treshold_control"],
            path_log_err=param["log"],
            data_err=data_err)
        df_op_processed = agenda.norm_agenda(df_op_chunk_filter, operateur)
        agenda.save_append_agenda(df_op_processed, folder = "data/agenda/", date=date,  postfix="-raw")
    if data_err :
        if (data_err["err_row"]/data_err["tot_row"]) > float(param["treshold_control"]) :
            # alerte dataframe
            print(" - - ATTENTION ALERTE : " + operateur + " invalide : trop de lignes en erreur.")
            print(" - - ATTENTION ALERTE : le fichier enregistré est possiblement corrompu, merci de consulter le fichier de statut.")
        agenda.save_data_err(data_err)
    return

# generation des fichiers OD

def generate_OD(date = datetime.today().strftime("%Y-%m-%d"), config="config/config.json", verbose=True) :
    param = agenda.read_config_agenda(config)
    df_agenda_op = agenda.load_agenda_raw(date=date, verbose=verbose)
    agenda.save_agenda_gzip(df_agenda_op, folder = "data/agenda/", date=date, postfix="-raw")
    # creation des JDD OPENDATA et ARS
    if verbose :
        print(" - - Enregistrement des fichiers opendata ...")
    df_centre, df_dep, df_reg, df_national, df_centre_ars = agenda.aggregate(df_agenda_op, date_init= datetime(2021, 1, 18), date=date, duree = param["borne_publication_opendata"], verbose=verbose)
    # sauvegarde ARS
    if verbose :
        print(" - - Enregistrement des fichiers ARS ...")
    L_region = ['HDF', 'ARA', 'IDF', 'GUY', 'OCC', 'NAQ', 'GES', 'PAC', 'COR',
       'BRE', 'GDP', 'BFC', 'MAR', 'REU', 'NOR', 'CVL', 'PDL']
    for region in L_region :
        df_agenda_reg = agenda.filter_raw_reg(df_centre_ars, region)
        agenda.save_agenda(df_agenda_reg, folder = "data/agenda/ars/", date=date, postfix=("_"+str(region)))
    #sauvegarde opendata
    agenda.save_agenda(df_centre, folder = "data/agenda/opendata/", date=date, postfix="_par_centre")
    agenda.save_agenda(df_dep, folder = "data/agenda/opendata/", date=date, postfix="_par_dep")
    agenda.save_agenda(df_reg, folder = "data/agenda/opendata/", date=date, postfix="_par_reg")
    agenda.save_agenda(df_national, folder = "data/agenda/opendata/", date=date, postfix="_national")
    # sauvegarde ars
    agenda.save_agenda(df_centre_ars, folder = "data/agenda/", date=date, postfix="")
    agenda.save_agenda_xlsx(df_centre_ars, folder = "data/agenda/", date=date, postfix="")
    return

# Génération des fichiers OD adaptée à la VM centos

def generate_OD_VM(date = datetime.today().strftime("%Y-%m-%d"), config="config/config.json", verbose=True) :
    param = agenda.read_config_agenda(config)
    df_agenda_op = agenda.load_agenda_raw_vm(date=date, verbose=verbose)
    agenda.save_agenda_gzip_vm(df_agenda_op, folder = "data/agenda/", date=date, postfix="-raw")
    # creation des JDD OPENDATA et ARS
    if verbose :
        print(" - - Enregistrement des fichiers opendata ...")
    df_centre, df_dep, df_reg, df_national, df_centre_ars = agenda.aggregate_vm(df_agenda_op, date_init= datetime(2021, 1, 18), date=date, duree = param["borne_publication_opendata"], verbose=verbose)
    # sauvegarde ARS
    if verbose :
        print(" - - Enregistrement des fichiers ARS ...")
    L_region = ['HDF', 'ARA', 'IDF', 'GUY', 'OCC', 'NAQ', 'GES', 'PAC', 'COR',
       'BRE', 'GDP', 'BFC', 'MAR', 'REU', 'NOR', 'CVL', 'PDL']
    for region in L_region :
        df_agenda_reg = agenda.filter_raw_reg(df_centre_ars, region)
        agenda.save_agenda_vm(df_agenda_reg, folder = "data/agenda/ars/", date=date, postfix=("_"+str(region)))
    # sauvegarde opendata
    agenda.save_agenda_vm(df_centre, folder = "data/agenda/opendata/", date=date, postfix="_par_centre")
    agenda.save_agenda_vm(df_dep, folder = "data/agenda/opendata/", date=date, postfix="_par_dep")
    agenda.save_agenda_vm(df_reg, folder = "data/agenda/opendata/", date=date, postfix="_par_reg")
    agenda.save_agenda_vm(df_national, folder = "data/agenda/opendata/", date=date, postfix="_national")
    # sauvegarde ars
    agenda.save_agenda_vm(df_centre_ars, folder = "data/agenda/", date=date, postfix="")
    agenda.save_agenda_xlsx_vm(df_centre_ars, folder = "data/agenda/", date=date, postfix="")
    return


# controle

def control_agenda(date = datetime.today().strftime("%Y-%m-%d"), config="config/config.json", verbose=True) :
    df_in_raw = agenda.load_agenda_raw(date=date, verbose=verbose)
    if verbose :
        print(" - - Controles fonctionnels des fichiers du " + date + "...")
    agenda.control_RG(df_in_raw)
    return

# publication

def publish_agenda_opendata(date = datetime.today().strftime("%Y-%m-%d"), config="config/config.json", env_publication="DATA DEMO", verbose=True) :
    api_config = route_data_gouv.read_config_api(config,str(env_publication))
    route_data_gouv.publish_rdvs(api_config, date = date, verbose=verbose)
    return

def publish_agenda_sftp_ars(date = datetime.today().strftime("%Y-%m-%d"), config="config/config.json", verbose=True) :
    server_out_sftp = route_sftp.read_config_sftp(config,"ATLASANTE SFTP DEPOT")
    # publication du fichier brut + doublon publié sans la date dans le prefixe
    publi_alloc = [{
        "path_local" : "data/agenda/" + date + " - prise_rdv-raw.csv.gz",
        "path_sftp" : date + " - prise_rdv-raw.csv.gz"
        },
        {
        "path_local" : "data/agenda/" + date + " - prise_rdv-raw.csv",
        "path_sftp" : date + " - prise_rdv-raw.csv"
        }]
    route_sftp.publish_agenda_sftp(server_out_sftp, *publi_alloc, date=date, verbose=verbose)
    # publication du fichier ARS
    publi_alloc = [
        {
        "path_local" : "data/agenda/" + date + " - prise_rdv.csv",
        "path_sftp" : date + " - prise_rdv.csv"
        },
        {
        "path_local" : "data/agenda/" + date + " - prise_rdv.xlsx",
        "path_sftp" : date + " - prise_rdv.xlsx"
        }]
    route_sftp.publish_agenda_sftp(server_out_sftp, *publi_alloc, date=date, verbose=verbose)
    # publication des fichiers ARS
    L_region = ['HDF', 'ARA', 'IDF', 'GUY', 'OCC', 'NAQ', 'GES', 'PAC', 'COR', 
        'BRE', 'GDP', 'BFC', 'MAR', 'REU', 'NOR', 'CVL', 'PDL']
    publi_region = []
    for region in L_region :
        publi_region.append({
            "path_local" : "data/agenda/ars/" + date + " - prise_rdv_" + region + ".csv",
            "path_sftp" : "prise_rdv_" + region + ".csv"
        })
    publi_region = publi_region
    route_sftp.publish_agenda_sftp(server_out_sftp, *publi_region, date=date, verbose=verbose)
    return

def publish_agenda_ftplib_sftp_ars(date = datetime.today().strftime("%Y-%m-%d"), config="config/config.json", verbose=True):
    server_out_sftp = route_sftp.read_config_sftp(config,"ATLASANTE SFTP DEPOT")
    #path_local = "data/agenda/" + date + " - prise_rdv-raw.csv.gz"
    #path_sftp = "test/" + date + " - prise_rdv-raw.csv.gz"
    #route_sftp.publish_agenda_ftplib_sftp_1_file(server_out_sftp, path_local, path_sftp, date=date, verbose=verbose)
    #path_local = "data/agenda/" + date + " - prise_rdv.xlsx"
    #path_sftp = "test/" + date + " - prise_rdv.xlsx"
    #route_sftp.publish_agenda_ftplib_sftp_1_file(server_out_sftp, path_local, path_sftp, date=date, verbose=verbose)
    #path_local = "data/agenda/" + date + " - prise_rdv.csv"
    #path_sftp = "test/" + date + " - prise_rdv.csv"
    #route_sftp.publish_agenda_ftplib_sftp_1_file(server_out_sftp, path_local, path_sftp, date=date, verbose=verbose)
    path_local = "data/agenda/" + date + " - prise_rdv-raw.csv"
    path_sftp = "test/" + date + " - prise_rdv-raw.csv"
    route_sftp.publish_agenda_ftplib_sftp_1_file(server_out_sftp, path_local, path_sftp, date=date, verbose=verbose)
    # publication des fichiers ARS
    L_region = ['HDF', 'ARA', 'IDF', 'GUY', 'OCC', 'NAQ', 'GES', 'PAC', 'COR',
        'BRE', 'GDP', 'BFC', 'MAR', 'REU', 'NOR', 'CVL', 'PDL']
    publi_region = []
    for region in L_region :
        publi_region.append({
            "path_local" : "data/agenda/ars/" + date + " - prise_rdv_" + region + ".csv",
            "path_sftp" :"test/prise_rdv_" + region + ".csv"
        })
    publi_region = publi_region
    route_sftp.publish_agenda_sftp(server_out_sftp, *publi_region, date=date, verbose=verbose)    
    return

def publish_agenda_sftp_alloc(date = datetime.today().strftime("%Y-%m-%d"), config="config/config.json", verbose=True) :
    server_out_sftp = route_sftp.read_config_sftp(config,"ATLASANTE RAPPROCHEMENT")
    publi_alloc = {
        "path_local" : "data/allocation/" + date +  " - prise_rdv-hebdo.csv",
        "path_sftp" : "prise_rdv-hebdo.csv"
    }
    route_sftp.publish_agenda_sftp(server_out_sftp, publi_alloc, date=date, verbose=verbose)
    return

def clean_sftp(date = datetime.today().strftime("%Y-%m-%d"), config="config/config.json", verbose=True) :
    server_out_sftp = route_sftp.read_config_sftp(config,"ATLASANTE SFTP DEPOT")
    path_sftp_csv_raw = date + " - prise_rdv-raw.csv"
    path_sftp_csv = date + " - prise_rdv.csv"
    path_sftp_gz = date + " - prise_rdv-raw.csv.gz"
    path_sftp_xlsx = date + " - prise_rdv.xlsx"
    route_sftp.clean_agenda_sftp(server_out_sftp, path_sftp_csv, path_sftp_csv_raw, path_sftp_gz, path_sftp_xlsx, verbose=verbose)
    return

#
# __ CRENEAUX ___
#

def import_creneaux_sftp(date= datetime.today().strftime("%Y-%m-%d"), config="config/config.json", verbose=True) :
    # recuperation des agenda sur le serveur sftp distant
    server_in_sftp = route_sftp.read_config_sftp(config,"ATLASANTE SFTP INPUT")
    for operateur in ["maiia","keldoc","doctolib"] :
        route_sftp.save_creneaux_op_sftp(operateur, server_in_sftp, date=date, verbose=verbose)
    return

def generate_creneaux(date = datetime.today().strftime("%Y-%m-%d"), config="config/config.json", verbose=True) :
    # recuperation et normalisation des daframes
    df_maiia = creneaux.load_creneaux_op("maiia", date=date)
    df_keldoc = creneaux.load_creneaux_op("keldoc", date=date)
    df_doctolib = creneaux.load_creneaux_op("doctolib", date=date)
    # operation de normalisation
    df_maiia = creneaux.norm_creneaux(df_maiia, operateur = "maiia")
    df_keldoc = creneaux.norm_creneaux(df_keldoc, operateur = "keldoc")
    df_doctolib = creneaux.norm_creneaux(df_doctolib, operateur = "doctolib")
    df_creneaux_op = creneaux.concat_operateur_creneaux(df_maiia, df_keldoc, df_doctolib)
    # sauvegarde creneaux
    if verbose :
        print(" - - Enregistrement des fichiers bruts des creneaux ...")
    creneaux.save_creneaux(df_creneaux_op, folder = "data/creneaux/", date=date, postfix="")
    return

#
# __ ALLOC ___
#

def import_alloc(date= datetime.today().strftime("%Y-%m-%d"), config="config/config.json", verbose=True) :
    ressource_atlasante = route_atlasante.read_config_ressource(config, "alloc")
    route_atlasante.save_file_atlasante(ressource_atlasante, date=date, verbose=verbose)
    return

def generate_rapp_alloc(date= datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    # transformation
    df_in_agenda = allocation.load_agenda_raw(date , verbose=True)
    df_in_agenda = allocation.transform_agenda_raw(df_in_agenda)
    agenda.save_agenda(df_in_agenda, folder = "data/allocation/", date=date, postfix="-hebdo")
    df_in_alloc = allocation.load_allocation(date , verbose=True)
    df_in_alloc = allocation.transform_allocation(df_in_alloc)
    df_rapp_agenda_alloc = allocation.rapprochement(df_in_agenda, df_in_alloc)
    # sauvegarde
    allocation.save_rapprochement(df_rapp_agenda_alloc, date=date, verbose=verbose)
    return

#
# __ STOCK ___
#

# FLUIDE

def import_fluide(date= datetime.today().strftime("%Y-%m-%d"), config="config/config.json", verbose=True) :
    fluide_config = stock.read_config_stock(config)
    param_config = route_postgre.read_config_database(config, server="LOCAL SERVER")
    df_fluide_raw = stock.load_fluide(date=date, verbose=verbose)
    df_stock_plateforme = stock.process_fluide(df_fluide_raw, fluide_config["dic_ratio_ucd"], verbose=verbose)
    print(df_stock_plateforme)
    route_postgre.insert_df_stock_plateforme(df_stock_plateforme, host=param_config["host"], user=param_config["username"], password=param_config["password"], verbose =True )
    stock.save_fluide(df_stock_plateforme, folder = "data/stock/fluide/opendata/", date=date, postfix="-quotidien", verbose=verbose)
    return

def generate_stock_fluide_tot(date= datetime.today().strftime("%Y-%m-%d"), config="config/config.json", verbose=True) :
    param_config = route_postgre.read_config_database(config, server="LOCAL SERVER")
    df_stock_plateforme = route_postgre.fetch_df_stock_plateforme(host=param_config["host"], user=param_config["username"], password=param_config["password"], verbose =verbose)
    stock.save_fluide(df_stock_plateforme, folder = "data/stock/fluide/opendata/", date=date, postfix="", verbose=verbose)
    return

# DISPOSTOCK

def import_dispostock(date= datetime.today().strftime("%Y-%m-%d"), config="config/config.json", verbose=True) :
    param_config = route_postgre.read_config_database(config, server="LOCAL SERVER")
    df_dispostock_raw = stock.load_dispostock(date=date, verbose=verbose)
    df_stock_es = stock.process_dispostock(df_dispostock_raw, date=date, verbose=verbose)
    print(df_stock_es)
    route_postgre.insert_df_stock_es(df_stock_es, host=param_config["host"], user=param_config["username"], password=param_config["password"], verbose =True )
    stock.save_dispostock(df_stock_es, folder = "data/stock/dispostock/opendata/", date=date, postfix="_quotidien", verbose=verbose)
    return

def generate_stock_dispostock_tot(date= datetime.today().strftime("%Y-%m-%d"), config="config/config.json", verbose=True) :
    param_config = route_postgre.read_config_database(config, server="LOCAL SERVER")
    df_stock_es = route_postgre.fetch_df_stock_es(host=param_config["host"], user=param_config["username"], password=param_config["password"], verbose =verbose)
    df_ret_es_finess, df_ret_es_dep, df_ret_es_reg, df_ret_es_national = stock.aggregate_es(df_stock_es, date=date, verbose=verbose)
    stock.save_dispostock(df_ret_es_finess, folder = "data/stock/dispostock/opendata/", date=date, postfix="_finess", verbose=verbose)
    stock.save_dispostock(df_ret_es_dep, folder = "data/stock/dispostock/opendata/", date=date, postfix="_dep", verbose=verbose)
    stock.save_dispostock(df_ret_es_reg, folder = "data/stock/dispostock/opendata/", date=date, postfix="_reg", verbose=verbose)
    stock.save_dispostock(df_ret_es_national, folder = "data/stock/dispostock/opendata/", date=date, postfix="_national", verbose=verbose)
    return

#
# __ INIT ___
#

# initialisation tqdm
tqdm.pandas()

# initialisation du parsing
parser = argparse.ArgumentParser()
parser.add_argument("domaine", type=str, help="Domaine disponible : agenda et alloc")
parser.add_argument("commande", type=str, help="Commande à exécuter")
parser.add_argument("-v", "--verbose", help="affiche le debuggage",
                    type=bool)
parser.add_argument("-c", "--config", help="Fichier de configuration",
                    type=str, default="config/config.json")
parser.add_argument("-d", "--date", help="Date de publication des fichiers, par defaut date d'aujourd'hui",
                    type=str, default = datetime.today().strftime("%Y-%m-%d"))
parser.add_argument("--datagouv", help="environnement de publication pour l'opendata",
                    type=str, default = "DATA DEMO")
args = parser.parse_args()

#logging
logging.basicConfig(filename="log/log_debug.log",
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S',
                            level=logging.DEBUG)
logging.basicConfig(filename="log/log_info.log",
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S',
                            level=logging.INFO)
logging.basicConfig(filename="log/log_warning.log",
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S',
                            level=logging.WARNING)

logging.info("Utilisation de l'utilitaire.")


# coeur
if __name__ == "__main__":
    __main__(args)
