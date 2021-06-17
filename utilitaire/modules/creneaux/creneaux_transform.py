import json
import pandas as pd
from datetime import datetime, timedelta, date
from os import walk
import re
import logging
from tqdm import tqdm

#
# LECTURE CONFIG
#

def read_config_creneaux(path_in) :
    with open(path_in) as f:
        dict_ret = json.load(f)
    L_ret = dict_ret["parametrage"]
    param_config = {}
    for param in L_ret :
        if param["domaine"] == "creneaux" :
            param_config = param.copy()
    logging.info("Lecture configuration creneaux " + path_in + ".")
    return param_config


# chargement

# par defaut la date du jour d execution est selectionnee
def get_creneaux_op_loc(operateur, date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    file_name_ret = ""
    _, _, filenames = next(walk("data/creneaux/"+str(operateur)))
    pattern_f = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-"+ str(operateur) + "-plages-horaires.csv$"
    if filenames :
        # filtrage des fichiers interessants
        filenames = [x for x in filenames if re.match(pattern_f, x)]
        filenames = [x for x in filenames if datetime.strptime(x[0:10],"%Y-%m-%d") <= datetime.strptime(date,"%Y-%m-%d")]
    else :
        print(" - - - Erreur : aucun fichier operateur en local")
        logging.info("Aucun fichier operateur en local.")
        raise FileNotFoundError
    if filenames :
        dates = list(map(lambda x : datetime.strptime(x[0:10],"%Y-%m-%d"),filenames))
        index_max = dates.index(max(dates))
        file_name_ret = filenames[index_max]
    else :
        print(" - - - Erreur : aucun fichier operateur en local mis a jour avant le " + 
                        date +
                        " pour " +
                        operateur + ".")
        logging.info("Aucun fichier operateur en local mis a jour avant le " + 
                        date +
                        " pour " +
                        operateur + ".")
        raise FileNotFoundError
    return("data/creneaux/" + str(operateur) + "/" + file_name_ret)


def load_creneaux_op(operateur, date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    # chemin défini génériquement
    path_in = get_creneaux_op_loc(operateur, date=date, verbose=verbose)
    if verbose :
        print(" - - - Chargement " + path_in + " ...")
    if operateur == "maiia" :
        df_ret = pd.read_csv(path_in,
        sep=",",encoding="utf-8",
        usecols=[
            "date_rdv","id_centre","nom_centre","cp_centre","nb_heures","nb_lignes"],
        dtype =str)
        df_ret["type_vaccin"] = "NR"
        df_ret["nb_creneaux"] = "NR"
        df_ret.rename(columns={"date_rdv": "date"},inplace=True)
    elif operateur == "keldoc" :
        df_ret = pd.read_csv(path_in,
        sep=";",encoding="utf-8",
        usecols=[
            "date","id_centre","nom_centre","cp_centre",
            "type_vaccin","nb_lignes",
            "nb_creneaux","nb_heures"],
        dtype=str)
    elif operateur == "doctolib" :
        df_ret = pd.read_csv(path_in,
        sep=",",encoding="utf-8",
        usecols=[
            "date","id_centre","nom_centre","cp_centre",
            "nb_heures","nb_lignes",
            "nb_créneaux","type_vaccin"], 
        dtype=str)
        df_ret.rename(columns={"nb_créneaux": "nb_creneaux"},inplace=True)
    else :
        raise ValueError
    if verbose :
        print(" - - - Fichier " + path_in + " charge avec succes.")
    logging.info(path_in + " charge en tant que fichier source creneaux operateur.")
    return df_ret

# formattage des cp_centre numérique keldoc
def format_cp_centre(cp_in) :
    # non nan
    cp_ret = cp_in
    if cp_in :
        if cp_in[0] != "0" :
            if cp_in[0] == " " :
                cp_in = cp_in[1:]
                cp_ret = cp_in
            if len(cp_in) == 4 :
                cp_ret = "0" + cp_in
    return cp_ret  

def norm_creneaux(df_in, operateur="maiia", verbose=True) :
    if verbose :
        print(" - - - Normalisation des creneaux " + str(operateur) + " ...")
    # chargement des utilitaires
    df_dep = pd.read_csv("utils/departement2019.csv",sep=",",dtype="str")
    df_reg = pd.read_csv("utils/region2019.csv",sep=",",dtype="str")
    if operateur not in ["maiia", "doctolib", "keldoc"] :
        print(" - - - Erreur : Type operateur agenda inconnu : " + str(operateur))
        raise ValueError
    df_ret = df_in.copy()
    df_ret.loc[df_ret["cp_centre"].notnull(),"cp_centre"] = df_ret.loc[df_ret["cp_centre"].notnull(),"cp_centre"].apply(lambda x : format_cp_centre(str(x)))
    #
    # operation specifique selon les operateurs
    #
    if operateur == "maiia" :
        # maiia
        df_ret["operateur"] = "maiia"
        # definition code departement
        df_ret["code_departement"] = df_ret["cp_centre"].str.slice(start=0, stop=2)
        df_ret.loc[df_ret["cp_centre"].str.slice(start=0, stop=2) == "97","code_departement"] = df_ret["cp_centre"].str.slice(start=0, stop=3)
        # remplacement département corse
        df_ret.loc[
            (df_ret["cp_centre"].fillna(0).astype(int) >= 20000) & 
            (df_ret["cp_centre"].fillna(0).astype(int) < 20200),"code_departement"] = "2A"
        df_ret.loc[
            (df_ret["cp_centre"].fillna(0).astype(int) >= 20200) & 
            (df_ret["cp_centre"].fillna(0).astype(int) <= 20999),"code_departement"] = "2B"
    elif operateur == "keldoc" :
        # keldoc
        df_ret["operateur"] = "keldoc"
        # definition code departement
        df_ret["code_departement"] = df_ret["cp_centre"].str.slice(start=0, stop=2)
        df_ret.loc[df_ret["cp_centre"].str.slice(start=0, stop=2) == "97","code_departement"] = df_ret["cp_centre"].str.slice(start=0, stop=3)
        # remplacement département corse
        df_ret.loc[
            (df_ret["cp_centre"].fillna(0).astype(int) >= 20000) & 
            (df_ret["cp_centre"].fillna(0).astype(int) < 20200),"code_departement"] = "2A"
        df_ret.loc[
            (df_ret["cp_centre"].fillna(0).astype(int) >= 20200) & 
            (df_ret["cp_centre"].fillna(0).astype(int) <= 20999),"code_departement"] = "2B"
        # normalisation des creneaux renseignés
        df_ret["nb_creneaux"] = df_ret["nb_creneaux"].replace({" -   ": ""})
        df_ret["nb_heures"] = df_ret["nb_heures"].replace({" -   ": ""})
        df_ret["nb_lignes"] = df_ret["nb_lignes"].replace({" -   ": ""})
        # normalisation des dates 
        df_ret["date"] = pd.to_datetime(df_ret["date"])
        df_ret["date"] = df_ret["date"].apply(str)
    elif operateur == "doctolib" :
        # keldoc
        df_ret["operateur"] = "doctolib"
        # definition code departement
        df_ret["code_departement"] = df_ret["cp_centre"].str.slice(start=0, stop=2)
        df_ret.loc[df_ret["cp_centre"].str.slice(start=0, stop=2) == "97","code_departement"] = df_ret["cp_centre"].str.slice(start=0, stop=3)
        # remplacement département corse
        # on prend le cp avant le " "
        df_ret.loc[
            (df_ret["cp_centre"].fillna(0).apply(lambda x : int(str(x).split(" ")[0])) >= 20000) & 
            (df_ret["cp_centre"].fillna(0).apply(lambda x : int(str(x).split(" ")[0])) < 20200),"code_departement"] = "2A"
        df_ret.loc[
            (df_ret["cp_centre"].fillna(0).apply(lambda x : int(str(x).split(" ")[0])) >= 20200) & 
            (df_ret["cp_centre"].fillna(0).apply(lambda x : int(str(x).split(" ")[0])) <= 20999),"code_departement"] = "2B"
    #
    df_ret = df_ret.merge(df_dep[["dep","reg"]], how="left",
        left_on="code_departement", right_on="dep")
    # jointure sur la region via le departement
    df_ret = df_ret.merge(df_reg[["reg","libelle3"]], how="left",
        left_on="reg", right_on="reg")
    df_ret.rename(columns={"libelle3": "region","reg" : "code_region"},inplace=True)
    #
    df_ret.loc[df_ret["code_departement"].isin(["2A","2B"]),"code_region"] = "94"
    df_ret.loc[df_ret["code_departement"].isin(["2A","2B"]),"region"] = "COR"
    # reordonnonancement des colonnes
    df_ret = df_ret[["date",
        "id_centre","nom_centre","cp_centre",
        "code_departement",
        "code_region","region",
        "nb_heures","nb_lignes","nb_creneaux","type_vaccin",
        "operateur"]]
    return df_ret


# concatenation des trois flux operateurs
def concat_operateur_creneaux(*df_ops) :
    df_ret = pd.concat(list(df_ops),ignore_index=True)
    df_ret = df_ret[["date",
        "id_centre","nom_centre","cp_centre",
        "code_departement",
        "code_region","region",
        "nb_heures","nb_lignes","nb_creneaux","type_vaccin",
        "operateur"]]
    return df_ret


# Sauvegarde

def save_creneaux(df_in, folder = "data/creneaux/", date=datetime.today().strftime("%Y-%m-%d"),  postfix="", verbose=True) :
    path_out = folder + date + " - creneaux_rdv" + str(postfix) + ".csv"
    df_in.to_csv(path_out, 
    index=False, na_rep="",sep=",",encoding="utf-8")
    logging.info(path_out + " enregistre.")
    if verbose :
        print(" - - - Enregistrement de " + path_out + " termine.")
    return