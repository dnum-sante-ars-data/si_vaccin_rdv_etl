import json
import pandas as pd
from datetime import datetime, timedelta
from datetime import date
from os import walk
from icecream import ic
import re
import logging

#
# PARSING des fichiers dispostock
#

# retrouve dans le répertoire local le dernier fichier des stocks DISPOSTOCK
# par defaut la date du jour d execution est selectionnee
def get_stock_dispostock_loc(date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    file_name_ret = ""
    _, _, filenames = next(walk("data/stock/dispostock/raw/"))
    pattern_f = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]_[0-9][0-9]h[0-9][0-9]_stock_vaccin_covid_dispostock" + ".csv$"
    if filenames :
        # filtrage des fichiers interessants
        filenames = [x for x in filenames if re.match(pattern_f, x)]
        filenames = [x for x in filenames if datetime.strptime(x[0:10],"%Y-%m-%d") == datetime.strptime(date,"%Y-%m-%d")]
    else :
        print(" - - - Erreur : aucun fichier DISPOSTOCK en local")
        logging.info("Aucun fichier operateur en local.")
        raise FileNotFoundError
    if filenames :
        dates = list(map(lambda x : datetime.strptime(x[0:10],"%Y-%m-%d"),filenames))
        index_max = dates.index(max(dates))
        file_name_ret = filenames[index_max]
    else :
        print(" - - - Erreur : aucun fichier DISPOSTOCK en local mis a jour pour le " + 
                        date + ".")
        logging.info("Aucun fichier DISPOSTOCK en local mis a jour pour le " + 
                        date + ".")
        raise FileNotFoundError
    return("data/stock/dispostock/raw/" + file_name_ret)

def load_dispostock(date=datetime.today().strftime("%Y-%m-%d"), verbose =True) :
    path_in = get_stock_dispostock_loc(date=date, verbose=verbose)
    df_ret = pd.read_csv(path_in, index_col=False, sep=";", dtype=str, usecols=["Ucd vaccin", "Nom vaccin", "IPE", "Finess Site : Finess géo de la PUI", "Nb doses", "Date de la dernière mise à jour", "Nombre d'UCD", "PUI Pivot ?"])
    df_ret.rename({"Ucd vaccin": "UCD_vaccin",
             "Nom vaccin": "nom_vaccin",
             "IPE": "IPE",
             "Finess Site : Finess géo de la PUI": "finess",
             "Nb doses": "nb_doses",
             "Date de la dernière mise à jour": "date_maj",
             "Nombre de doses injection 2": "nb_doses_injection_2",
             "Nombre d'UCD": "nb_UCD",
             "PUI Pivot ?": "PUI_pivot"}, axis='columns',inplace=True)
    if verbose :
        print("- - - Chargement de " + path_in + " termine : " + str(len(df_ret)) + " lignes chargees.")
    return df_ret

# transformation au format pour l'enregistrement en BDD

def process_dispostock(df_in, date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    df_ret = df_in.copy()
    # filtrage type de vaccin
    df_ret = df_ret[df_ret["UCD_vaccin"].isin(["3400890009454", "3400890009676","3400890010047"])]
    df_ret = df_ret[df_ret["PUI_pivot"] == "Oui"]
    # affectation type vaccin
    df_ret["type_de_vaccin"] ="NR"
    df_ret.loc[df_ret["UCD_vaccin"] == "3400890009454","type_de_vaccin"] = "Pfizer"
    df_ret.loc[df_ret["UCD_vaccin"] == "3400890009676","type_de_vaccin"] = "Moderna"
    df_ret.loc[df_ret["UCD_vaccin"] == "3400890010047","type_de_vaccin"] = "AstraZeneca"
    # mise au format date
    df_ret["date"] = date
    df_ret["date_maj"] = df_ret.loc[df_ret["date_maj"].notnull(),"date_maj"] = pd.to_datetime(df_ret.loc[df_ret["date_maj"].notnull(),"date_maj"], format='%d/%m/%Y %H:%M', errors='coerce').dt.date
    # passage en int
    df_ret["nb_doses"] =  df_ret["nb_doses"].apply(int)
    df_ret["nb_UCD"] =  df_ret["nb_UCD"].apply(int)
    df_ret_finess_date = df_ret[["nb_UCD","nb_doses","finess","type_de_vaccin","date","date_maj"]]
    return df_ret_finess_date

# transformation pour l'opendata d'un dump de BDD

def aggregate_es(df_in, date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    # chargement des utilitaires
    df_finess = pd.read_csv("utils/liste_es_pivots.csv",sep=";",dtype="str", usecols=["finess", "libelle_pui"])
    df_dep = pd.read_csv("utils/departement2019.csv",sep=",",dtype="str")
    df_reg = pd.read_csv("utils/region2019.csv",sep=",",dtype="str")
    df_es_finess = pd.merge(df_in, df_finess, left_on=["finess"], right_on=["finess"], how="left")
    # code departement
    df_es_finess["code_departement"] = df_es_finess["finess"].str.slice(start=0, stop=2)
    df_es_finess.loc[df_es_finess["finess"].str.slice(start=0, stop=2) == "97","code_departement"] = df_es_finess["finess"].str.slice(start=0, stop=2) + df_es_finess["finess"].str.slice(start=3, stop=4)
    df_es_finess.loc[df_es_finess["finess"].str.slice(start=0, stop=2) == "98","code_departement"] = "973"
    # jointure sur la region via le departement
    df_es_finess = df_es_finess.merge(df_dep[["dep","reg","libelle"]], how="left",
        left_on="code_departement", right_on="dep")
    df_es_finess = df_es_finess.merge(df_reg[["reg","libelle3"]], how="left",
        left_on="reg", right_on="reg")
    df_es_finess.rename(columns={"libelle3": "region", "libelle": "departement","reg" : "code_region", "nb_UCD" : "nb_ucd"},inplace=True)
    df_es_finess_raw = df_es_finess.copy()
    # aggregation selon les différents niveaux
    df_es_dep = df_es_finess_raw.groupby(["code_departement","departement", "type_de_vaccin","date"], as_index=False).agg(
                nb_doses=pd.NamedAgg(column='nb_doses', aggfunc=sum), 
                nb_ucd=pd.NamedAgg(column='nb_ucd', aggfunc=sum)).reset_index()
    df_es_dep = df_es_dep[["code_departement","departement","type_de_vaccin","nb_doses","nb_ucd","date"]]
    df_es_reg = df_es_finess_raw.groupby(["code_region","region", "type_de_vaccin","date"], as_index=False).agg(
                nb_doses=pd.NamedAgg(column='nb_doses', aggfunc=sum), 
                nb_ucd=pd.NamedAgg(column='nb_ucd', aggfunc=sum)).reset_index()
    df_es_reg = df_es_reg[["code_region","region","type_de_vaccin","nb_doses","nb_ucd","date"]]
    df_es_national = df_es_finess_raw.groupby(["type_de_vaccin","date"], as_index=False).agg(
                nb_doses=pd.NamedAgg(column='nb_doses', aggfunc=sum), 
                nb_ucd=pd.NamedAgg(column='nb_ucd', aggfunc=sum)).reset_index()
    df_es_national = df_es_national[["type_de_vaccin","nb_doses","nb_ucd","date"]]
    df_es_finess = df_es_finess_raw[["code_departement","departement","libelle_pui","finess","type_de_vaccin","nb_doses","nb_ucd","date"]]
    return df_es_finess, df_es_dep, df_es_reg, df_es_national

#
# SAUVEGARDE
#

def save_dispostock(df_in, folder = "data/stock/dispostock/opendata/", date=datetime.today().strftime("%Y-%m-%d"), postfix="", verbose=True) :
    path_out = folder + date + " - stocks_es" + postfix + ".csv"
    df_in.to_csv(path_out, 
    index=False, na_rep="",sep=";",encoding="utf-8")
    logging.info(path_out + " enregistre.")
    if verbose :
        print(" - - - Enregistrement de " + path_out + " termine.")
    return