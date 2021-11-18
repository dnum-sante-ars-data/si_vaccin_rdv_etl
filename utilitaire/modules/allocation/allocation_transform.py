import pandas as pd
from datetime import datetime, timedelta, date
import re
import logging

# fonctions d import

def load_agenda_raw(date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    path_in = "data/agenda/" + date + " - prise_rdv-raw.csv"
    df_ret = pd.read_csv(path_in,
        sep=",",
        usecols=[
            "date_creation", "date_rdv", "id_centre", "nom_centre", "cp_centre",
            "code_departement", "code_region", "region", "motif_rdv",
            "rang_vaccinal", "parcours_rdv", "annee_naissance", "honore", "annule",
            "operateur"],
        dtype=str ,encoding="utf-8")
    df_ret["date_rdv"] = pd.to_datetime(df_ret["date_rdv"], format='%Y-%m-%d', errors='raise')
    if verbose :
        print(" - - - Fichier " + path_in + " charge avec succes.")
    logging.info(path_in + " charge.")
    return df_ret

def load_allocation(date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    path_in = "data/allocation/atlasante/" + date + " - covid_vaccin_allocation_src.csv"
    df_ret = pd.read_csv(path_in,
                    sep=";",
                    usecols=["allocation_semaine", "centre_id","nombre_dose"],
                    dtype=str)
    df_ret["allocation_semaine"] = pd.to_datetime(df_ret["allocation_semaine"], format='%Y-%m-%d', errors='raise')
    if verbose :
        print(" - - - Fichier " + path_in + " charge avec succes.")
    logging.info(path_in + " charge.")
    return df_ret

# fonction custom d'aggregat pour les donnees rdvs

def rdv_annul_taux(x) :
    nb_annul = len(x[x == "true"])/len(x)
    return nb_annul

def rdv_annul(x) :
    nb_annul = len(x[x == "true"])
    return nb_annul

def rdv1inj(x) :
    nb_1inj = len(x[x == "1"])
    return nb_1inj

def rdv2inj(x) :
    nb_1inj = len(x[x == "2"])
    return nb_1inj

def rdvNRinj(x) :
    nb_NRinj = len(x[x == "NR"])
    return nb_NRinj

def transform_agenda_raw(df_in) :
    logging.info("Formattage du fichier agenda pour rapprochement avec les allocations")
    df_ret =  df_in.copy()
    df_ret["num_semaine"] = df_ret["date_rdv"].apply(lambda x : x.isocalendar()[1])
    df_ret["debut_semaine"] = df_ret["date_rdv"].apply(lambda x : (x - timedelta(days=x.weekday())).strftime("%Y-%m-%d"))
    #aggregation sur nombre injection
    df_ret = df_ret.groupby(["id_centre","cp_centre", "num_semaine","debut_semaine"]).agg(
                rdv1inj=pd.NamedAgg(column="rang_vaccinal", aggfunc=rdv1inj), 
                rdv2inj=pd.NamedAgg(column="rang_vaccinal", aggfunc=rdv2inj), 
                rdvNRinj=pd.NamedAgg(column="rang_vaccinal", aggfunc=rdvNRinj), 
                rdv_annul=pd.NamedAgg(column='annule', aggfunc=rdv_annul), 
                rdv_annul_taux=pd.NamedAgg(column='annule', aggfunc=rdv_annul_taux)).reset_index()
    df_ret.rename(columns={"id_centre" : "lieu_gid"},inplace=True)
    df_ret = df_ret[["lieu_gid","num_semaine","debut_semaine","rdv1inj","rdv2inj","rdvNRinj","rdv_annul","rdv_annul_taux"]]
    return df_ret

# fonction custom d'aggregat pour les allocations de doses

def sum_doses(x) :
    ret = x.astype(int).sum()
    return ret

def transform_allocation(df_in) :
    logging.info("Formattage du fichier allocation pour rapprochement avec le fichier agenda")
    df_ret =  df_in.copy()
    df_ret["num_semaine"] = df_ret["allocation_semaine"].apply(lambda x : x.isocalendar()[1])
    df_ret["debut_semaine"] = df_ret["allocation_semaine"].apply(lambda x : (x - timedelta(days=x.weekday())).strftime("%Y-%m-%d"))
    df_ret = df_ret.groupby(["centre_id", "num_semaine", "debut_semaine"]).agg(doses1inj=pd.NamedAgg(column="nombre_dose", aggfunc=sum_doses)).reset_index()
    # renommage
    df_ret.rename(columns={"centre_id" : "lieu_gid"},inplace=True)
    return df_ret

def rapprochement(df_centre, df_alloc) :
    df_rapp_agenda_alloc = df_centre.merge(df_alloc, left_on=["lieu_gid", "debut_semaine"], right_on=["lieu_gid", "debut_semaine"], how="inner")
    df_rapp_agenda_alloc.reset_index(inplace=True, drop=True)
    df_rapp_agenda_alloc["doses2inj"] = 0
    df_rapp_agenda_alloc["proj"] = (df_rapp_agenda_alloc["rdv1inj"] + 
                                        df_rapp_agenda_alloc["rdv2inj"] + 
                                        df_rapp_agenda_alloc["rdvNRinj"] - 
                                        df_rapp_agenda_alloc["doses1inj"] - 
                                        df_rapp_agenda_alloc["doses2inj"])
    df_rapp_agenda_alloc.index.name = "id_ligne"
    return df_rapp_agenda_alloc

def save_rapprochement(df_in, folder = "data/allocation/", date=datetime.today().strftime("%Y-%m-%d"), verbose=True):
    path_out = folder + date + " - rapprochement_alloc_rdv" + ".csv"
    df_in.to_csv(path_out, 
    index=False, na_rep="",sep=",",encoding="utf-8")
    logging.info(path_out + " enregistre.")
    if verbose :
        print(" - - - Enregistrement de " + path_out + " termine.")
    return