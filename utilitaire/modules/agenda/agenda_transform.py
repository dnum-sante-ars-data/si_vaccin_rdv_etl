import json
import pandas as pd
from datetime import datetime, timedelta, date
from os import walk, path, remove
import re
import logging
from tqdm import tqdm
import dask
import dask.dataframe as dd

#
# LECTURE CONFIG
#

def read_config_agenda(path_in) :
    with open(path_in) as f:
        dict_ret = json.load(f)
    L_ret = dict_ret["parametrage"]
    param_config = {}
    for param in L_ret :
        if param["domaine"] == "agenda" :
            param_config = param.copy()
    logging.info("Lecture configuration agenda " + path_in + ".")
    return param_config

#
# CHARGEMENT
#

# retrouve dans le répertoire local la dernière version de l'agenda disponible
# par defaut la date du jour d execution est selectionnee
def get_agenda_op_loc(operateur, date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    file_name_ret = ""
    _, _, filenames = next(walk("data/agenda/"+str(operateur)))
    pattern_f = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-"+ str(operateur) + "-rdv.csv$"
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
    return("data/agenda/" + str(operateur) + "/" + file_name_ret)

# chargement par chunck des csv recuperes sur le serveur sftp en fonction de l editeur
def load_agenda_op(operateur, size=1000000, date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    # chemin défini génériquement
    path_in = get_agenda_op_loc(operateur, date=date, verbose=verbose)
    if verbose :
        print(" - - - Chargement " + path_in + " ...")
    if operateur == "maiia" :
        columns_maiia = [
            "date_creation","date_rdv","id_centre","nom_centre","cp_centre",
            "motif_rdv","rang_vaccinal","parcours_rdv",
            "annee_naissance","honore","annule"]
        reader = pd.read_csv(path_in,
        sep=",",encoding="utf-8",
        usecols= columns_maiia,
        dtype =str,
        chunksize=size)
    elif operateur == "keldoc" :
        columns_keldoc = [
            "date_creation","date_rdv","id_centre","nom_centre","cp_centre",
            "motif_rdv","rang_vaccinal","parcours_rdv",
            "annee_naissance","honore","annule","type_vaccin"]
        reader = pd.read_csv(path_in,
        sep=";",encoding="utf-8",
        usecols= columns_keldoc,
        dtype=str,
        chunksize=size)
    elif operateur == "doctolib" :
        columns = pd.read_csv(path_in, 
                        dtype=str,encoding="utf-8", sep=",", nrows=0).columns.tolist()
        if "motif_rdv" in columns : 
            columns_doctolib = [
            "date_creation","date_rdv","id_centre","nom_centre","cp_centre",
            "motif_rdv","rang_vaccinal","parcours_rdv",
            "honore","annule","type_vaccin"]
        else :
            columns_doctolib = [
            "date_creation","date_rdv","id_centre","nom_centre","cp_centre", "rang_vaccinal","parcours_rdv",
            "honore","annule","type_vaccin"]
        reader = pd.read_csv(path_in,
        sep=",",encoding="utf-8",
        usecols=columns_doctolib, 
        dtype=str,
        chunksize=size)
    else :
        raise ValueError
    if verbose :
        print(" - - - Fichier " + path_in + " charge avec succes.")
    logging.info(path_in + " charge en tant que fichier source operateur.")
    return reader

# charge l agenda concatene
def load_agenda_raw(date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    path_in = "data/agenda/" + date + " - prise_rdv-raw.csv"
    df_ret = pd.read_csv(path_in,
        sep=",",
        usecols=[
            "date_creation", "date_rdv", "id_centre", "nom_centre", "cp_centre",
            "code_departement", "code_region", "region", "motif_rdv",
            "rang_vaccinal", "parcours_rdv", "annee_naissance", "honore", "annule",
            "operateur","type_vaccin","rdv_cnam","rdv_rappel"],
        dtype=str ,encoding="utf-8")
    df_ret["date_rdv"] = pd.to_datetime(df_ret["date_rdv"], format='%Y-%m-%d', errors='raise')
    if verbose :
        print(" - - - Fichier " + path_in + " charge avec succes.")
    logging.info(path_in + " charge.")
    return df_ret

# charge l'agenda concatene avec adaptation pour la VM centos
def load_agenda_raw_vm(date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    path_in = "data/agenda/" + date + " - prise_rdv-raw.csv"
    dd_ret = dd.read_csv(path_in,
        sep=",",
        usecols=[
            "date_creation", "date_rdv", "id_centre", "nom_centre", "cp_centre",
            "code_departement", "code_region", "region", "motif_rdv",
            "rang_vaccinal", "parcours_rdv", "annee_naissance", "honore", "annule",
            "operateur","type_vaccin","rdv_cnam","rdv_rappel"],
        dtype=str ,encoding="utf-8")
    dd_ret["date_rdv"] = dd.to_datetime(dd_ret["date_rdv"], format='%Y-%m-%d', errors='raise')
    if verbose :
        print(" - - - Fichier " + path_in + " charge avec succes.")
    logging.info(path_in + " charge.")
    return dd_ret

def load_agenda_raw_chunk(size=1000000, date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    path_in = "data/agenda/" + date + " - prise_rdv-raw.csv"
    df_ret = pd.read_csv(path_in,
        sep=",",
        usecols=[
            "date_creation", "date_rdv", "id_centre", "nom_centre", "cp_centre",
            "code_departement", "code_region", "region", "motif_rdv",
            "rang_vaccinal", "parcours_rdv", "annee_naissance", "honore", "annule",
            "operateur","type_vaccin","rdv_cnam","rdv_rappel"],
        dtype=str ,
        encoding="utf-8",
        chunksize=size)
    #df_ret["date_rdv"] = pd.to_datetime(df_ret["date_rdv"], format='%Y-%m-%d', errors='raise')
    if verbose :
        print(" - - - Fichier " + path_in + " charge avec succes.")
    logging.info(path_in + " charge.")
    return df_ret

def load_agenda_centre_temp(verbose=True, folder = "data/agenda/") :
    # emplacement des fichiers temps par defaut
    path_in = folder + "agenda_centre_temp.csv"
    df_ret = pd.read_csv(path_in,
        sep=",",
        usecols=["date_rdv", "id_centre","nom_centre","cp_centre",
            "code_departement", "code_region","region","rang_vaccinal", "operateur","type_vaccin",
            "nb","nb_rdv_cnam","nb_rdv_rappel"],
        dtype=str ,
        encoding="utf-8")
    df_ret["date_rdv"] = pd.to_datetime(df_ret["date_rdv"], format='%Y-%m-%d', errors='raise')
    df_ret["nb"] = df_ret["nb"].apply(int)
    df_ret["nb_rdv_cnam"] = df_ret["nb_rdv_cnam"].apply(int)
    df_ret["nb_rdv_rappel"] = df_ret["nb_rdv_rappel"].apply(int)
    if verbose :
        print(" - - - Fichier " + path_in + " charge avec succes.")
    logging.info(path_in + " charge.")
    return df_ret

def date_form (df_in):
    df_ret = df_in
    df_ret["date_rdv"] = pd.to_datetime(df_ret["date_rdv"], format='%Y-%m-%d', errors='raise')
    return df_ret

#
# FILTRAGE LIGNES
#

# generation des logs
def log_df(df_in, file_log = "log/agenda/err_agenda_log.log", operateur="", message="") :
    with open(file_log, 'a') as file:
        file.write(datetime.today().strftime("%Y-%m-%d") + " : " + str(len(df_in)) + " lignes concernees." + "\n")
        file.write("Operateur : " +  operateur + "\n")
        file.write(message +  "\n")
        file.close()
    df_in.to_csv(file_log, sep=",", header=True, mode='a',index=False)
    return

def control_date_format_series(s_in, operateur) :
    if operateur == "keldoc" :
        format='%d/%m/%y'
    else :
        format = '%Y-%m-%d'
    s_datetime = pd.to_datetime(s_in,format=format,errors='coerce')
    s_filter = ~s_datetime.isnull()
    return s_filter

def control_cp(cp_in) :
    if cp_in :
        pattern_f = "[0-9][0-9][0-9][0-9][0-9]?( Cedex)?$"
        return bool(re.match(pattern_f, str(cp_in)))
    else :
        # on ne filtre pas les CP vides
        return False

# Filtre le format des inputs operateurs
def filter_err_agenda(df_in, operateur="maiia", treshold_control="0.0001", path_log_err="log/agenda/err_agenda_log.log", data_err={},verbose=True) :
    treshold_control = float(treshold_control)
    # initialisation
    df_ret = df_in
    if operateur == "maiia" :
        df_ret["type_vaccin"] = "NR"
    elif operateur == "doctolib" :
        df_ret["annee_naissance"] = "NR"
        if not ("motif_rdv" in list(df_ret.columns)) : 
            df_ret["motif_rdv"] = "NR"
    count_err = 0
    tot_row = len(df_in)
    if verbose :
        print(" - - - Detection des lignes en erreur pour " + operateur + " ...")
    #
    # controle annule/honnore
    #
    filter_honore = df_ret["honore"].isin(["f","F","FAUX","FALSE","false","t","T","VRAI","TRUE","true"]) | df_ret["honore"].isnull()
    df_err = df_ret[~filter_honore]
    if not df_err.empty :
        logging.warning(" - - - " + operateur + " : erreur de formattage champ 'honore'.")
        logging.warning(" - - - " + str(len(df_err)) + " lignes concernees.")
        log_df(df_err, file_log=path_log_err, operateur=operateur, message="honore invalide.")
        df_ret = df_ret[filter_honore]
        count_err += len(df_err)
    filter_annule = df_ret["annule"].isin(["f","F","FAUX","FALSE","false","t","T","VRAI","TRUE","true"]) | df_ret["annule"].isnull()
    df_err = df_ret[~filter_annule]
    if not df_err.empty :
        logging.warning(" - - - " + operateur + " : erreur de formattage champ 'annule'.")
        logging.warning(" - - - " + str(len(df_err)) + " lignes concernees.")
        log_df(df_err, file_log=path_log_err, operateur=operateur, message="annule invalide.")
        df_ret = df_ret[filter_annule]
        count_err += len(df_err)
    # controle date
    #
    # NB : date_creation peut etre vide mais pas date rdv
    if verbose :
        print(" - - - Verification date_rdv ...")
    filter_date_rdv = control_date_format_series(df_ret["date_rdv"], operateur)
    df_err = df_ret[~filter_date_rdv]
    if not df_err.empty :
        logging.warning(" - - - " + operateur + " : erreur de formattage champ 'date_rdv'.")
        logging.warning(" - - - " + str(len(df_err)) + " lignes concernees.")
        log_df(df_err, file_log=path_log_err, operateur=operateur, message="date_rdv invalide.")
        df_ret = df_ret[filter_date_rdv]
        count_err += len(df_err)
    if verbose :
        print(" - - - Verification date_creation ...")
    df_err = df_ret[~df_ret["date_creation"].isnull()]
    filter_date_creation = control_date_format_series(df_err["date_creation"], operateur)
    df_err = df_err[~filter_date_creation]
    if not df_err.empty :
        logging.warning(" - - - " + operateur + " : erreur de formattage champ 'date_creation'.")
        logging.warning(" - - - " + str(len(df_err)) + " lignes concernees.")
        log_df(df_err, file_log=path_log_err, operateur=operateur, message="date_creation invalide.")
        df_ret = df_ret[filter_date_creation]
        count_err += len(df_err)
    #
    # controle cp_centre
    #
    if verbose :
        print(" - - - Verification cp_centre ...")
    filter_cp = df_ret["cp_centre"].progress_apply(control_cp)
    df_err = df_ret[~filter_cp]
    if not df_err.empty :
        logging.warning(" - - - " + operateur + " : erreur de formattage champ 'cp_centre'.")
        logging.warning(" - - - " + str(len(df_err)) + " lignes concernees.")
        log_df(df_err, file_log=path_log_err, operateur=operateur, message="cp_centre invalide.")
        df_ret = df_ret[filter_cp]
        count_err += len(df_err)
    # enregistrement dans le csv de log
    if data_err == {} :
        data_err = {"date_traitement": [datetime.today().strftime('%Y-%m-%d %H:%M:%S')],
                "operateur": operateur, 
                "tot_row": tot_row,
                "err_row": count_err}
    else :
        # incrémente les erreurs
        data_err["tot_row"] = data_err["tot_row"] + tot_row
        data_err["err_row"] = data_err["err_row"] + count_err
    return df_ret, data_err

# enregistre les erreurs rencontrées
def save_data_err(data_err) :
    df_err_log = pd.DataFrame.from_dict(data_err)
    try :
        if path.isfile("log/agenda/rapport_err_agenda.csv"):
            df_err_log.to_csv("log/agenda/rapport_err_agenda.csv", 
                index=False, na_rep="",sep=";",encoding="utf-8",
                mode="a",header=False)
        else :
            df_err_log.to_csv("log/agenda/rapport_err_agenda.csv", 
                index=False, na_rep="",sep=";",encoding="utf-8",
                mode="w",header=True)
    except :
        print("- - Erreur : enregistrement dans log/agenda/rapport_err_agenda.csv en echec.")
    return

# filtrage des lignes erreurs dans le flux entrant

#
# NORMALISATION
#

# formattage des cp_centre numérique keldoc
def format_cp_centre(cp_in) :
    # non nan
    cp_ret = cp_in
    if cp_in :
        if cp_in[0] != "0" :
            if len(cp_in) == 4 :
                cp_ret = "0" + cp_in
    return cp_ret 

# identification du rdv CNAM
def check_motif_cnam(str_in) :
    if ("CNAM" in str_in.upper()) :
        return "true"
    else :
        return "false"

# identification du rdv rappel
# les patterns déclenchant une qualification en rappel sont ici
def check_motif_rappel(str_in) :
    if ("RAPPEL" in str_in.upper()) :
        return "true"
    elif ("3" in str_in.upper()) :
        return "true"
    elif ("IMMUN" in str_in.upper()) :
        return "true"
    elif ("TROIS" in str_in.upper()) :
        return "true"
    else :
        return "false"

def norm_agenda(df_in, operateur="maiia") :
    # chargement des utilitaires
    df_dep = pd.read_csv("utils/departement2019.csv",sep=",",dtype="str")
    df_reg = pd.read_csv("utils/region2019.csv",sep=",",dtype="str")
    if operateur not in ["maiia", "doctolib", "keldoc"] :
        print(" - - - Erreur : Type operateur agenda inconnu : " + str(operateur))
        raise ValueError
    df_ret = df_in.copy()
    del(df_in)
    # normalisation annule/honnore
    df_ret["honore"].fillna("NR",inplace=True)
    df_ret["annule"].fillna("NR",inplace=True)
    df_ret["honore"].replace(["t","T","VRAI","TRUE"], "true",inplace=True)
    df_ret["honore"].replace(["f","F","FAUX","FALSE"], "false",inplace=True)
    df_ret["annule"].replace(["t","T","VRAI","TRUE"], "true",inplace=True)
    df_ret["annule"].replace(["f","F","FAUX","FALSE"], "false",inplace=True)
    # id_centre
    if df_ret["id_centre"].isnull().values.any() :
        print(" - - -  Attention : id_centre non systématiquement renseigné pour " + str(operateur))
        logging.warning("Attention : id_centre non systematiquement renseigne pour " + str(operateur))
    df_ret["id_centre"].fillna("NR",inplace=True)
    # annee naissance
    if df_ret["annee_naissance"].isnull().values.any() :
        print(" - - -  Attention : annee_naissance non systématiquement renseigné pour " + str(operateur))
        logging.warning("Attention : annee non systematiquement renseigne pour " + str(operateur))
    df_ret["annee_naissance"].fillna("NR",inplace=True)
    # motif_rdv
    df_ret["motif_rdv"].fillna("NR",inplace=True)
    # rang vaccinal
    # 2021/10/08 Mise à jour de la RG
    df_ret.loc[df_ret["rang_vaccinal"] == "R", "rang_vaccinal"] = "3"
    df_ret.loc[df_ret["rang_vaccinal"].str.contains("3", na=False), "rang_vaccinal"] = "3"
    df_ret.loc[df_ret["rang_vaccinal"].str.contains("4", na=False), "rang_vaccinal"] = "4"
    df_ret.loc[~df_ret["rang_vaccinal"].isin(["1","2","3","4","5","6"]), "rang_vaccinal"] = "NR"
    df_ret["rang_vaccinal"].fillna("NR",inplace=True)
    # type vaccin
    df_ret["type_vaccin"].fillna("NR",inplace=True)
    # contrôle date
    if operateur == "maiia" :
        df_ret["date_rdv"] = pd.to_datetime(df_ret["date_rdv"], format='%Y-%m-%d', errors='raise')
    elif operateur == "doctolib" :
        df_ret["date_rdv"] = pd.to_datetime(df_ret["date_rdv"], format='%Y-%m-%d', errors='raise')
    elif operateur == "keldoc" :
        df_ret["date_rdv"] = pd.to_datetime(df_ret["date_rdv"], format="%d/%m/%y", errors='raise')
    # date_rdv remplace par date creation par defaut
    if (operateur == "maiia") or (operateur == "doctolib") :
        df_ret.loc[df_ret["date_creation"].notnull(),"date_creation"] = pd.to_datetime(df_ret.loc[df_ret["date_creation"].notnull(),"date_creation"],
                                                                                                format='%Y-%m-%d',
                                                                                                errors='raise')
        df_ret.loc[df_ret["date_creation"].isnull(),"date_creation"] = df_ret.loc[df_ret["date_creation"].isnull(),"date_rdv"]
    elif (operateur == "keldoc") :
        df_ret.loc[df_ret["date_creation"].notnull(),"date_creation"] = pd.to_datetime(df_ret.loc[df_ret["date_creation"].notnull(),"date_creation"],
                                                                                                format="%d/%m/%y",
                                                                                                errors='raise')
        df_ret.loc[df_ret["date_creation"].isnull(),"date_creation"] = df_ret.loc[df_ret["date_creation"].isnull(),"date_rdv"]
    if df_ret["date_rdv"].dt.year.max() >= 2022 :
        print(" - - - Attention : certains rdv sont positionnés après 2022 pour " + str(operateur))
        logging.info("Attention : certains rdv sont positionnés après 2022 pour " + str(operateur))
        logging.warning("Attention : certains rdv sont positionnés après 2022 pour " + str(operateur))
    # correction sur les CP des centres
    print(" - - - Normalisation code postal pour " + str(operateur) + " ...")
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
    elif operateur == "doctolib" :
        # keldoc
        df_ret["operateur"] = "doctolib"
        # definition code departement
        df_ret["code_departement"] = df_ret["cp_centre"].str.slice(start=0, stop=2)
        df_ret.loc[df_ret["cp_centre"].str.slice(start=0, stop=2) == "97","code_departement"] = df_ret["cp_centre"].str.slice(start=0, stop=3)
        df_ret["parcours_rdv"].fillna("NR", inplace =True)
        # remplacement département corse
        # on prend le cp avant le " "
        df_ret.loc[
            (df_ret["cp_centre"].fillna(0).apply(lambda x : int(str(x).split(" ")[0])) >= 20000) & 
            (df_ret["cp_centre"].fillna(0).apply(lambda x : int(str(x).split(" ")[0])) < 20200),"code_departement"] = "2A"
        df_ret.loc[
            (df_ret["cp_centre"].fillna(0).apply(lambda x : int(str(x).split(" ")[0])) >= 20200) & 
            (df_ret["cp_centre"].fillna(0).apply(lambda x : int(str(x).split(" ")[0])) <= 20999),"code_departement"] = "2B"
     #
    # operation generique post traitement operateur
    print(" - - - Code Postal normalisé")
    # prise de rdv
    df_ret["motif_rdv"].fillna("NR",inplace=True)
    # affectation du boolean rdv_cnam
    df_ret["rdv_cnam"] = df_ret["motif_rdv"].apply(check_motif_cnam)
    # affectation du boolean rdv_rappel
    df_ret["rdv_rappel"] = df_ret["motif_rdv"].apply(check_motif_rappel)
    # cp_centre
    df_ret["cp_centre"].fillna("NR",inplace=True)
    # code_departement
    df_ret["code_departement"].fillna("NR",inplace=True)
    # jointure sur le departement
    df_ret = df_ret.merge(df_dep[["dep","reg"]], how="left",
        left_on="code_departement", right_on="dep")
    # jointure sur la region via le departement
    df_ret = df_ret.merge(df_reg[["reg","libelle3"]], how="left",
        left_on="reg", right_on="reg")
    df_ret.rename(columns={"libelle3": "region","reg" : "code_region"},inplace=True)
    #
    df_ret.loc[df_ret["code_departement"] == "2A","code_region"] = "94"
    df_ret.loc[df_ret["code_departement"] == "2B","code_region"] = "94"
    df_ret.loc[df_ret["code_departement"] == "2A","region"] = "COR"
    df_ret.loc[df_ret["code_departement"] == "2B","region"] = "COR"
    # reordonnonancement des colonnes
    df_ret = df_ret[["date_creation","date_rdv",
        "id_centre","nom_centre","cp_centre",
        "code_departement",
        "code_region","region",
        "motif_rdv","rang_vaccinal","parcours_rdv",
        "annee_naissance",
        "honore","annule",
        "operateur",
        "type_vaccin",
        "rdv_cnam",
        "rdv_rappel"]]
    return df_ret

# concatenation des trois flux operateurs
def concat_operateur_agenda(*df_ops) :
    df_ret = pd.concat(list(df_ops),ignore_index=True)
    df_ret = df_ret[["date_creation","date_rdv",
        "id_centre","nom_centre","cp_centre",
        "code_departement",
        "code_region","region",
        "motif_rdv","rang_vaccinal","parcours_rdv",
        "annee_naissance",
        "honore","annule",
        "operateur",
        "type_vaccin",
        "rdv_cnam",
        "rdv_rappel"]]
    return df_ret

# ARS

# filtre sur le nom de la region
def filter_raw_reg(df_in, nom_reg) :
    L_region = ['HDF', 'ARA', 'IDF', 'GUY', 'OCC', 'NAQ', 'GES', 'PAC', 'COR',
       'BRE', 'GDP', 'BFC', 'MAR', 'REU', 'NOR', 'CVL', 'PDL']
    if str(nom_reg) in L_region :
        df_ret = df_in[df_in["region"] == str(nom_reg)]
    else :
        raise ValueError
    return df_ret

#
# controle fonctionnel
#

def rdv_annul(x) :
    nb_annul = len(x[x == "true"])
    return nb_annul

def rdv_honor(x) :
    nb_annul = len(x[x == "true"])
    return nb_annul

def N_rdv_honor(x) :
    nb_annul = len(x[x == True])
    return nb_annul

# Verifie si les deux RGs suivantes sont valides
#Controle effectué :
#- RG 1 : ALERTE si RDV annulés > RDV pris
#- RG 2 : ALERTE si RDV non-honorés > RDV pris avant la date actuelle
def control_RG(df_in, verbose=True) :
    df_control =  df_in.copy()
    df_control["num_semaine"] = df_control["date_rdv"].apply(lambda x : x.isocalendar()[1])
    df_control["debut_semaine"] = df_control["date_rdv"].apply(lambda x : (x - timedelta(days=x.weekday())).strftime("%Y-%m-%d"))
    # definition fonctionnelle des rdvs non honores
    df_control["N_honor"] = (df_control["annule"] == "false") & (df_control["honore"] == "false")
    #aggregation 
    df_control = df_control.groupby(["id_centre","cp_centre", "num_semaine","debut_semaine"]).agg(
                rdv_annul=pd.NamedAgg(column='annule', aggfunc=rdv_annul), 
                rdv_honore=pd.NamedAgg(column='honore', aggfunc=rdv_honor),
                rdv_non_honore=pd.NamedAgg(column='N_honor', aggfunc=N_rdv_honor)).reset_index()
    # control RG1
    df_RG1 = df_control[df_control["rdv_annul"] > df_control["rdv_honore"]]
    if verbose and not df_RG1.empty :
        print(str(len(df_RG1["cp_centre"].unique())) + " centres ne respectent pas la RG1.")
        logging.info(str(len(df_RG1["cp_centre"].unique())) + " centres ne respectent pas la RG1.")
        logging.warning(str(len(df_RG1["cp_centre"].unique())) + " centres ne respectent pas la RG1.")
    # control RG2
    df_RG2 = df_control[df_control["num_semaine"] < datetime.today().isocalendar()[1]]
    df_RG2 = df_RG2[(df_RG2["rdv_non_honore"] > df_RG2["rdv_honore"])]
    if verbose and not df_RG2.empty :
        print(str(len(df_RG2["cp_centre"].unique())) + " centres ne respectent pas la RG2.")
        logging.info(str(len(df_RG1["cp_centre"].unique())) + " centres ne respectent pas la RG2.")
        logging.warning(str(len(df_RG1["cp_centre"].unique())) + " centres ne respectent pas la RG2.")
    return

#
# OPEN DATA
#

def aggregate(df_in, date_init="", date=datetime.today().strftime("%Y-%m-%d"), duree = 4, verbose=True) :
    # param input
    duree = int(duree)
    if date_init == "" :
        date_init = datetime(2021, 1, 18)
    date = datetime.strptime(date,"%Y-%m-%d")
    logging.info("Date de filtrage pour l'opendata : " + str(date))
    logging.info("Duree de filtrage : " + str(duree))
    df_ret = df_in.copy()
    # filtrage rdv réel
    df_ret = df_ret[df_ret["honore"] != "false"]
    df_ret = df_ret[df_ret["annule"] != "true"]
    df_ret["nb"] = "1"
    # pre agrégation
    logging.info("Debut agregation")
    if verbose :
        print(" - - - Agrégation à la maille centre ...")
    df_ret = df_ret.groupby(["date_rdv", "id_centre","nom_centre","cp_centre",
        "code_departement", "code_region","region","rang_vaccinal", "operateur","type_vaccin"], 
        as_index=False).agg(
            nb=("cp_centre", "count"),
            nb_rdv_cnam=("rdv_cnam", lambda x : len(x[x == "true"])),
            nb_rdv_rappel=("rdv_rappel", lambda x : len(x[x == "true"]))
            )
    if verbose :
        print(" - - - Agrégation à la maille centre terminee")
        print(" - - - Agrégation détaillées ...")
    # aggregation ARS
    df_ret_centre_ars = df_ret.groupby(["code_region", "region", "code_departement", "id_centre", "nom_centre", "rang_vaccinal", "date_rdv","type_vaccin"], as_index=False).agg(
            nb=("nb", "sum"),
            nb_rdv_cnam=("nb_rdv_cnam", "sum"),
            nb_rdv_rappel=("nb_rdv_rappel", "sum")
            )
    # pre filtre sur date pour opendata
    if date.weekday() != 4 :
        print(" - - - Attention, les donnees de la semaine ne sont pas toutes remontees")
    df_ret = df_ret[
        (df_ret["date_rdv"] >= date_init - timedelta(days=date_init.weekday())) &
        (df_ret["date_rdv"] <= date - timedelta(days=date.weekday()) + timedelta(days=duree*7 -1)) ]
    df_ret["date_debut_semaine"] = df_ret["date_rdv"].apply(lambda x : (x - timedelta(days=x.weekday())).strftime("%Y-%m-%d"))
    df_ret = df_ret[df_ret["rang_vaccinal"] != "NR"]
    #agg par centre pour opendata et ARS
    df_ret_centre = df_ret.groupby(["code_region", "region", "code_departement", 
        "id_centre", "nom_centre", "rang_vaccinal", "date_debut_semaine"], as_index=False).agg(
            nb=("nb", "sum"),
            nb_rdv_cnam=("nb_rdv_cnam", "sum"),
            nb_rdv_rappel=("nb_rdv_rappel", "sum")
            )
    #agg par departement
    df_ret_dep = df_ret.groupby(["code_region", "region", "code_departement", "rang_vaccinal", "date_debut_semaine"], as_index=False)["nb"].sum()
    #agg par region
    df_ret_reg = df_ret.groupby(["code_region", "region", "rang_vaccinal", "date_debut_semaine"], as_index=False)["nb"].sum()
    # national
    df_ret_national = df_ret.groupby(["rang_vaccinal", "date_debut_semaine"], as_index=False)["nb"].sum()
    # remise en forme
    df_ret_centre_ars.rename(columns={"code_departement" : "departement"},inplace=True)
    df_ret_centre.rename(columns={"code_departement" : "departement"},inplace=True)
    df_ret_dep.rename(columns={"code_departement" : "departement"},inplace=True)
    df_ret_centre = df_ret_centre[["code_region", "region", "departement", "id_centre", "nom_centre", "rang_vaccinal", "date_debut_semaine", "nb", "nb_rdv_cnam", "nb_rdv_rappel"]]
    df_ret_centre_ars = df_ret_centre_ars[["code_region", "region", "departement", "id_centre", "nom_centre", "rang_vaccinal","type_vaccin","date_rdv", "nb", "nb_rdv_cnam", "nb_rdv_rappel"]]
    df_ret_dep = df_ret_dep[["code_region", "region", "departement", "rang_vaccinal", "date_debut_semaine", "nb"]]
    df_ret_reg = df_ret_reg[["code_region", "region", "rang_vaccinal", "date_debut_semaine", "nb"]]
    df_ret_national = df_ret_national[["rang_vaccinal", "date_debut_semaine", "nb"]]
    # tri
    df_ret_centre.sort_values(by=["region","nom_centre","departement","date_debut_semaine","rang_vaccinal"], inplace=True)
    df_ret_dep.sort_values(by=["departement","region","date_debut_semaine","rang_vaccinal"], inplace=True)
    df_ret_reg.sort_values(by=["region","date_debut_semaine","rang_vaccinal"], inplace=True)
    df_ret_national.sort_values(by=["date_debut_semaine", "rang_vaccinal"], inplace=True)
    #logs et affichage de controle
    logging.info("Aggregation des fichiers reussie.")
    print(" - - - Aggrégation des fichiers réussie. Résumé : ")
    print(df_ret_national)
    return df_ret_centre, df_ret_dep, df_ret_reg, df_ret_national, df_ret_centre_ars

# Optimisation de la fonction d'aggrégation
# aggregate_chunk_centre aggrege par centre une tranche
# agregate_from_centre aggrege a partir de la sortie d'aggregate_chunk_centre les aggregations classiques

def aggregate_chunk_centre(df_in, verbose=True) :
    df_ret = df_in.copy()
    # filtrage rdv réel
    df_ret = df_ret[df_ret["honore"] != "false"]
    df_ret = df_ret[df_ret["annule"] != "true"]
    df_ret["nb"] = "1"
    # pre agrégation
    if verbose :
        print(" - - - Aggrégation par tranche avant reconstitution des aggrégats ...")
    logging.info("Debut agregation tranche")
    df_ret_chunk_centre = df_ret.groupby(["date_rdv", "id_centre","nom_centre","cp_centre",
        "code_departement", "code_region","region","rang_vaccinal", "operateur","type_vaccin"], 
        as_index=False).agg(
            nb=("cp_centre", "count"),
            nb_rdv_cnam=("rdv_cnam", lambda x : len(x[x == "true"])),
            nb_rdv_rappel=("rdv_rappel", lambda x : len(x[x == "true"]))
            )
    if verbose :
        print(" - - - Agrégation de la tranche à la maille centre terminee")
    return df_ret_chunk_centre

def aggregate_from_centre(df_in, date_init="", date=datetime.today().strftime("%Y-%m-%d"), duree = 4, verbose=True) :
    # param input
    duree = int(duree)
    if date_init == "" :
        date_init = datetime(2021, 1, 18)
    date = datetime.strptime(date,"%Y-%m-%d")
    logging.info("Date de filtrage pour l'opendata : " + str(date))
    logging.info("Duree de filtrage : " + str(duree))
    df_ret = df_in.copy()
    df_ret = df_ret.groupby(["date_rdv", "id_centre","nom_centre","cp_centre",
        "code_departement", "code_region","region","rang_vaccinal", "operateur","type_vaccin"], 
        as_index=False).agg(
            nb=("nb", "sum"),
            nb_rdv_cnam=("nb_rdv_cnam", "sum"),
            nb_rdv_rappel=("nb_rdv_rappel", "sum")
            )
    if verbose :
        print(" - - Attention, les donnees de la semaine ne sont pas toutes remontees")
    # aggregation ARS
    df_ret_centre_ars = df_ret.groupby(["code_region", "region", "code_departement", "id_centre", "nom_centre", "rang_vaccinal", "date_rdv","type_vaccin"], as_index=False).agg(
            nb=("nb", "sum"),
            nb_rdv_cnam=("nb_rdv_cnam", "sum"),
            nb_rdv_rappel=("nb_rdv_rappel", "sum")
            )
    # pre filtre sur date pour opendata
    if date.weekday() != 4 :
        print(" - - - Attention, les donnees de la semaine ne sont pas toutes remontees")
    df_ret = df_ret[
        (df_ret["date_rdv"] >= date_init - timedelta(days=date_init.weekday())) &
        (df_ret["date_rdv"] <= date - timedelta(days=date.weekday()) + timedelta(days=duree*7 -1)) ]
    df_ret["date_debut_semaine"] = df_ret["date_rdv"].apply(lambda x : (x - timedelta(days=x.weekday())).strftime("%Y-%m-%d"))
    df_ret = df_ret[df_ret["rang_vaccinal"] != "NR"]
    #agg par centre pour opendata et ARS
    df_ret_centre = df_ret.groupby(["code_region", "region", "code_departement", 
        "id_centre", "nom_centre", "rang_vaccinal", "date_debut_semaine"], as_index=False).agg(
            nb=("nb", "sum"),
            nb_rdv_cnam=("nb_rdv_cnam", "sum"),
            nb_rdv_rappel=("nb_rdv_rappel", "sum")
            )
    #agg par departement
    df_ret_dep = df_ret.groupby(["code_region", "region", "code_departement", "rang_vaccinal", "date_debut_semaine"], as_index=False)["nb"].sum()
    #agg par region
    df_ret_reg = df_ret.groupby(["code_region", "region", "rang_vaccinal", "date_debut_semaine"], as_index=False)["nb"].sum()
    # national
    df_ret_national = df_ret.groupby(["rang_vaccinal", "date_debut_semaine"], as_index=False)["nb"].sum()
    # remise en forme
    df_ret_centre_ars.rename(columns={"code_departement" : "departement"},inplace=True)
    df_ret_centre.rename(columns={"code_departement" : "departement"},inplace=True)
    df_ret_dep.rename(columns={"code_departement" : "departement"},inplace=True)
    df_ret_centre = df_ret_centre[["code_region", "region", "departement", "id_centre", "nom_centre", "rang_vaccinal", "date_debut_semaine", "nb", "nb_rdv_cnam", "nb_rdv_rappel"]]
    df_ret_centre_ars = df_ret_centre_ars[["code_region", "region", "departement", "id_centre", "nom_centre", "rang_vaccinal","type_vaccin","date_rdv", "nb", "nb_rdv_cnam", "nb_rdv_rappel"]]
    df_ret_dep = df_ret_dep[["code_region", "region", "departement", "rang_vaccinal", "date_debut_semaine", "nb"]]
    df_ret_reg = df_ret_reg[["code_region", "region", "rang_vaccinal", "date_debut_semaine", "nb"]]
    df_ret_national = df_ret_national[["rang_vaccinal", "date_debut_semaine", "nb"]]
    # tri
    df_ret_centre.sort_values(by=["region","nom_centre","departement","date_debut_semaine","rang_vaccinal"], inplace=True)
    df_ret_dep.sort_values(by=["departement","region","date_debut_semaine","rang_vaccinal"], inplace=True)
    df_ret_reg.sort_values(by=["region","date_debut_semaine","rang_vaccinal"], inplace=True)
    df_ret_national.sort_values(by=["date_debut_semaine", "rang_vaccinal"], inplace=True)
    #logs et affichage de controle
    logging.info("Aggregation des fichiers reussie.")
    print(" - - - Aggrégation des fichiers réussie. Résumé : ")
    print(df_ret_national)
    return df_ret_centre, df_ret_dep, df_ret_reg, df_ret_national, df_ret_centre_ars

#fonctions pour aggrégations adaptées pour la vm centos
def nb_rdv_cnam(row):
    if row["rdv_cnam"] == "true":
        return 1
    return 0

def nb_rdv_rappel(row):
    if row["rdv_rappel"] == "true":
        return 1
    return 0

#aggregations adaptées pour la vm centos
def aggregate_vm(df_in, date_init="", date=datetime.today().strftime("%Y-%m-%d"), duree = 4, verbose=True) :
    # param input
    duree = int(duree)
    if date_init == "" :
        date_init = datetime(2021, 1, 18)
    date = datetime.strptime(date,"%Y-%m-%d")
    logging.info("Date de filtrage pour l'opendata : " + str(date))
    logging.info("Duree de filtrage : " + str(duree))
    df_ret = df_in.copy()
    # filtrage rdv réel
    df_ret = df_ret[df_ret["honore"] != "false"]
    df_ret = df_ret[df_ret["annule"] != "true"]
    df_ret["nb"] = "1"
    # pre agrégation
    logging.info("Debut agregation")
    if verbose :
        print(" - - - Agrégation à la maille centre ...")
    df_ret["nb"] = df_ret["cp_centre"]
    df_ret["nb_rdv_cnam"] = df_ret.apply(lambda row: nb_rdv_cnam(row), axis=1, meta=int)
    df_ret["nb_rdv_rappel"] = df_ret.apply(lambda row: nb_rdv_rappel(row), axis=1, meta=int)
    df_ret = df_ret.groupby(["date_rdv", "id_centre","nom_centre","cp_centre",
        "code_departement", "code_region","region","rang_vaccinal", "operateur","type_vaccin"]).agg({
        "nb":"count",
        "nb_rdv_cnam":"sum",
        "nb_rdv_rappel":"sum"}).reset_index()
    if verbose :
        print(" - - - Agrégation à la maille centre terminee")
        print(" - - - Agrégation détaillées ...")
    # aggregation ARS
    df_ret_centre_ars = df_ret.groupby(["code_region", "region", "code_departement", "id_centre", "nom_centre", "rang_vaccinal", "date_rdv","type_vaccin"]).agg({
        "nb": "sum",
        "nb_rdv_cnam": "sum",
        "nb_rdv_rappel":"sum"}).reset_index()
    # pre filtre sur date pour opendata
    if date.weekday() != 4 :
        print(" - - - Attention, les donnees de la semaine ne sont pas toutes remontees")
    df_ret = df_ret[
        (df_ret["date_rdv"] >= date_init - timedelta(days=date_init.weekday())) &
        (df_ret["date_rdv"] <= date - timedelta(days=date.weekday()) + timedelta(days=duree*7 -1)) ]
    df_ret["date_debut_semaine"] = df_ret["date_rdv"].apply(lambda x : (x - timedelta(days=x.weekday())).strftime("%Y-%m-%d"))
    df_ret = df_ret[df_ret["rang_vaccinal"] != "NR"]
    #agg par centre pour opendata et ARS
    df_ret_centre = df_ret.groupby(["code_region", "region", "code_departement", 
        "id_centre", "nom_centre", "rang_vaccinal", "date_debut_semaine"]).agg({   
        "nb": "sum",
        "nb_rdv_cnam": "sum",
        "nb_rdv_rappel":"sum"}).reset_index()
    #agg par departement
    df_ret_dep = df_ret.groupby(["code_region", "region", "code_departement", "rang_vaccinal", "date_debut_semaine"])["nb"].sum().reset_index()
    #agg par region
    df_ret_reg = df_ret.groupby(["code_region", "region", "rang_vaccinal", "date_debut_semaine"])["nb"].sum().reset_index()
    # national
    df_ret_national = df_ret.groupby(["rang_vaccinal", "date_debut_semaine"])["nb"].sum().reset_index()
    # remise en forme
    df_ret_centre_ars = df_ret_centre_ars.rename(columns={"code_departement" : "departement"})
    df_ret_centre = df_ret_centre.rename(columns={"code_departement" : "departement"})
    df_ret_dep = df_ret_dep.rename(columns={"code_departement" : "departement"})
    df_ret_centre = df_ret_centre[["code_region", "region", "departement", "id_centre", "nom_centre", "rang_vaccinal", "date_debut_semaine", "nb", "nb_rdv_cnam", "nb_rdv_rappel"]]
    df_ret_centre_ars = df_ret_centre_ars[["code_region", "region", "departement", "id_centre", "nom_centre", "rang_vaccinal","type_vaccin","date_rdv", "nb", "nb_rdv_cnam", "nb_rdv_rappel"]]
    df_ret_dep = df_ret_dep[["code_region", "region", "departement", "rang_vaccinal", "date_debut_semaine", "nb"]]
    df_ret_reg = df_ret_reg[["code_region", "region", "rang_vaccinal", "date_debut_semaine", "nb"]]
    df_ret_national = df_ret_national[["rang_vaccinal", "date_debut_semaine", "nb"]]
    # tri
    df_ret_centre.sort_values(by=["region","nom_centre","departement","date_debut_semaine","rang_vaccinal"])
    df_ret_dep.sort_values(by=["departement","region","date_debut_semaine","rang_vaccinal"])
    df_ret_reg.sort_values(by=["region","date_debut_semaine","rang_vaccinal"])
    df_ret_national.sort_values(by=["date_debut_semaine", "rang_vaccinal"])
    #logs et affichage de controle
    logging.info("Aggregation des fichiers reussie.")
    print(" - - - Aggrégation des fichiers réussie. Résumé : ")
    print(df_ret_national)
    return df_ret_centre, df_ret_dep, df_ret_reg, df_ret_national, df_ret_centre_ars


#
# SAUVEGARDE
#

def save_agenda(df_in, folder = "data/agenda/", date=datetime.today().strftime("%Y-%m-%d"),  postfix="-raw", verbose=True) :
    path_out = folder + date + " - prise_rdv" + str(postfix) + ".csv"
    df_in.to_csv(path_out, 
    index=False, na_rep="",sep=",",encoding="utf-8")
    logging.info(path_out + " enregistre.")
    if verbose :
        print(" - - - Enregistrement de " + path_out + " termine.")
    return

def save_agenda_vm(df_in, folder = "data/agenda/", date=datetime.today().strftime("%Y-%m-%d"),  postfix="-raw", verbose=True) :
    path_out = folder + date + " - prise_rdv" + str(postfix) + ".csv"
    df_in.to_csv(path_out,
    index=False, na_rep="",sep=",",encoding="utf-8", mode="a")
    logging.info(path_out + " enregistre.")
    if verbose :
        print(" - - - Enregistrement de " + path_out + " termine.")
    return

def save_append_agenda(df_in, folder = "data/agenda/", date=datetime.today().strftime("%Y-%m-%d"),  postfix="-raw") :
    path_out = folder + date + " - prise_rdv" + str(postfix) + ".csv"
    if path.isfile(path_out):
        df_in.to_csv(path_out, 
            index=False, na_rep="",sep=",",encoding="utf-8",
            mode="a",header=False)
    else :
        df_in.to_csv(path_out, 
            index=False, na_rep="",sep=",",encoding="utf-8",
            mode="w",header=True)
    return

def save_append_agenda_gzip(df_in, folder = "data/agenda/", date=datetime.today().strftime("%Y-%m-%d"),  postfix="-raw") :
    path_out = folder + date + " - prise_rdv" + str(postfix) + ".csv.gz"
    if path.isfile(path_out):
        df_in.to_csv(path_out,
            index=False, na_rep="",sep=",",encoding="utf-8",
            mode="a",header=False, compression="gzip")
    else :
        df_in.to_csv(path_out,
            index=False, na_rep="",sep=",",encoding="utf-8",
            mode="w",header=True)
    return

def save_agenda_gzip(df_in, folder = "data/agenda/", date=datetime.today().strftime("%Y-%m-%d"),  postfix="-raw", verbose=True) :
    path_out = folder + date + " - prise_rdv" + str(postfix) + ".csv.gz"
    df_in.to_csv(path_out, 
    index=False, na_rep="",sep=",",encoding="utf-8", compression="gzip")
    logging.info(path_out + " enregistre.")
    if verbose :
        print(" - - - Enregistrement de " + path_out + " termine.")
    return

def save_agenda_gzip_vm(df_in, folder = "data/agenda/", date=datetime.today().strftime("%Y-%m-%d"),  postfix="-raw", verbose=True) :
    path_out = folder + date + " - prise_rdv" + str(postfix) + ".csv.gz"
    #df_in = df_in.compute()
    df_in.to_csv(path_out,
    index=False, na_rep="",sep=",",encoding="utf-8", compression="gzip", mode="a")
    logging.info(path_out + " enregistre.")
    if verbose :
        print(" - - - Enregistrement de " + path_out + " termine.")
    return

def save_agenda_xlsx(df_in, folder = "data/agenda/", date=datetime.today().strftime("%Y-%m-%d"),  postfix="-raw", verbose=True) :
    path_out = folder + date + " - prise_rdv" + str(postfix) + ".xlsx"
    df_in.to_excel(path_out, 
    index=False,sheet_name=(date + " - prise_rdv" + str(postfix)), na_rep="",encoding="utf-8")
    logging.info(path_out + " enregistre.")
    if verbose :
        print(" - - - Enregistrement de " + path_out + " termine.")
    return

def save_agenda_xlsx_vm(df_in, folder = "data/agenda/", date=datetime.today().strftime("%Y-%m-%d"),  postfix="-raw", verbose=True) :
    path_out = folder + date + " - prise_rdv" + str(postfix) + ".xlsx"
    df_in = df_in.compute()
    df_in.to_excel(path_out,
    index=False,sheet_name=(date + " - prise_rdv" + str(postfix)), na_rep="",encoding="utf-8")
    logging.info(path_out + " enregistre.")
    if verbose :
        print(" - - - Enregistrement de " + path_out + " termine.")
    return

# sauvegarde temporaire des bouts de chunk avant de les reconstituer

def save_append_chunk(df_in, folder = "data/agenda/", postfix="agenda_centre_temp") :
    path_out = folder + postfix+ ".csv"
    if path.isfile(path_out):
        df_in.to_csv(path_out, 
            index=False, na_rep="",sep=",",encoding="utf-8",
            mode="a",header=False)
    else :
        df_in.to_csv(path_out, 
            index=False, na_rep="",sep=",",encoding="utf-8",
            mode="w",header=True)
    return

def prepare_chunk_run(folder = "data/agenda/", date=datetime.today().strftime("%Y-%m-%d")) :
    if path.exists(folder + "agenda_centre_temp" + ".csv"):
        remove(folder + "agenda_centre_temp" + ".csv")
    if path.exists(folder + date + " - prise_rdv" + "-raw" + ".csv.gz"):
        remove(folder + date + " - prise_rdv" + "-raw" + ".csv.gz")
    print(" - - Nettoyage des fichiers temporaires pour les aggrégations terminé.")
    return