import json
import pandas as pd
from datetime import datetime, timedelta
from datetime import date
from os import walk
from icecream import ic
import re
import logging


#
# LECTURE CONFIG
#

def read_config_stock(path_in) :
    with open(path_in) as f:
        dict_ret = json.load(f)
    L_ret = dict_ret["parametrage"]
    param_config = {}
    for param in L_ret :
        if param["domaine"] == "stock" :
            param_config = param.copy()
    logging.info("Lecture configuration stock " + path_in + ".")
    return param_config



#
# CHARGEMENT
#

# retrouve dans le répertoire local le dernier fichier des stocks FLUIDE
# par defaut la date du jour d execution est selectionnee
def get_stock_fluide_loc(date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    file_name_ret = ""
    _, _, filenames = next(walk("data/stock/fluide/raw/"))
    pattern_f = "SPF-SPF_002-INVRPT-" + "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]" + ".txt$"
    if filenames :
        # filtrage des fichiers interessants
        filenames = [x for x in filenames if re.match(pattern_f, x)]
        filenames = [x for x in filenames if datetime.strptime(x[19:27],"%Y%m%d") <= datetime.strptime(date,"%Y-%m-%d")]
    else :
        print(" - - - Erreur : aucun fichier FLUIDE en local")
        logging.info("Aucun fichier operateur en local.")
        raise FileNotFoundError
    if filenames :
        dates = list(map(lambda x : datetime.strptime(x[19:27],"%Y%m%d"),filenames))
        index_max = dates.index(max(dates))
        file_name_ret = filenames[index_max]
    else :
        print(" - - - Erreur : aucun fichier FLUIDE en local mis a jour avant le " + 
                        date + ".")
        logging.info("Aucun fichier FLUIDE en local mis a jour avant le " + 
                        date + ".")
        raise FileNotFoundError
    return("data/stock/fluide/raw/" + file_name_ret)

#
# PARSING des fichiers fluide
#

def parse_line_fluide(line_in) :
    dict_ret = {}
    # split avec ; sans prendre le dernier element apres le dernier ;
    L_input = line_in[:-1].split(';')
    # type : DET, LIN, ENT
    type_line = L_input[0]
    if type_line not in ["DET","LIN","ENT"] :
        print("- - - Lignes en erreur : " + str(line_in))
        return {}
    # traitement des types de ligne
    elif type_line == "ENT" :
        if len(L_input) == 5 :
            dict_ret = {
                        "type_line" : "ENT",
                        "id_centre" : L_input[1],
                        "raison_sociale_centre" : L_input[2],
                        "date_stock" : L_input[3],
                        "nb_prevision" : L_input[4]
                        }
        else :
            print("- - - Lignes en erreur : format ENT incorrect " + str(line_in))
            return {}
    elif type_line == "LIN" :
        if len(L_input) == 34 :
            dict_ret = {
                "type_line" : "LIN",
                "id_fournisseur" : L_input[1],
                "raison_sociale_fournisseur" : L_input[2],
                "code_produit_entrepot" : L_input[3],
                "produit_VL" : L_input[4],
                "code_produit_EAN" : L_input[5],
                "designation_produit" : L_input[6],
                "PCB_produit" : L_input[7],
                "qte_stock_UVC" : L_input[8],
                "qte_stock_colis" : L_input[9],
                "qte_stock_palette" : L_input[10],
                "qte_immobilisee_UVC" : L_input[11],
                "qte_immobilisee_colis" : L_input[12],
                "qte_immobilisee_palette" : L_input[13],
                "qte_prepa_UVC" : L_input[14],
                "qte_prepa_colis" : L_input[15],
                "qte_prepa_palette" : L_input[16],
                "qte_encours_stock_UVC" : L_input[17],
                "qte_encours_stock_colis" : L_input[18],
                "qte_encours_stock_palette" : L_input[19],
                "date_reception" : L_input[20],
                "prevision_UVC" : L_input[21],
                "prevision_colis" : L_input[22],
                "prevision_palette" : L_input[23],
                "moyenne_UVC" : L_input[24],
                "moyenne_colis" : L_input[25],
                "moyenne_palette" :L_input[26],
                "couverture_jours" : L_input[27],
                "besoin_UVC" : L_input[28],
                "besoin_colis" : L_input[29],
                "besoin_palette" : L_input[30],
                "date_livraison" : L_input[31],
                "code_produit_fourn" : L_input[32],
                "designation_produit_fourn" : L_input[33]
            }
        else :
            print("- - - Lignes en erreur : format LIN incorrect " + str(line_in))
            return {}
    elif type_line == "DET" :
        if len(L_input) == 13 :
            dict_ret = {
                "type_line" : "DET",
                "code_palette" : L_input[1],
                "qte_UVC" : L_input[2],
                "qte_colis" : L_input[3],
                "qte_palette" : L_input[4],
                "UVC_prep" : L_input[5],
                "motif_immobilisation" : L_input[6],
                "code_lot" : L_input[7],
                "date_peremption" : L_input[8],
                "date_reception" : L_input[9],
                "date_fabrication" : L_input[10],
                "poids_palette" : L_input[11]
            }
        else :
            print("- - - Lignes en erreur : format DET incorrect " + str(line_in))
            logging.info("Lignes en erreur : format DET incorrect " + str(line_in))
            return {}
    return dict_ret


def append_line(dic_line_in, df_in, dic_ENT_in, dic_LIN_in) :
    # dic_ENT_in : ligne de type ENT actuel
    # dic_LIN_in : ligne de type LIN actuel
    df_ret = df_in.copy()
    dic_ENT_ret = dic_ENT_in
    dic_LIN_ret = dic_LIN_in
    if "type_line" not in dic_line_in :
         return df_ret, dic_ENT_ret, dic_LIN_ret
    if dic_line_in["type_line"] == "ENT" :
        dic_ENT_ret = dic_line_in
    elif dic_line_in["type_line"] == "LIN" :
        dic_LIN_ret = dic_line_in
    elif dic_line_in["type_line"] == "DET" :
        row_append = {
            "id_centre" : dic_ENT_in["id_centre"],
            "date_stock_ENT" : dic_ENT_in["date_stock"],
            "raison_sociale" : dic_LIN_in["raison_sociale_fournisseur"],
            "id_fournisseur" : dic_LIN_in["id_fournisseur"],
            "code_produit_entrepot" : dic_LIN_in["code_produit_entrepot"],
            "code_produit_EAN" : dic_LIN_in["code_produit_EAN"],
            "code_produit_fourn" : dic_LIN_in["code_produit_fourn"],
            "designation_produit" : dic_LIN_in["designation_produit"],
            "qte_stock_UVC" : dic_LIN_in["qte_stock_UVC"],
            "qte_immobilisee_UVC" : dic_LIN_in["qte_immobilisee_UVC"],
            "date_livraison" : dic_LIN_in["date_livraison"],
            "qte_UVC_detail" : dic_line_in["qte_UVC"],
            "code_lot_detail" : dic_line_in["code_lot"],
            "date_peremption" : dic_line_in["date_peremption"],
            "date_reception" : dic_line_in["date_reception"],
            "date_fabrication" : dic_line_in["date_fabrication"]
        }
        df_ret = df_ret.append(row_append, ignore_index=True)
    return df_ret, dic_ENT_ret, dic_LIN_ret

def load_fluide(date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    path_fluide = get_stock_fluide_loc(date=date, verbose=verbose)
    if verbose :
        print("- - - Chargement de " + path_fluide + " ...")
    dic_ENT_curr = {}
    dic_LIN_curr = {}
    line_count = 0
    df_ret = pd.DataFrame.from_dict({
            "id_centre" : [],
            "raison_sociale" : [],
            "date_stock_ENT" : [],
            "id_fournisseur" : [],
            "code_produit_entrepot" : [],
            "code_produit_EAN" : [],
            "code_produit_fourn" : [],
            "designation_produit" : [],
            "qte_stock_UVC" : [],
            "qte_immobilisee_UVC" : [],
            "date_livraison" : [],
            "qte_UVC_detail" : [],
            "code_lot_detail" : [],
            "date_peremption" : [],
            "date_reception" : [],
            "date_fabrication" : []
        })
    file_reader =  open(path_fluide, 'r')
    lines_fluide = file_reader.readlines()
    for curr_line in lines_fluide :
        line_count = line_count + 1
        curr_line = curr_line.strip()
        dic_line = parse_line_fluide(curr_line)
        # iteration
        df_ret, dic_ENT_ret, dic_LIN_ret = append_line(dic_line, df_ret, dic_ENT_curr, dic_LIN_curr)
        dic_ENT_curr = dic_ENT_ret
        dic_LIN_curr = dic_LIN_ret
    file_reader.close()
    if verbose :
        print("- - - Chargement de " + path_fluide + " termine : " + str(line_count) + " chargees.")
    logging.info("Chargement de " + path_fluide + " termine : " + str(line_count) + " chargees.")
    return df_ret

# transformation au format pour l'opendata

def process_fluide(df_in,dic_ratio_ucd, verbose=True) :
    logging.info("Processing des fichiers aggreges FLUIDE ...")
    # dic_ratio_ucd : dictionaire de transcodage pour les doses UCD/VACCIN, a parametrer
    df_ret = df_in.copy()
    # mise en forme des dates
    df_ret.loc[df_ret["date_stock_ENT"].notnull(),"date_stock_ENT"] = pd.to_datetime(df_ret.loc[df_ret["date_stock_ENT"].notnull(),"date_stock_ENT"], format='%Y%m%d%H%M', errors='coerce').dt.date
    df_ret.loc[df_ret["date_peremption"].notnull(),"date_peremption"] = pd.to_datetime(df_ret.loc[df_ret["date_peremption"].notnull(),"date_peremption"], format='%Y%m%d', errors='coerce').dt.date
    df_ret.loc[df_ret["date_reception"].notnull(),"date_reception"] = pd.to_datetime(df_ret.loc[df_ret["date_reception"].notnull(),"date_reception"], format='%Y%m%d', errors='coerce').dt.date
    df_ret.loc[df_ret["date_fabrication"].notnull(),"date_fabrication"] = pd.to_datetime(df_ret.loc[df_ret["date_fabrication"].notnull(),"date_fabrication"], format='%Y%m%d', errors='coerce').dt.date
    # on ne garde que les produits vaccin
    filter_vaccin = df_ret["code_produit_fourn"].isin([
        "Vaccins PFIZER",
        "Vaccins ASTRAZENECA",
        "Vaccins MODERNA"
    ])
    df_ret = df_ret[filter_vaccin]
    df_ret.loc[df_ret["qte_UVC_detail"].notnull(),"qte_UVC_detail"] = df_ret.loc[df_ret["qte_UVC_detail"].notnull(),"qte_UVC_detail"].apply(int)
    df_ret["qte_UVC_detail"] = df_ret["qte_UVC_detail"].fillna(0)
    # regroupement par plateforme
    df_ret_plateforme = df_ret.groupby(["code_produit_fourn", "date_stock_ENT"], as_index=False)["qte_UVC_detail"].sum()
    # renommage des colonnes
    df_ret_plateforme.rename({"code_produit_fourn": "type_de_vaccin",
             "date_stock_ENT": "date",
             "qte_UVC_detail": "nb_UCD"}, axis='columns',inplace=True)
    df_ret_plateforme.replace({"type_de_vaccin": {
        "Vaccins PFIZER": "Pfizer",
        "Vaccins ASTRAZENECA": "AstraZeneca",
        "Vaccins MODERNA": "Moderna"}}, inplace=True)
    df_ret_plateforme["nb_doses"] = 0
    df_ret_plateforme.loc[df_ret_plateforme["type_de_vaccin"] == "Pfizer", "nb_doses"] = df_ret_plateforme.loc[df_ret_plateforme["type_de_vaccin"] == "Pfizer", "nb_UCD"] * int(dic_ratio_ucd["Pfizer"])
    df_ret_plateforme.loc[df_ret_plateforme["type_de_vaccin"] == "Moderna", "nb_doses"] = df_ret_plateforme.loc[df_ret_plateforme["type_de_vaccin"] == "Moderna", "nb_UCD"] * int(dic_ratio_ucd["Moderna"])
    df_ret_plateforme.loc[df_ret_plateforme["type_de_vaccin"] == "AstraZeneca", "nb_doses"] = df_ret_plateforme.loc[df_ret_plateforme["type_de_vaccin"] == "AstraZeneca", "nb_UCD"] * int(dic_ratio_ucd["AstraZeneca"])
    logging.info("Processing des fichiers aggreges FLUIDE realise avec succes.")
    return df_ret_plateforme

#
# SAUVEGARDE
#

def save_fluide(df_in, folder = "data/stock/fluide/opendata/", date=datetime.today().strftime("%Y-%m-%d"), postfix="", verbose=True) :
    path_out = folder + date + " - stocks-plateformes" + postfix + ".csv"
    df_in.to_csv(path_out, 
    index=False, na_rep="",sep=";",encoding="utf-8")
    logging.info(path_out + " enregistre.")
    if verbose :
        print(" - - - Enregistrement de " + path_out + " termine.")
    return