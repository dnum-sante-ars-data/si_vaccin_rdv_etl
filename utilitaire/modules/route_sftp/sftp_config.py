import json
import zipfile
import pysftp
import re
from datetime import datetime
from datetime import date
import os.path
import logging

import subprocess
import wget

import os
from tqdm import tqdm
from glob import glob
import ftplib
from progressbar import AnimatedMarker, Bar, BouncingBar, Counter, ETA, AdaptiveETA, FileTransferSpeed, FormatLabel, Percentage, ProgressBar, ReverseBar, RotatingMarker, SimpleProgress, Timer, UnknownLength


#
# FONCTION GENERIQUE
#

# retourne la config d'un serveur au format dictionnaire
def read_config_sftp(path_in, server_name) :
    with open(path_in) as f:
        dict_ret = json.load(f)
    L_ret = dict_ret["sftp"]
    server_config = {}
    for server in L_ret :
        if server["server"] == server_name :
            server_config = server.copy()
    logging.info("Lecture config SFTP " + path_in + ".")
    return server_config

# serveur_config : dictionnaire de config du serveur
# path_sftp : emplacement depuis le root du fichier à copier
# path_out : emplacement local où copier
def get_file(sftp, path_sftp, path_out, verbose=True) :
    sftp.get(path_sftp, path_out)
    if verbose :
        print(" - - - Transfert de fichier réussi depuis sftp://" 
            + str(path_sftp)
            + " vers "
            + str(path_out))
    sftp.close()
    logging.info(" - - - Transfert de fichier réussi depuis sftp://" 
            + str(path_sftp)
            + " vers "
            + str(path_out))
    return

# supprime path_sftp et poste le fichier path_local en le renommant        
def post_file(sftp, path_local, path_sftp, verbose=True) :
    sftp.cwd("")
    if not os.path.isfile(path_local):
        print (" - - - Erreur :" + str(path_local), " non trouve")
        return
    print(" - - - Chargement " + path_local + " ...")
    if sftp.lexists(path_sftp) == True :
        if verbose :
            print(" - - - Suppression de " + path_sftp + " avant rechargement ...")
        sftp.remove(path_sftp)
        if verbose :
            print(" - - - " + path_sftp + " supprime.")
            logging.info(path_sftp + " supprime.")
    sftp.put(path_local, path_sftp)
    if verbose :
        print(" - - - Transfert de fichier réussi depuis " 
            + str(path_local)
            + " vers sftp://"
            + str(path_sftp))
    logging.info("Transfert de fichier réussi depuis " 
            + str(path_local)
            + " vers sftp://"
            + str(path_sftp))
    return

def delete_file(sftp, path_sftp, verbose=True) :
    sftp.cwd("")
    if sftp.lexists(path_sftp) == True :
        if verbose :
            print(" - - - Suppression de " + path_sftp)
        sftp.remove(path_sftp)
        if verbose :
            print(" - - - " + path_sftp + " supprime.")
            logging.info(path_sftp + " supprime.")
    return

# utils connexe

def dezip_op(folder_path_in, file_in, verbose=True) :
    with zipfile.ZipFile(folder_path_in + "/" + file_in, 'r') as zip_ref:
        zip_ref.extractall(folder_path_in)
        print("- - - " + folder_path_in + "/" + file_in + " extrait.")
        logging.info("dezip_op : " + folder_path_in + "/" + file_in + " extrait.")
    return

# 
# SPE 
#

# AGENDA

# parsing des noms de fichier sur le serveur sftp selon operateur
# le nom de fichier le plus recent est retourne
def get_agenda_op_sftp(sftp, operateur, date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    # controle operateur existe
    if operateur not in ["maiia", "doctolib", "keldoc"] :
        print("Type operateur agenda inconnu : " + str(operateur))
        raise ValueError
    # arborescence du sftp
    if operateur == "maiia" :
        sftp.cwd("maiia")
        pattern_f = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-"+ str(operateur) + ".zip$"
    elif operateur == "keldoc" :
        sftp.cwd("nehs")
        pattern_f = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-"+ str(operateur) + "-rdv.csv$"
    elif operateur == "doctolib" :
        sftp.cwd("doctolib")
        pattern_f = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-"+ str(operateur) + "-rdv.csv$"
    directory_structure = sftp.listdir_attr()
    filenames = list(map(lambda x : x.filename, directory_structure))
    # verification que le pattern correspond à celui du fichier
    if filenames :
        filenames = [x for x in filenames if re.match(pattern_f, x)]
        filenames = [x for x in filenames if datetime.strptime(x[0:10],"%Y-%m-%d") <= datetime.strptime(date,"%Y-%m-%d")]
    else :
        print(" - - Erreur : aucun fichier operateur en local pour le " + date)
        raise FileNotFoundError
    if filenames :
        dates = list(map(lambda x : datetime.strptime(x[0:10],"%Y-%m-%d"),filenames))
    else :
        print(" - - Erreur : aucun fichier operateur en local pour le " + date)
        raise FileNotFoundError
    index_max = dates.index(max(dates))
    file_name_ret = filenames[index_max]
    # retour dans le dossier parent
    sftp.cwd("../")
    if verbose :
        print(" - - - Fichier agenda le plus recent sur le serveur : "
            + str(file_name_ret))
    logging.info(operateur + " : Fichier agenda le plus recent sur le serveur : " + str(file_name_ret) + ".")
    return file_name_ret

# telechargement complet

def save_agenda_op_sftp(operateur, server_in_config, date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    if operateur not in ["maiia", "doctolib", "keldoc"] :
        print(" - - - Erreur : Type operateur agenda inconnu : " + str(operateur))
        raise ValueError
    host = server_in_config["host"]
    username = server_in_config["username"]
    password = server_in_config["password"]

    # config pour ne pas checker de clé existante
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None 
    with pysftp.Connection(host=host, username=username, password=password, port =2222, cnopts=cnopts) as sftp:
        # localisation du fichier a recuperer sur le serveur sftp
        file_name_sftp = get_agenda_op_sftp(sftp, operateur, date=date, verbose=verbose)
        if operateur == "maiia" :
            path_sftp = "maiia/" + file_name_sftp
            path_local = "data/agenda/"+str(operateur) + "/" + file_name_sftp
            get_file(sftp, path_sftp, path_local, verbose)
            # dezippage des fichier maiia
            dezip_op("data/agenda/"+str(operateur), file_name_sftp, verbose)
        elif operateur == "keldoc" :
            path_sftp = "nehs/" + file_name_sftp
            path_local = "data/agenda/"+str(operateur) + "/" + file_name_sftp
            get_file(sftp, path_sftp, path_local, verbose)
            sftp.close()
        if operateur == "doctolib" :
            path_sftp = "doctolib/" + file_name_sftp
            path_local = "data/agenda/"+str(operateur) + "/" + file_name_sftp
            get_file(sftp, path_sftp, path_local, verbose)
    return

# telechargement complet via wget

def save_wget_agenda_op_sftp(operateur, server_in_config, date=datetime.today().strftime("%Y-%m-%d"), verbose=True):
    print('--- Lancement de la commande wget')
    if operateur not in ["maiia", "doctolib", "keldoc"] :
        print(" - - - Erreur : Type operateur agenda inconnu : " + str(operateur))
        raise ValueError
    #sftp_host = sftp_host
    host = server_in_config["host"]
    username = server_in_config["username"]
    password = server_in_config["password"]
    # config pour ne pas checker de clé existante
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None 
# localisation du fichier a recuperer sur le serveur sftp
    with pysftp.Connection(host=host, username=username, password=password, port =2222, cnopts=cnopts) as sftp:
        file_name_sftp = get_agenda_op_sftp(sftp, operateur, date=date, verbose=verbose)
        if operateur == "maiia" :
            dst = "data/agenda/"+str(operateur) + "/"
            path_sftp = "maiia/" + file_name_sftp
            cmd = 'wget --directory-prefix='+dst+' --user="'+username+'" --password="'+password+'"  ftp://'+host+'/'+path_sftp+' --progress=bar'
            subprocess.run(cmd, shell=True)
            print(' - Commande "'+cmd+'" exécutée')
            # dezippage des fichier maiia
            dezip_op("data/agenda/"+str(operateur), file_name_sftp, verbose)
        elif operateur == "keldoc" :
            dst = "data/agenda/"+str(operateur) + "/"
            path_sftp = "nehs/" + file_name_sftp
            cmd = 'wget --directory-prefix='+dst+' --user="'+username+'" --password="'+password+'"  ftp://'+host+'/'+path_sftp+' --progress=bar'
            subprocess.run(cmd, shell=True)
            print(' - Commande "'+cmd+'" exécutée')
        if operateur == "doctolib" :
            dst = "data/agenda/"+str(operateur) + "/"
            path_sftp = "doctolib/" + file_name_sftp
            cmd = 'wget --directory-prefix='+dst+' --user="'+username+'" --password="'+password+'"  ftp://'+host+'/'+path_sftp+' --progress=bar'
            subprocess.run(cmd, shell=True)
            print(' - Commande "'+cmd+'" exécutée')
    return

# publication sur le serveur sftp ARS

def publish_agenda_sftp_ars(server_out_config, date=datetime.today().strftime("%Y-%m-%d"), publish_region=True, verbose=True) :
    host = server_out_config["host"]
    username = server_out_config["username"]
    password = server_out_config["password"]

    # config pour ne pas checker de clé existante
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None 
    with pysftp.Connection(host=host, username=username, password=password, port =2222, cnopts=cnopts) as sftp:
        # publication du fichier brut
        path_local = "data/agenda/" + date + " - prise_rdv-raw.csv.gz"
        path_sftp = date + " - prise_rdv-raw.csv.gz"
        post_file(sftp, path_local,path_sftp, verbose=verbose)
        path_local = "data/agenda/" + date + " - prise_rdv-raw.csv.gz"
        # doublon publié sans la date dans le prefixe
        path_sftp = "prise_rdv-raw.csv.gz"
        post_file(sftp, path_local,path_sftp, verbose=verbose)
        path_local = "data/agenda/" + date + " - prise_rdv-raw.csv"
        path_sftp = date + " - prise_rdv-raw.csv"
        post_file(sftp, path_local,path_sftp, verbose=verbose)
        # publication du fichier ARS
        path_local = "data/agenda/" + date + " - prise_rdv.csv"
        path_sftp = date + " - prise_rdv.csv"
        post_file(sftp, path_local,path_sftp, verbose=verbose)
        path_local = "data/agenda/" + date + " - prise_rdv.xlsx"
        path_sftp = date + " - prise_rdv.xlsx"
        post_file(sftp, path_local,path_sftp, verbose=verbose)
        if publish_region :
            # publication des fichiers ARS si le parametre publish_region est affecte a True
            L_region = ['HDF', 'ARA', 'IDF', 'GUY', 'OCC', 'NAQ', 'GES', 'PAC', 'COR', 
                'BRE', 'GDP', 'BFC', 'MAR', 'REU', 'NOR', 'CVL', 'PDL']
            for region in L_region :
                path_local = "data/agenda/ars/" + date + " - prise_rdv_" + region + ".csv"
                path_sftp = "prise_rdv_" + region + ".csv"
                post_file(sftp, path_local,path_sftp, verbose=verbose)
    return

def publish_agenda_sftp(server_out_config, *l_publication, date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    host = server_out_config["host"]
    username = server_out_config["username"]
    password = server_out_config["password"]

    # config pour ne pas checker de clé existante
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None 
    with pysftp.Connection(host=host, username=username, password=password, port =2222, cnopts=cnopts) as sftp:
        for publi in list(l_publication) :
            # publication du fichier brut
            path_local = publi["path_local"]
            path_sftp = publi["path_sftp"]
            post_file(sftp, path_local,path_sftp, verbose=verbose)
    return

global sftp

def publish_agenda_ftplib_sftp(server_out_config, *l_publication, date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    print('--- Lancement de la publication via ftplib')
    host = server_out_config["host"]
    username = server_out_config["username"]
    password = server_out_config["password"]
    sftp = ftplib.FTP(host, username, password)
    for publi in list(l_publication) :
        # publication du fichier brut
        path_local = publi["path_local"]
        size_path_local = os.path.getsize(path_local)
        path_sftp = publi["path_sftp"]
        file_to_transfer = open(path_local, 'rb')
        with tqdm(unit = 'blocks', unit_scale = True, leave = True, miniters = 1, desc = 'Uploading......', total = size_path_local) as tqdm_instance:
            sftp.storbinary('STOR ' + path_sftp, file_to_transfer, 2048, callback = lambda sent: tqdm_instance.update(len(sent)))
            file_to_transfer.close()
        sftp.quit()
        sftp = None
        print(' - Publication exécutée')
        return

def clean_agenda_sftp(server_out_config,*l_path_sftp, verbose=True) :
    host = server_out_config["host"]
    username = server_out_config["username"]
    password = server_out_config["password"]

    # config pour ne pas checker de clé existante
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(host=host, username=username, password=password, port =2222, cnopts=cnopts) as sftp:
        for path_sftp in list(l_path_sftp) :
            delete_file(sftp, path_sftp, verbose)
    return


# CRENEAUX

# parsing des noms de fichier sur le serveur sftp selon operateur
# le nom de fichier le plus recent est retourne
def get_creneaux_op_sftp(sftp, operateur, date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    # controle operateur existe
    if operateur not in ["maiia", "doctolib", "keldoc"] :
        print("Type operateur agenda inconnu : " + str(operateur))
        raise ValueError
    # arborescence du sftp
    if operateur == "maiia" :
        sftp.cwd("maiia")
        pattern_f = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-"+ str(operateur) + ".zip$"
    elif operateur == "keldoc" :
        sftp.cwd("nehs")
        pattern_f = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-"+ str(operateur) + "-plages-horaires.csv$"
    elif operateur == "doctolib" :
        sftp.cwd("doctolib")
        pattern_f = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-"+ str(operateur) + "-plages-horaires.csv$"
    directory_structure = sftp.listdir_attr()
    filenames = list(map(lambda x : x.filename, directory_structure))
    # verification que le pattern correspond à celui du fichier
    if filenames :
        filenames = [x for x in filenames if re.match(pattern_f, x)]
        filenames = [x for x in filenames if datetime.strptime(x[0:10],"%Y-%m-%d") <= datetime.strptime(date,"%Y-%m-%d")]
    else :
        print(" - - Erreur : aucun fichier operateur en local pour le " + date)
        raise FileNotFoundError
    if filenames :
        dates = list(map(lambda x : datetime.strptime(x[0:10],"%Y-%m-%d"),filenames))
    else :
        print(" - - Erreur : aucun fichier operateur en local pour le " + date)
        raise FileNotFoundError
    index_max = dates.index(max(dates))
    file_name_ret = filenames[index_max]
    # retour dans le dossier parent
    sftp.cwd("../")
    if verbose :
        print(" - - - Fichier agenda le plus recent sur le serveur : "
            + str(file_name_ret))
    logging.info(operateur + " : Fichier agenda le plus recent sur le serveur : " + str(file_name_ret) + ".")
    return file_name_ret

def save_creneaux_op_sftp(operateur, server_in_config, date=datetime.today().strftime("%Y-%m-%d"), verbose=True) :
    if operateur not in ["maiia", "doctolib", "keldoc"] :
        print(" - - - Erreur : Type operateur agenda inconnu : " + str(operateur))
        raise ValueError
    host = server_in_config["host"]
    username = server_in_config["username"]
    password = server_in_config["password"]

    # config pour ne pas checker de clé existante
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None 
    with pysftp.Connection(host=host, username=username, password=password, port =2222, cnopts=cnopts) as sftp:
        # localisation du fichier a recuperer sur le serveur sftp
        file_name_sftp = get_creneaux_op_sftp(sftp, operateur, date=date, verbose=verbose)
        if operateur == "maiia" :
            path_sftp = "maiia/" + file_name_sftp
            path_local = "data/creneaux/"+str(operateur) + "/" + file_name_sftp
            get_file(sftp, path_sftp, path_local, verbose)
            # dezippage des fichier maiia
            dezip_op("data/creneaux/"+str(operateur), file_name_sftp, verbose)
        elif operateur == "keldoc" :
            path_sftp = "nehs/" + file_name_sftp
            path_local = "data/creneaux/"+str(operateur) + "/" + file_name_sftp
            get_file(sftp, path_sftp, path_local, verbose)
            sftp.close()
        if operateur == "doctolib" :
            path_sftp = "doctolib/" + file_name_sftp
            path_local = "data/creneaux/"+str(operateur) + "/" + file_name_sftp
            get_file(sftp, path_sftp, path_local, verbose)
    return
