# Introduction

# Présentation

Ce dépôt git contient un utilitaure en ligne de commande permettant d'exécuter certains traitements par lot sur les données VACCSI.
Pour les données de rendez-vous :
- le module agenda permet d'importer les données des opérateurs, les reformater et les préparer pour le SI-Pilotage et l'opendata
- le module stock qui n'est pas utilisé a été retiré pour alléger et limiter la complexité de l'etl

# Dépendances

Python : 3.8.5 64-bit (conda)
Librairies : pysftp, pandas, datetime, json, re, zipfile, requests, logging, psycopg2, tqdm, re, os, argparse, openpyxl, icecream
Pour le module de gestion de stock : Postgresql 13

# Utilisation

## Mise à jour de la configuration

Mettre à jour les données relatives aux environnements (credentials SFTP/ data.gouv, id des ressources à upload sur data.gouv).
Créer un fichier config.json reprenant le template de config_demo.json.

Pour les champs de paramétrages :
- agenda :
    - "treshold_control" : pourcentage d'erreurs tolérés dans les fichiers opérateurs. Si un fichier dépasse ce seuil d'erreur, il n'est pas pris en compte pour la publication.
    - "log" : emplacement du fichier de log
    - "borne_publication_opendata" : permet d'indiquer le nombre de semaines dans le futur à inclure pour les publications opendata des rendez-vous.
- sftp :
    - ajouter les paramètres de connexion

Optionnel (utile uniquement pour le module de gestion de stock)
Lancer le script 'deploy.py' afin d'initialiser la BDD.

## Telechargement en local des rdv depuis un serveur SFTP

Utiliser l'utilitaire main.py. Le script détecte automatiquement les fichiers opérateurs les plus récents sur le SFTP dont la date de publication est antérieure à une date précisée en option.

```console
python main.py agenda import --config fichier_de_config.json --date AAAA-MM-JJ --verbose true
```

Options
- config : permet de choisir le fichier de config à utiliser
- date choisit la date maximale pour les imports réalisés

## Transformation des fichiers

Une fois téléchargés depuis les sftp, les fichiers doivent être traités.

### Rendez-vous

Utiliser l'utilitaire main.py. Le fichier brut digdash est généré dans data/agenda au format "AAAA-MM-JJ - prise_rdv-raw.csv".

```console
python main.py agenda process --config fichier_de_config.json --date AAAA-MM-JJ --verbose true
```
Puis pour générer les aggrégats pour l'OD et les synthèses :

```console
python main.py agenda process_OD --config fichier_de_config.json --date AAAA-MM-JJ --verbose true
```

** Alternatives : **

```console
python main.py agenda process_OD_VM --config fichier_de_config.json --date AAAA-MM-JJ --verbose true
```

Génération des aggrégats pour l'OD et les synthèses optimisée pour une VM de prod (utilisation de Dask).

```console
python main.py agenda process_OD_chunk --config fichier_de_config.json --date AAAA-MM-JJ --verbose true
```

Génération des aggrégats pour l'OD et les synthèses optimisée (sans Dask).

Les fichiers pour l'opendata sont générés dans data/agenda/opendata

Options
- config : permet de choisir le fichier de config à utiliser
- date : choisit la date maximale pour les fichiers en local utilisé. Le paramètre date est utilisé pour nommer les fichiers générés

Controles de format effectués :
- Booléen dans les champs annule/honore. Les lignes mal formattées ne sont pas prises en compte
- format des dates dans les champs date_rdv/date creation. Les lignes mal formattées ne sont pas prises en compte
- controle du format cp_centre. Les lignes mal formattées ne sont pas prises en compte
- detection d'absence d'annee de naissance (pas d'action). Les lignes mal formattées ne sont pas prises en compte
- detection d'absence d'id centre (pas d'action)

Les lignes en erreur sont ajoutées au fichier "log/agenda/err_agenda_log.log"

Pour controler les fichiers ainsi produits :

```console
python main.py agenda control --config fichier_de_config.json --date AAAA-MM-JJ --verbose true
```

Controles fonctionnels effectués :
- RG 1 : ALERTE si RDV annulés > RDV pris
- RG 2 : ALERTE si RDV non-honorés > RDV pris
Les résultats des contrôles sont affichés si des incohérences apparaissent et sont enregistrés dans le fichier de log générique.

NB : par RDV non-honorés on entend rdv pour lesquels le flag 'annule' est 'false' et le flag honore à 'false' également.



## Publication des fichiers

Utiliser l'utilitaire main.py. Les fichiers sont publiés soit sur data.gouv soit sur le sftp ARS.

Publication ARS :

```console
python main.py agenda publish_sftp_ars --config fichier_de_config.json --date AAAA-MM-JJ --verbose true
```
Nettoyage des fichiers en ligne sur le SFTP ARS :

```console
python main.py agenda clean_sftp --config fichier_de_config.json --date AAAA-MM-JJ --verbose true
```

Publication data.gouv

```console
python main.py agenda publish_opendata --datagouv "DATA PROD" --config fichier_de_config.json --date AAAA-MM-JJ --verbose true
```

Options
- config : permet de choisir le fichier de config à utiliser
- date choisit la date exacte des fichiers en local utilisé
- datagouv : indique sur quel environnement data.gouv le dataset est publié. Le nom de l'environnement peut être spécifié dans config.json

# Packaging du projet

Le projet si_vaccin_etl se décline autours de plusieurs répertoires
- un répertoire "modules" contenant les modules custom développés pour la collecte et la transformation de la donnée
- un répertoire "data" contenant les datasets utilisé
- un répertoire "config" permettant de changer la configuration de l'outil
- un répertoire "utils" contenant des fichiers tiers (liste des codes département/région par exemple)
- un répertoire "log" pour les logs

## Modules

### agenda

Fonctionnalités d'aggrégation et de transformation des rendez-vous.

### stocks

Fonctionnalités d'aggrégation et de transformation des stocks.
Seuls les stocks plateformes sont pris en charge dans cette version

### route_sftp

Fonctionnalités d'import/export automatique vers les différents serveurs SFTP.

### route_data_gouv

Fonctionnalités de publication sur les API Data.gouv

# Gestion des logs

Les logs de l'utilitaire sont retournés dans le fichier "log_debug.log"


# Gestion des logs

A l'exécution, des logs applicatifs et fonctionnels sont produits dans le dossier log/

Pour chaque domaine, un sous dossier dédié existe

## agenda

A l'exécution de la commande 'process', des logs d'exécution complémentaires concernant les erreurs dans les fichiers sources opérateurs sont produits.
- err_agenda_log.log : liste les lignes non valides
- rapport_err_agenda.csv : résumé macro des erreurs rencontrées