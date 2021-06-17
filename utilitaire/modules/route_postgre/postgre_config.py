import json
import pandas as pd
from datetime import datetime, timedelta
from datetime import date
from icecream import ic
import psycopg2
from psycopg2.extensions import AsIs
import pandas.io.sql as sqlio
import logging


def read_config_database(path_in, server="LOCAL SERVER") :
    with open(path_in) as f:
        dict_ret = json.load(f)
    L_ret = dict_ret["postgresql_db"]
    param_config = {}
    for param in L_ret :
        if param["server"] == server :
            param_config = param.copy()
    print("Lecture configuration serveur " + path_in + ".")
    return param_config

# Deploiement

def deploy_database(host="localhost", user="postgres", password="adminpostgre") :
    conn = psycopg2.connect(
        host=host,
        user=user,
        password=password)
    cursor = conn.cursor()
    conn.autocommit = True
    query_schema = "create database "+"datavac_etl"+";"
    cursor.execute(query_schema)
    conn.close()
    return

def init_empty_schema(host="localhost", user="postgres", password="adminpostgre", verbose = True) :
    print(" - - - Initialisation de la BDD ... ")
    conn = psycopg2.connect(
        host=host,
        user=user,
        database="datavac_etl",
        password=password)
    cursor = conn.cursor()
    conn.autocommit = True
    # CREATION schema
    query_schema = """CREATE SCHEMA IF NOT EXISTS %s AUTHORIZATION %s;"""
    param = (AsIs("datavac_stock"), AsIs("postgres"))
    cursor.execute(query_schema, param)
    # CREATION TABLE STOCK PLATEFORME
    query_schema_plateforme = """CREATE TABLE IF NOT EXISTS datavac_stock.stock_plateforme (
    id          serial primary key,
    nb_ucd      integer,
    nb_doses    integer,
    type_de_vaccin  varchar(40) NOT NULL,
    date        date
    );
    ALTER TABLE datavac_stock.stock_plateforme DROP CONSTRAINT IF EXISTS unique_stock_date;
    ALTER TABLE datavac_stock.stock_plateforme
    ADD CONSTRAINT unique_stock_date
    UNIQUE (type_de_vaccin, date) ;"""
    cursor.execute(query_schema_plateforme)
    query_schema_es = """CREATE TABLE IF NOT EXISTS datavac_stock.stock_es (
    id          serial primary key,
    nb_ucd      integer,
    nb_doses    integer,
    finess varchar(40) NOT NULL,
    type_de_vaccin  varchar(40) NOT NULL,
    date        date,
    date_maj    date
    );
    ALTER TABLE datavac_stock.stock_es DROP CONSTRAINT IF EXISTS unique_stock_date_finess;
    ALTER TABLE datavac_stock.stock_es
    ADD CONSTRAINT unique_stock_date_finess
    UNIQUE (finess, type_de_vaccin, date) ;"""
    cursor.execute(query_schema_es)
    conn.close()
    if verbose :
        print(" - - - Initialisation de la BDD terminée")
    return

# fonctions d insertion en BDD

def exec_insert_row_plateforme(row_in, cursor) :
    insert_sql = """
    INSERT INTO datavac_stock.stock_plateforme (nb_ucd, nb_doses, type_de_vaccin, date)
    VALUES (%s, %s, '%s', '%s')
    ON CONFLICT (type_de_vaccin, date) DO UPDATE SET
    (nb_ucd, nb_doses) = (EXCLUDED.nb_ucd, EXCLUDED.nb_doses);
    """
    param = (AsIs(row_in["nb_UCD"]), AsIs(row_in["nb_doses"]), AsIs(row_in["type_de_vaccin"]), AsIs(row_in["date"]),)
    cursor.execute(insert_sql, param)
    return

def insert_df_stock_plateforme(df_in, host="localhost", user="postgres", password="adminpostgre", verbose =True ) :
    if verbose :
        print(" - - - Insertion des stocks FLUIDE dans datavac_stock.stock_plateforme ... ")
    conn = psycopg2.connect(
        host=host,
        user=user,
        database="datavac_etl",
        password=password)
    conn.autocommit = True
    cursor = conn.cursor()
    df_in.apply(lambda x : exec_insert_row_plateforme(x, cursor),axis=1)
    conn.close()
    if verbose :
        print(" - - - Insertion réussie.")
    return

def exec_insert_row_es(row_in, cursor) :
    insert_sql = """
    INSERT INTO datavac_stock.stock_es (nb_ucd, nb_doses, finess, type_de_vaccin, date, date_maj)
    VALUES (%s, %s, '%s', '%s', '%s', '%s')
    ON CONFLICT (finess, type_de_vaccin, date) DO UPDATE SET
    (nb_ucd, nb_doses) = (EXCLUDED.nb_ucd, EXCLUDED.nb_doses);
    """
    param = (AsIs(row_in["nb_UCD"]), AsIs(row_in["nb_doses"]), AsIs(row_in["finess"]), AsIs(row_in["type_de_vaccin"]), AsIs(row_in["date"]), AsIs(row_in["date_maj"]))
    cursor.execute(insert_sql, param)
    return

def insert_df_stock_es(df_in, host="localhost", user="postgres", password="adminpostgre", verbose =True ) :
    if verbose :
        print(" - - - Insertion des stocks DISPOSTOCK dans datavac_stock.stock_es ... ")
    conn = psycopg2.connect(
        host=host,
        user=user,
        database="datavac_etl",
        password=password)
    conn.autocommit = True
    cursor = conn.cursor()
    df_in.apply(lambda x : exec_insert_row_es(x, cursor),axis=1)
    conn.close()
    if verbose :
        print(" - - - Insertion réussie.")
    return

# recuperation des donnees plateforme

def fetch_df_stock_plateforme(host="localhost", user="postgres", password="adminpostgre", verbose =True) :
    if verbose :
        print(" - - - Recuperation des stocks FLUIDE depuis datavac_stock.stock_plateforme ... ")
    conn = psycopg2.connect(
        host=host,
        user=user,
        database="datavac_etl",
        password=password)
    conn.autocommit = True
    sql = "select nb_ucd, nb_doses, type_de_vaccin, date from datavac_stock.stock_plateforme;"
    df_ret = sqlio.read_sql_query(sql, conn)
    conn.close()
    # Tri selon les dates
    df_ret.sort_values(by=["date", "type_de_vaccin"],inplace=True)
    if verbose :
        print(" - - - Stocks FLUIDE recuperes depuis datavac_stock.stock_plateforme.")
    return df_ret

def fetch_df_stock_es(host="localhost", user="postgres", password="adminpostgre", verbose =True) :
    if verbose :
        print(" - - - Recuperation des stocks DISPOSTOCK depuis datavac_stock.stock_es ... ")
    conn = psycopg2.connect(
        host=host,
        user=user,
        database="datavac_etl",
        password=password)
    conn.autocommit = True
    sql = "select nb_ucd, nb_doses, finess, type_de_vaccin, date, date_maj from datavac_stock.stock_es;"
    df_ret = sqlio.read_sql_query(sql, conn)
    conn.close()
    print(" - - - Stocks DISPOSTOCK recuperes depuis datavac_stock.stock_es.")
    return df_ret