import pandas as pd
import json
import uuid
import os
import polars as pl
import duckdb
from pathlib import Path
import dill
from SmartPyDumper.object_inspect.type_helper import get_str_type

__SPLIT_TKN__='___'

def generate_filepath(folder_path, x):
    str_type=get_str_type(x)
    item_id=str(uuid.uuid4())
    return str(Path(folder_path)/f'{item_id}{__SPLIT_TKN__}{str_type}')        

def serialize_object(folder_path, x):
    file_path=generate_filepath(folder_path, x)
    str_type=get_str_type(x)
    if 'polars' in str_type:
        if 'lazyframe' in str_type:
            x.sink_parquet(file_path)
        elif 'dataframe' in str_type:
            x.write_parquet(file_path)
    elif 'duck' in str_type:
        raise Exception('Serialize function is not implemented for duckdb, please convert to polars first with .pl()')
        #x.pl().write_parquet(file_path)
    else:
        with open(file_path, 'wb') as file:
            dill.dump(x, file)
    return file_path


"""
limite :
> à éviter sur des big dict/list (tout sera exporté de manière décomposé)
si big data >>> préférer dataframe (polars, pandas, etc)
"""
def serialize(
    value, ## <--- value to serialize (ex. DataFrame, dict, list, etc.)
    folder_path
):
    # donnée complexe : pandas, polars
    ## TODO : modes
    # 'pessimistic' : part du principe que dans chaque dict/list se trouve au moins une donnée complexe
    # chaque élément est donc décomposé et exporté seul
    # > fonctionne tout le temps mais peut être pas très optimal si large dict/list
    # 'optimistic' : part du principe que dans les dict/list il n'y a aucune donnée complexe
    # chaque dict/list est donc exporté en un fichier (en principe x peut être exporté tel quel avec dill)
    # 'cautious' : prudent, vérifie le contenu entier de chaque dict/list pour déterminer si au moins une donnée complexe
    # si oui export décomposé
    # si non export en un bloc
    # > fonctionne tout le temps mais plus lent car vérif, pas toujours très rentable si seulement un élément complexe dans un grand dict
    # 'smart' : 

    ### convert dict/list[objects] into dict/list[paths]
    def handle_serialize_node(x):
        # if node is list : iterate over each element
        if isinstance(x, list):
            return [handle_serialize_node(v) for v in x]
        elif isinstance(x, tuple):
            return tuple([handle_serialize_node(v) for v in list(x)])
        # if node is dict : iterate over each element
        elif isinstance(x, dict):
            return {k: handle_serialize_node(v) for k, v in x.items()}
        # if node is an concrete object, evaluate it
        else:
            file_path=serialize_object(folder_path, x)
            return file_path
        
    res=handle_serialize_node(value)
    
    # if value was a list, res is a list containing hierarchy of path of each locally serialized items
    # if value was a dict, res is a dict containing hierarchy of path of each locally serialized items
    # else, value was a unique item, and res corresponds to path of this locally serialized item
    
    with open(str(Path(folder_path)/'main'), 'wb') as file:
        dill.dump(res, file)


def get_str_type_from_path(path):
    return path.split(__SPLIT_TKN__)[-1]

def deserialize_object(x):
    str_type=get_str_type_from_path(x)
    if 'polars' in str_type:
        if 'lazyframe' in str_type:
            data=pl.scan_parquet(x)
        elif 'dataframe' in str_type:
            data=pl.read_parquet(x)
    elif 'duck' in str_type:
        data=pl.read_parquet(x)
        data=duckdb.sql('select * from data')
    else:
        with open(x, 'rb') as file:
            data = dill.load(file)
    return data

def deserialize(folder_path):
    
    with open(str(Path(folder_path)/'main'), 'rb') as file:
        value = dill.load(file)
    
    ### convert dict/list[paths] into dict/list[objects]
    def handle_deserialize_node(x):
        # if node is list : iterate over each element
        if isinstance(x, list):
            return [handle_deserialize_node(v) for v in x]
        elif isinstance(x, tuple):
            return tuple([handle_deserialize_node(v) for v in list(x)])
        # if node is dict : iterate over each element
        elif isinstance(x, dict):
            return {k: handle_deserialize_node(v) for k, v in x.items()}
        # else, node is a path of a locally serialized object
        else:
            data=deserialize_object(x)
            return data
        
    
    # if root object corresponds to a list or dict, it is list/dict[paths]
    # else, root path corresponds to the path of a unique locally serialized object
    res=handle_deserialize_node(value)

    return res
