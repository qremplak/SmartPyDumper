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
EXTENSION_UNIT_PYDUMP='.upydump' 
EXTENSION_VAR_PYDUMP='.vpydump' 

def is_complex(str_type):
    if (
        'polars' in str_type 
        or 'duck' in str_type
        ):
        return True
    else:
        return False

def generate_filepath(folder_path, x):
    str_type=get_str_type(x)
    item_id=str(uuid.uuid4())
    return str(Path(folder_path)/f'{item_id}{__SPLIT_TKN__}{str_type}') + EXTENSION_UNIT_PYDUMP   

def serialize_object(folder_path, x):
    file_path=generate_filepath(folder_path, x)
    str_type=get_str_type(x)

    ## if complex object : specific export routine, storing file_path
    if is_complex(str_type):
        if 'polars' in str_type:
            if 'lazyframe' in str_type:
                x.sink_parquet(file_path)
            elif 'dataframe' in str_type:
                x.write_parquet(file_path)
            return file_path
        elif 'duck' in str_type:
            raise Exception('Serialize function is not implemented for duckdb, please convert to polars first with .pl()')
            #x.pl().write_parquet(file_path)
        
    ## if not complex object : can be exported by standard routine, storing original object
    else:
        return x


def serialize(
    value, ## <--- value to serialize (ex. DataFrame, dict, list, etc.)
    folder_path
):
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
            file_path_or_object=serialize_object(folder_path, x)
            return file_path_or_object
        
    res=handle_serialize_node(value)
    
    # if value was a list/dict, res is a list/dict containing hierarchy of objects 
    # >> if complex object : path of locally serialized object
    # >> if not complex object : object itself
    # else, value was a unique item, and res corresponds to path of this locally serialized item
    
    with open(str(Path(folder_path)/EXTENSION_VAR_PYDUMP), 'wb') as file:
        dill.dump(res, file)

def is_pydump_unit_path(x):
    return type(x) is str and EXTENSION_UNIT_PYDUMP in x

def get_str_type_from_unit(x):
    if is_pydump_unit_path(x):
        return x.split(__SPLIT_TKN__)[-1].replace(EXTENSION_UNIT_PYDUMP, '')
    else:
        return get_str_type(x)

def deserialize_object(x):
    # if object is a path of an exported pydump unit, it corresponds to an exported complex object
    # > read and return the object
    str_type=get_str_type_from_unit(x)
    if is_complex(str_type):
        if 'polars' in str_type:
            if 'lazyframe' in str_type:
                data=pl.scan_parquet(x)
            elif 'dataframe' in str_type:
                data=pl.read_parquet(x)
        elif 'duck' in str_type:
            data=pl.read_parquet(x)
            data=duckdb.sql('select * from data')
        return data
    # else, it's a not-complex object to return itself
    else:
        return x
    #else:
    #    with open(x, 'rb') as file:
    #        data = dill.load(file)
    

def deserialize(folder_path):
    
    with open(str(Path(folder_path)/EXTENSION_VAR_PYDUMP), 'rb') as file:
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
