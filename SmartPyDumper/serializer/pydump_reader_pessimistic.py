import pandas as pd
import json
import uuid
import os
import polars as pl
import duckdb
from pathlib import Path
import dill
import SmartPyDumper.object_inspect.type_helper as th
from SmartPyDumper.serializer.smart_serializer import *
from SmartPyDumper.object_inspect.type_helper import VGROUP

RENDER_SAMPLE_SIZE=100

class PydumpUnitReader:

    def __init__(self, path):
        self.path=path
        self.type_data=get_str_type_from_path(path)
        self.data=deserialize_object(path)
        self.group_data=th.detect_group(self.data)
        self.var_name=Path(path).stem
    
    def to_dict(self):
        return {
            'path':self.path,
            'type_data':self.type_data,
            'group_data':self.group_data,
            'data':self.data,
        }

    def render_txt(self):
        data, _ = th.cast_to_group(
            self.data,
            sample=RENDER_SAMPLE_SIZE
        )
        
        return str(data)
       
class PydumpVarReader:
    # folder name should be variable name
    def __init__(self, dumper_path):
        with open(str(Path(dumper_path)/EXTENSION_VAR_PYDUMP), 'rb') as file:
            value = dill.load(file)
        
        self.var_name=Path(dumper_path).stem
        
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
                return PydumpUnitReader(x)

        # if root object corresponds to a list or dict, it is list/dict[paths]
        # else, root path corresponds to the path of a unique locally serialized object
        res=handle_deserialize_node(value)
        self.metadata=res
    
    def render_txt(self):
        
        ### convert dict/list[paths] into dict/list[objects]
        def handle_txt_node(x):
            # if node is list : iterate over each element
            if isinstance(x, list):
                return [handle_txt_node(v) for v in x]
            elif isinstance(x, tuple):
                return tuple([handle_txt_node(v) for v in list(x)])
            # if node is dict : iterate over each element
            elif isinstance(x, dict):
                return {k: handle_txt_node(v) for k, v in x.items()}
            # else, node is an object
            else:
                return x.render_txt()
        
        object_txt=handle_txt_node(self.metadata)
        # object_txt is either :
        # > if Var is a dict/list : a dict/list of string corresponding to str representation of each value object
        # > else if Var is an unique object : a unique string corresponding to str representation of the unique object 

        if isinstance(object_txt, list) \
            or isinstance(object_txt, tuple) \
                or isinstance(object_txt, dict):
            return str(object_txt)
        else:
            return object_txt
