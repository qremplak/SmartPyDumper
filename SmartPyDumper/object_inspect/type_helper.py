from enum import Enum

class VGROUP(Enum):
    MODULE_FUNCTION = 'module_function'
    DATAFRAME = "dataframe"
    NUMPY = 'numpy'
    LIST_DICT = 'list_dict'
    PRIMITIVE = 'primitive'
    OTHER = 'other'

def get_str_type(x, lower=True):
    _type= ".".join([x.__class__.__module__, x.__class__.__name__])
    if lower:
        return _type.lower()
    else:
        _type
    #return str(type(x)).lower()

def detect_group(var):
    type_=get_str_type(var)
    type_str=str(type_).lower()
    if callable(var) or 'module' in type_str:
        return VGROUP.MODULE_FUNCTION
    #elif 'pandas' in type_str:
    elif 'pandas' in type_str \
        or 'polars' in type_str \
        or 'duckdb' in type_str:
        return VGROUP.DATAFRAME
    elif 'numpy' in type_str:
        return VGROUP.NUMPY
    elif type_ is list or type_ is dict or type_ is tuple:
        return VGROUP.LIST_DICT
    elif isinstance(var, (bool, str, int, float, type(None))):
        return VGROUP.PRIMITIVE
    else:
        return VGROUP.OTHER

def cast_to_group(var, sample=None):
    type_=type(var)
    type_str=get_str_type(var)
    group=detect_group(var)
    if group==VGROUP.MODULE_FUNCTION:
        return var, group
    elif group==VGROUP.DATAFRAME:
        if 'series' in type_str:
            if sample is not None:
                var=var.head(sample)
            var=var.to_frame()
        if 'polars' in type_str and 'laryframe' in type_str:
            if sample is not None:
                var=var.head(sample)
            var=var.collect().to_pandas()
        if 'polars' in type_str and 'dataframe' in type_str:
            if sample is not None:
                var=var.head(sample)
            var=var.to_pandas()
        return var, group
    elif group==VGROUP.NUMPY:
        return var, group
    elif group==VGROUP.LIST_DICT:
        return var, group
    elif group==VGROUP.PRIMITIVE:
        return var, group
    elif group==VGROUP.OTHER:
        return var, group

if __name__=='__main__':
    import pandas as pd
    print(get_str_type(pd.DataFrame()))
    import pandas as pd
    print(cast_to_group(pd.Series()))