import pytest
import SmartPyDumper.serializer.smart_serializer as ss
from SmartPyDumper.serializer.pydump_reader import PydumpUnitReader, PydumpVarReader
from pathlib import Path
import pandas as pd
import polars as pl
import duckdb
import numpy as np
from datetime import datetime

DATA_FOLDER='SmartPyDumper/serializer/tests/temp'

@pytest.fixture
def dict_ex_data():
    return {
        'pandas_simple':pd.DataFrame({'aa':[1,2,3], 'bb':[7,6,8]}), 
        'pandas_float':pd.DataFrame({'aa':[4/5,2,3], 'bb':[7,1/3,8]}),
        'pandas_dates':pd.DataFrame({'aa':[datetime(2015,11,11),datetime(2015,11,11),datetime(2050,11,11)], 'bb':[7,6,8]}), 
        'pandas_None':pd.DataFrame({'aa':[None,2,3], 'bb':[7,3,8]}), 
        'pandas_NaN':pd.DataFrame({'aa':[np.nan,2,3], 'bb':[7,1/3,8]}), 
    }

@pytest.fixture
def ex_hybrid_dict_data(dict_ex_data):
    return {
            'AA':[1,2,3,4,{'a':8, 'b':np.nan}],
            'BB':[dict_ex_data['pandas_NaN'], dict_ex_data['pandas_None']],
            'CC':{
                'polars_df':pl.from_pandas(dict_ex_data['pandas_NaN']),
                'polars_lf':pl.from_pandas(dict_ex_data['pandas_None']).lazy(),
                'param':1/3,
            }
        }

def test_read_pydump_metadata(ex_hybrid_dict_data):
    ss.serialize(ex_hybrid_dict_data, DATA_FOLDER)
    var=PydumpVarReader(DATA_FOLDER)
    assert 'AA' in var.metadata
    assert 'BB' in var.metadata
    test_object=var.metadata['BB'][0]
    assert 'path' in test_object.to_dict()
    assert 'type_data' in test_object.to_dict()
    assert 'pandas' in test_object.to_dict()['type_data']
    assert 'group_data' in test_object.to_dict()
    assert 'data' in test_object.to_dict()
    assert 'CC' in var.metadata

def test_read_pydump_txt(ex_hybrid_dict_data):
    ss.serialize(ex_hybrid_dict_data, DATA_FOLDER)
    var=PydumpVarReader(DATA_FOLDER)
    txt=var.render_txt()
    print(txt)
    assert 'AA' in txt
    assert 'BB' in txt
    assert 'CC' in txt