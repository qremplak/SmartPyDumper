import pytest

from SmartPyDumper.object_inspect.compare import SmartCompare
from datetime import datetime
import pandas as pd
import polars as pl
import duckdb
import numpy as np

def test_compare_simple_values():
    assert SmartCompare('a', 'a').identical
    assert SmartCompare(111, 111).identical
    assert SmartCompare(46.60, 46.60).identical
    assert SmartCompare(datetime(2050,11,11), datetime(2050,11,11)).identical
    assert SmartCompare(True, True).identical
    assert SmartCompare(False, False).identical

    assert not SmartCompare('a', 'b').identical
    assert not SmartCompare(111, 112).identical
    assert not SmartCompare(46.60, 46.65).identical
    assert not SmartCompare(datetime(2050,11,11), datetime(2055,11,11)).identical
    assert not SmartCompare(True, False).identical

def test_compare_None_values():
    assert SmartCompare(None, None).identical
    
def test_compare_NaN_values():
    assert SmartCompare(np.nan, np.nan).identical

def test_compare_NaN_to_None_values():
    assert not SmartCompare(np.nan, None).identical

def test_compare_float_values():
    assert SmartCompare(1/3, 1/3).identical
    assert not SmartCompare(1/3, 0.333333333333333).identical
    ### /!\ à partir d'un certain degré de précision, considérés égaux
    assert SmartCompare(1/3, 0.3333333333333333333333333333333333333333333333333333).identical
    
def test_compare_dicts_simple_values():
    assert SmartCompare(
        ['aaa', 'bbb', {4:[7,6,8], 'ccc':None}],
        ['aaa', 'bbb', {4:[7,6,8], 'ccc':None}]
    ).identical
    assert SmartCompare(
        {
            'a' : [2,4,8], 
            786 : np.nan, 
            33: {},
            'b': {
                'x':55,
                'y':99
            }
        },
        {
            'a' : [2,4,8], 
            786 : np.nan, 
            33: {},
            'b': {
                'x':55,
                'y':99
            }
        }
    ).identical
    assert not SmartCompare(
        {
            'a' : [2,4,8], 
            786 : None, 
            33: {},
            'b': {
                'x':55,
                'y':99
            }
        },
        {
            'a' : [2,4,10], # <---- DIFF
            786 : None, 
            33: {},
            'b': {
                'x':55,
                'y':99
            }
        }
    ).identical

@pytest.fixture
def dict_ex_data():
    return {
        'identical_simple':{
            'a': pd.DataFrame({'aa':[1,2,3], 'bb':[7,6,8]}), 
            'b': pd.DataFrame({'aa':[1,2,3], 'bb':[7,6,8]}), 
        },
        'identical_float':{
            'a': pd.DataFrame({'aa':[4/5,2,3], 'bb':[7,1/3,8]}), 
            'b': pd.DataFrame({'aa':[4/5,2,3], 'bb':[7,1/3,8]}), 
        },
        'identical_dates':{
            'a': pd.DataFrame({'aa':[datetime(2015,11,11),datetime(2015,11,11),datetime(2050,11,11)], 'bb':[7,6,8]}), 
            'b': pd.DataFrame({'aa':[datetime(2015,11,11),datetime(2015,11,11),datetime(2050,11,11)], 'bb':[7,6,8]}), 
        },
        'identical_None':{
            'a': pd.DataFrame({'aa':[None,2,3], 'bb':[7,3,8]}), 
            'b': pd.DataFrame({'aa':[None,2,3], 'bb':[7,3,8]}), 
        },
        'identical_NaN':{
            'a': pd.DataFrame({'aa':[np.nan,2,3], 'bb':[7,1/3,8]}), 
            'b': pd.DataFrame({'aa':[np.nan,2,3], 'bb':[7,1/3,8]}), 
        },
        'diff_simple':{
            'a': pd.DataFrame({'aa':[1,2,3], 'bb':[7,6,8]}), 
            'b': pd.DataFrame({'aa':[1,2,4], 'bb':[8,6,8]}), 
        },
        'diff_None':{
            'a': pd.DataFrame({'aa':[None,2,3], 'bb':[7,6,8]}), 
            'b': pd.DataFrame({'aa':[1,2,3], 'bb':[7,6,8]}), 
        },
        'diff_NaN':{
            'a': pd.DataFrame({'aa':[np.nan,2,3], 'bb':[7,6,8]}), 
            'b': pd.DataFrame({'aa':[1,2,3], 'bb':[7,6,8]}), 
        },
        'diff_dates':{
            'a': pd.DataFrame({'aa':[datetime(2010,11,11),datetime(2015,11,11),datetime(2050,11,11)], 'bb':[7,6,8]}), 
            'b': pd.DataFrame({'aa':[datetime(2015,11,11),datetime(2015,11,11),datetime(2050,11,11)], 'bb':[7,6,8]}), 
        },
        'diff_size':{
            'a': pd.DataFrame({'aa':[1,2,3, 7], 'bb':[7,6,8, 7]}), 
            'b': pd.DataFrame({'aa':[1,2,3], 'bb':[7,6,8]}), 
        },
        'diff_structure':{
            'a': pd.DataFrame({'aa':[1,2,3], 'cc':[7,6,8]}), 
            'b': pd.DataFrame({'aa':[1,2,3], 'bb':[7,6,8]}), 
        },
    }

def assert_compare_data(dict_ex_data, prepare=(lambda x:x)):
    print(prepare)
    for case, data in dict_ex_data.items():
        print(case)
        data = dict((k, prepare(v)) for k,v in data.items())
        if 'diff' in case:
            assert not SmartCompare(**data).identical
        else:
            assert SmartCompare(**data).identical

def test_compare_unique_pandas_dataframe(dict_ex_data):
    assert_compare_data(dict_ex_data)

def test_compare_unique_duckdb_data_NOT_IMPLEMENTED(dict_ex_data):
    with pytest.raises(Exception, match='not implemented'):
        assert_compare_data(
            dict_ex_data,
            prepare=lambda x: duckdb.sql('SELECT * FROM x')
            )

def test_compare_unique_polars_dataframe(dict_ex_data):
    assert_compare_data(
        dict_ex_data,
        prepare=lambda x: pl.from_pandas(x)
        )
        
def test_compare_unique_polars_lazyframe(dict_ex_data):
    assert_compare_data(
        dict_ex_data,
        prepare=lambda x: pl.from_pandas(x).lazy()
        )

def test_compare_dicts_hybrid_values(dict_ex_data):
    assert SmartCompare(
        {
            'AA':[1,2,3,4,{'a':8, 'b':np.nan}],
            'BB':[dict_ex_data['identical_dates']['a'], dict_ex_data['identical_None']['b']],
            'CC':{
                'polars_df':pl.from_pandas(dict_ex_data['identical_float']['a']),
                'polars_lf':pl.from_pandas(dict_ex_data['diff_NaN']['a']).lazy(),
                'param':1/3,
            }
        },
        {
            'AA':[1,2,3,4,{'a':8, 'b':np.nan}],
            'BB':[dict_ex_data['identical_dates']['a'], dict_ex_data['identical_None']['b']],
            'CC':{
                'polars_df':pl.from_pandas(dict_ex_data['identical_float']['a']),
                'polars_lf':pl.from_pandas(dict_ex_data['diff_NaN']['a']).lazy(),
                'param':1/3,
            }
        }
    ).identical
    assert not SmartCompare(
        {
            'AA':[1,2,3,4,{'a':8, 'b':np.nan}],
            'BB':[dict_ex_data['identical_dates']['a'], dict_ex_data['identical_None']['b']],
            'CC':{
                'polars_df':pl.from_pandas(dict_ex_data['identical_float']['a']),
                'polars_lf':pl.from_pandas(dict_ex_data['diff_NaN']['a']).lazy(),
                'param':1/3,
            }
        },
        {
            'AA':[1,2,3,4,{'a':8, 'b':np.nan}],
            'BB':[dict_ex_data['identical_dates']['a'], dict_ex_data['identical_None']['b']],
            'CC':{
                'polars_df':pl.from_pandas(dict_ex_data['identical_float']['a']),
                'polars_lf':pl.from_pandas(dict_ex_data['diff_NaN']['b']).lazy(), # <------ diff
                'param':1/3,
            }
        }
    ).identical