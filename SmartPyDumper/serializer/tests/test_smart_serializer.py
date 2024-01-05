import pytest
import SmartPyDumper.serializer.smart_serializer as ss
from SmartPyDumper.object_inspect.compare import SmartCompare
from pathlib import Path
import pandas as pd
import polars as pl
import duckdb
import numpy as np
from datetime import datetime
import shutil
from importlib.metadata import version

DATA_FOLDER='SmartPyDumper/serializer/tests/temp'
def init_data_folder():
    shutil.rmtree(DATA_FOLDER)
    Path(DATA_FOLDER).mkdir()

def assert_serialize(data):
    init_data_folder()
    print(data)
    print(type(data))
    ss.serialize(data, DATA_FOLDER)
    deserialized=ss.deserialize(DATA_FOLDER)
    print(deserialized)
    print(type(deserialized))
    assert SmartCompare(data, deserialized).identical

def test_serialize_unique_simple_value():
    assert_serialize('aaa')

def test_serialize_list_NaN():
    assert_serialize(np.nan)

@pytest.fixture
def dict_ex_data():
    return {
        'pandas_simple':pd.DataFrame({'aa':[1,2,3], 'bb':[7,6,8]}), 
        'pandas_float':pd.DataFrame({'aa':[4/5,2,3], 'bb':[7,1/3,8]}),
        'pandas_dates':pd.DataFrame({'aa':[datetime(2015,11,11),datetime(2015,11,11),datetime(2050,11,11)], 'bb':[7,6,8]}), 
        'pandas_None':pd.DataFrame({'aa':[None,2,3], 'bb':[7,3,8]}), 
        'pandas_NaN':pd.DataFrame({'aa':[np.nan,2,3], 'bb':[7,1/3,8]}), 
    }

def test_serialize_unique_pandas_dataframe(dict_ex_data):
    for case, df in dict_ex_data.items():
        print(case)
        assert_serialize(df)

def test_serialize_unique_duckdb_data_NOT_IMPLEMENTED(dict_ex_data):
    with pytest.raises(Exception, match='not implemented'):
        for case, df in dict_ex_data.items():
            print(case)
            assert_serialize(duckdb.sql('SELECT * FROM df'))
        
def test_serialize_unique_polars_dataframe(dict_ex_data):
    for case, df in dict_ex_data.items():
        print(case)
        assert_serialize(pl.from_pandas(df))

def test_serialize_unique_polars_dataframe_MultipleParquets_FromHttps_WithListOrDictValues():
    if version('duckdb')<='0.9.0':
        ## duckdb.read_parquet(list_urls) not existed before 0.9.0
        return
    BASE_URL='https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/21.04/output/etl/parquet/diseases/'
    #df=pl.read_parquet(BASE_URL)
    # >> ne fonctionne pas actuellement
    # >> syntaxe glob '<url>/*.parquet' : OK en local (et p-ê cloud) mais pas en http(s)
    
    # récupération manuelle de la liste des fichiers
    # ['file1.parquet', 'file2.parquet' ...]
    urls=pd.read_html(BASE_URL)[0]
    urls=urls.dropna(subset='Name')
    urls=urls[urls['Name'].str.contains('.parquet')]
    list_urls=(BASE_URL+urls['Name']).to_list()
    #dfq=pl.read_parquet(list_urls, use_pyarrow=True)
    # >> liste de fichiers https ne fonctionne pas en polars, mais ok en duckdb
    df=duckdb.read_parquet(list_urls).pl()
    assert_serialize(df)

def test_serialize_unique_polars_dataframe_Bigger_NeedingMultipleParquetSerialisation():
    #import httpfs
    URL='https://www.stats.govt.nz/assets/Uploads/Balance-of-payments/BoPIIP-June-2023-quarter/Download-data/balance-of-payments-and-international-investment-position-june-2023-quarter.csv'
    #df=duckdb.read_csv(URL).pl()
    #df=pl.from_pandas(pd.read_csv(URL))
    df=pl.DataFrame(np.random.random((1000000, 10)))
    print(df)
    assert_serialize(df)

def test_serialize_unique_polars_lazyframe(dict_ex_data):
    for case, df in dict_ex_data.items():
        print(case)
        assert_serialize(pl.from_pandas(df).lazy())

def test_serialize_list_simple_values():
    assert_serialize(['aaa', 'bbb', 1/3, datetime(2010,11,11), 44, 555, None, np.nan])

def test_serialize_list_hybrid_values(dict_ex_data):
    assert_serialize(['aaa', 
                      dict_ex_data['pandas_dates'], 
                      1/3, 
                      datetime(2010,11,11), 
                      44, 
                      dict_ex_data['pandas_NaN'], 
                      None, 
                      np.nan])

def test_serialize_dict_simple_values():
    assert_serialize(
        {
            'a' : [2,4,8], 
            786 : None, 
            33: {},
            'b': {
                'x':np.nan,
                'y':99
            }
        }
    )

def test_serialize_dict_hybrid_values(dict_ex_data):
    assert_serialize(
        {
            'AA':[1,2,3,4,{'a':8, 'b':np.nan}],
            'BB':[dict_ex_data['pandas_NaN'], dict_ex_data['pandas_None']],
            'CC':{
                'polars_df':pl.from_pandas(dict_ex_data['pandas_NaN']),
                'polars_lf':pl.from_pandas(dict_ex_data['pandas_None']).lazy(),
                'param':1/3,
            }
        }
    )