
import SmartPyDumper.serializer.smart_serializer as ss
import pandas as pd
import polars as pl
import numpy as np
import duckdb

# Define output folder
# make sure serialization folder is existing
SERIALIZATION_FOLDER='temp/ex_data'

## Below, `ex_data` is a dict 
#   containing lists 
#       containing Pandas DataFrame, 
#                  Polars DataFrame,
#                  Polars LazyFrame,
#                  duckdb data converted to Polars

# Prepare simple Pandas DataFrame
pd_df=pd.DataFrame({'aa':[np.nan,2,3], 'bb':[7,1/3,8]})
# Prepare simple Polars DataFrame
pl_df=pl.from_pandas(pd_df)
# Prepare simple Polars LazyFrame
pl_lf=pl_df.lazy()
# Prepare simple Polars DataFrame from duckdb
pl_duckdb=duckdb.sql("SELECT * FROM read_parquet('https://duckdb.org/data/prices.parquet');").pl()

# Prepare json-like variable containing hybrid data
ex_data={
        'AA':[1,2,3,4, {'a':8, 'b':np.nan}],
        'BB':[pd_df, pd_df.sum()],
        'CC':{
            'polars_df':pl_df,
            'polars_lf':pl_lf,
            'duckdb':pl_duckdb,
            'param':1/3,
        }
}

print(ex_data)
ss.serialize(ex_data, SERIALIZATION_FOLDER)

# [...]

deserialized=ss.deserialize(SERIALIZATION_FOLDER)
print(deserialized)