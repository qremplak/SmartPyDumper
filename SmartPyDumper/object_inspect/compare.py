import pandas as pd
import polars as pl
from deepdiff import DeepDiff # intéressant mais ne gère que les diff de dict/list de valeurs simples (non pandas, polars etc)
# >> obligé de faire une comparaison unitaire récursive de chaque élément complexe
# >> donc quitte à effectuer un parcours récursif autant comparer 1 à 1 les éléments simples également
from SmartPyDumper.object_inspect.type_helper import get_str_type
import warnings


a_label='old_value'
b_label='new_value'
WARNING_SIZE=100000

### WORK FOR : unique value, simple dict/list, pandas, polars, duckdb (convert to polar first)
### DONT WORK FOR : dict/list containing pandas, polars, etc.
class NodeDiff():
    def __init__(self, dict_diff, type_var, identical):
        self.dict_diff=dict_diff
        self.type_var=type_var
        self.identical=identical

    def to_msg(self):
        return self.dict_diff.to_json()
    
    @classmethod
    def diff_dict_list(cls, a, b):
        dict_diff=DeepDiff(a, b, ignore_nan_inequality=True)
        #print(type(a), type(b))
        #print(dict_diff)
        return cls(dict_diff, 'dict', len(dict_diff)==0)

    @classmethod
    def diff_pandas(cls, a, b):
        #print('diff pandas')
        _type='pandas.DataFrame'
        #https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.compare.html
        diff_columns=DeepDiff(list(a.columns), list(b.columns))
        if len(diff_columns)!=0:
            #print(diff_columns)
            diff_columns['columns_changed'] = diff_columns.pop('values_changed')
            return cls(diff_columns, _type, False)
        elif a.shape!=b.shape:
            dict_diff= {'size_changed': f'Shapes are differents : {a.shape}!={b.shape}'}
            return cls(dict_diff, _type, False)
        else:
            #print('>> compare dataframe contents')
            # l'utilisation de df.compare(df2) renvoie un dataframe d'une taille "non optimisée" (beaucoup de NaN majorant les dimensions)
            """
                    aa                  bb          
                old_value new_value old_value new_value
                0       NaN       NaN       7.0       8.0
                2       3.0       4.0       NaN       NaN
            """
            # >> application colonne par colonne, et renvoie d'une structure comparable à DeepDiff
            all_diff={}
            for col in a.columns:
                try:
                    diff= a[col].compare(
                        b[col], 
                        align_axis=1, result_names=(a_label, b_label))
                    """
                        old_value  new_value
                    2        3.0        4.0
                    """
                except ValueError as e:
                    if 'more than one element is ambiguous' in str(e):
                        print(f'Column {col} contains list or dict values, unable to compare all contents.')
                        # comparaison minimale, ex. (a[col].str.len()).compare(b[col].str.len())
                        print('> Minimal comparison : observe length of content')
                        diff=(a[col].str.len()).compare(
                            b[col].str.len(),
                            align_axis=1, result_names=('old_content_length', 'new_content_length')
                        )
                    else:
                        raise ValueError(e)
            
                dtype=str(diff.index.dtype)
                if 'int' in dtype or 'float' in dtype:
                    diff.index=diff.index.astype(str)
                else:
                    diff.index="'"+diff.index.astype(str)+"'"
                diff.index=f"root['{col}'].loc["+diff.index+"]"
                all_diff.update(diff.to_dict(orient='index'))

            if len(all_diff)>0:
                return cls({'values_changed' : all_diff}, _type, False)
            else:
                return cls({}, _type, True)
            
    @classmethod
    def smart_diff(cls, a, b):
        #print(a, b)
        a_type=get_str_type(a)
        b_type=get_str_type(b)
        if a_type!=b_type:
            #print('type changed')
            diff_dict={'type_changed':f'Objects types are differents : {a_type}!={b_type}'}
            return cls(diff_dict, a_type, False)
        else:
            #print('same type')
            str_type=a_type
            _type=type(a)

        if 'pandas' in str_type:
            #print('pandas')
            #raise Exception('not able to compare pandas yet')
            #a=a.to_dict()
            #b=b.to_dict()
            if len(a)>WARNING_SIZE or len(b)>WARNING_SIZE:
                warnings.warn("[Warning] Compare function is not recommended for Big DataFrames.")
            #print(str_type)
            if 'series' in str_type:
                return cls.diff_pandas(a.to_frame(), b.to_frame())
            elif 'dataframe' in str_type:
                #print('is dataframe')
                return cls.diff_pandas(a, b)
            
        elif 'polars' in str_type:
            #print('polars')

            if 'lazyframe' in str_type:
                warnings.warn("[Warning] Compare function is not implemented for native Polars LazyFrame. Data is converted to Pandas first.")
                # no len function for lazyframe, always raise warning for BigData
                warnings.warn("[Warning] Compare function is not recommended for Big DataFrames.")
                return cls.diff_pandas(a.collect().to_pandas(), b.collect().to_pandas())
            elif 'dataframe' in str_type:
                if len(a)>WARNING_SIZE or len(b)>WARNING_SIZE:
                    warnings.warn("[Warning] Compare function is not recommended for Big DataFrames.")
                warnings.warn("[Warning] Compare function is not implemented for native Polars DataFrame. Data is converted to Pandas first.")
                return cls.diff_pandas(a.to_pandas(), b.to_pandas())
            
        elif 'duck' in str_type:
            #print('polars')
            #warnings.warn("[Warning] Compare function is not implemented for native duckdb. Data is converted to Pandas first.")
            raise Exception('Compare function is not implemented for duckdb, please convert to polars first with .pl()')
            #print('a.df(), b.df()')
            #print(a.df(), b.df())
            #return diff_pandas(a.df(), b.df())
        elif not isinstance(_type, list) and not isinstance(_type, dict):
            #print('compare simple value')
            #identical=a==b
            #if not identical:
            #    diff_dict={'value_changed', '{a}!={b}'}
            #else:
            #    diff_dict={}
            #return cls(diff_dict, str_type, identical)
            
            # utiliser diff_dict_list même pour une valeur concrète simple
            # >> semble fonctionner + considérer np.nan=np.nan >> ce qui nous arrange dans notre cas d'usage
            diff=cls.diff_dict_list(a, b)
            return diff
        
        diff=cls.diff_dict_list(a, b)
        return diff
    
class SmartCompare:

    def __init__(self, a, b):
        self.identical=True
        # on effectue une première comparaison rapide
        diff=NodeDiff.smart_diff(a, b)
        #print('>>>>>>', diff.identical)
        # Si différence, 3 cas de figure
        # 1. objets concrets simples (str, int, float, ...) avec a!=b >> si différent, pas besoin d'aller plus loin
        # 1. objets concrets complexes (pandas, polars, ...) avec a!=b >> si différent, pas besoin d'aller plus loin
        # 2. dict/list simple avec différences observées sur la structure ou un contenu >> pas besoin d'aller plus loin
        # 3. dict/list hybrides mais structure différente, pas besoin de tester le contenu >> pas besoin d'aller plus loin
        # (NodeDiff ne peut pas comparer le contenu si valeur complexe)
        if not diff.identical:
            self.tree_diff=diff
            self.identical=False
        # si objets hybrides mais structure identique, on peut comparer leur contenu 1 à 1
        else:
            self.tree_diff=self.recursive_compare(a, b)

    def recursive_compare(self, a, b):
        # if node is list : iterate over each element
        if isinstance(a, list):
            return [self.recursive_compare(x,y) for x,y in zip(a,b)]
        # if node is dict : iterate over each element
        elif isinstance(a, dict):
            return {k: self.recursive_compare(a[k], b[k]) for k in a.keys()}
        # if node is an concrete object, evaluate it
        else:
            diff= NodeDiff.smart_diff(a, b)
            if not diff.identical:
                self.identical=False
            return diff
        
    def to_msg(self):
        pass

if __name__=='__main__':
    import numpy as np
    print('\n > ex 1')
    print(SmartCompare('aa', 'aa').identical)
    print(SmartCompare('aa', 'bb').identical)
    print('\n > ex 2')
    print(SmartCompare({'aa':[1,2,3]}, {'aa':[1,2,4]}).identical)
    print('\n > ex 3')
    print(SmartCompare({'aa':[1,2,3]}, {'aa':[1,2,3]}).identical)
    print('\n > ex 4')
    print(SmartCompare({'aa':[4,2,3]}, {'aa':[1,2,4]}).identical)
    print('\n > ex 5')
    print(SmartCompare(pd.DataFrame({'aa':[1,2,3]}), pd.DataFrame({'aa':[1,2,3]})).identical)
    print('\n > ex 6')
    print(SmartCompare(pd.DataFrame({'aa':[1,2,3], 'bb':[7,6,8]}), pd.DataFrame({'aa':[1,2,4], 'bb':[8,6,8]})).identical)
    print('\n > ex 7')
    print(SmartCompare(pd.DataFrame({'aa':[1,2,3]}), pd.DataFrame({'bb':[1,2,4]})).identical)