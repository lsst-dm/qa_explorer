import re
import json
from itertools import product

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

class ParquetTable(object):
    def __init__(self, filename):
        self.filename = filename
        self.pf = pq.ParquetFile(filename)

        self._pandas_md = None
        self._columns = None
        self._columnIndex = None  

    @classmethod
    def writeParquet(cls, df, filename):
        """Write pandas dataframe to parquet
        """
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filename, compression='none')

    @property
    def pandas_md(self):
        if self._pandas_md is None:
            self._pandas_md = json.loads(self.pf.metadata.metadata[b'pandas'])
        return self._pandas_md

    @property
    def columnIndex(self):
        if self._columnIndex is None:
            self._columnIndex = self._get_columnIndex()
        return self._columnIndex
    
    def _get_columnIndex(self):
        return pd.Index(self.columns)
    
    @property
    def columns(self):
        if self._columns is None:
            self._columns = self._get_columns()
        return self._columns
    
    def _get_columns(self):
        return self.pf.metadata.schema.names
    
    def to_df(self, columns=None):
        if columns is None:
            return self.pf.read().to_pandas()
        
        try:
            df = self.pf.read(columns=columns, use_pandas_metadata=True).to_pandas()
        except AttributeError:
            raise
            columns = [c for c in columns in c in self.columnIndex]
            df = self.pf.read(columns=columns, use_pandas_metadata=True).to_pandas()
        
        return df


class MultilevelParquetTable(ParquetTable):
            
    @property
    def columnLevels(self):
        return self.columnIndex.names
        
    def _get_columnIndex(self):
        levelNames = [f['name'] for f in self.pandas_md['column_indexes']]
        return pd.MultiIndex.from_tuples(self.columns, names=levelNames)
        
    def _get_columns(self):
        columns = self.pf.metadata.schema.names
        pattern = "'(.*)', '(.*)', '(.*)'"
        matches = [re.search(pattern, c) for c in columns]
        return [m.groups() for m in matches if m is not None]
    
    def to_df(self, columns=None):
        if columns is None:
            return self.pf.read().to_pandas()
        
        if isinstance(columns, dict):
            columns = self._colsFromDict(columns)
                        
        pfColumns = self._stringify(columns)
        try:
            df = self.pf.read(columns=pfColumns, use_pandas_metadata=True).to_pandas()
        except AttributeError:
            columns = [c for c in columns in c in self.columnIndex]
            pfColumns = self._stringify(columns)
            df = self.pf.read(columns=pfColumns, use_pandas_metadata=True).to_pandas()
        
        # Drop levels of column index that have just one entry
        levelsToDrop = [n for l,n in zip(df.columns.levels, df.columns.names) 
                        if len(l)==1]
        df.columns = df.columns.droplevel(levelsToDrop)
        return df
        
    def _colsFromDict(self, colDict):
        new_colDict = {}
        for i,l in enumerate(self.columnLevels):
            if l in colDict:
                if isinstance(colDict[l], str):
                    new_colDict[l] = [colDict[l]]
                else:
                    new_colDict[l] = colDict[l]
            else:
                new_colDict[l] = self.columnIndex.levels[i]
                
        levelCols = [new_colDict[l] for l in self.columnLevels]
        cols = product(*levelCols)
        return list(cols)
        
    def _stringify(self, cols):
        return [str(c) for c in cols]

