import numpy as np
import pandas as pd
import fastparquet
import glob, re
import logging

class Catalog(object):
    index_column = 'id'
    def __init__(self, data):
        self.data
        self.columns = data.columns

    def _sanitize_columns(self, columns):
        bad_cols = [c for c in columns if c not in self.columns]
        if bad_cols:
            logging.warning('Columns not available: {}'.format(bad_cols))
        return list(set(columns) - set(bad_cols))

    def get_columns(self, columns, check_columns=True):
        if check_columns:
            columns = self._sanitize_columns(columns)
        return self.data[columns]

class ParquetReadWorker(object):
    def __init__(self, columns, index_column='id'):
        if index_column not in columns:
            self.columns = columns + [index_column]
        else:
            self.columns = columns
        self.index_column = index_column

    def __call__(self, pfile):
        df = pfile.to_pandas(columns=self.columns)
        df.index = df[self.index_column]
        return df

class ParquetCatalog(Catalog):
    index_column = 'id'
    def __init__(self, filenames):
        if isinstance(filenames, str):
            self.filenames = [filenames]
        else:
            self.filenames = filenames
        self.pfiles = [fastparquet.ParquetFile(f) for f in self.filenames]
        self.columns = self.pfiles[0].columns
        if not all([p.columns==self.columns for p in self.pfiles]):
            raise ValueError('Not all parquet files have the same columns!')

        self._cache = None

    def _purge_cache(self, cols=None):
        if self._cache is not None:
            if cols is None:
                del self._cache
                self._cache = None
            else:
                for c in cols:
                    if c != self.index_column:
                        del self._cache[c]

    def get_columns(self, columns, check_columns=True, pool=None):
        if check_columns:
            columns = self._sanitize_columns(columns)

        # if self.index_column not in columns:
        #     columns = columns + [self.index_column]

        if pool is None:
            mapper = map
        else:
            mapper = pool.map

        if self._cache is None:
            cols_to_read = columns
        else:
            cols_to_read = list(set(columns) - (set(self._cache.columns)))

        if cols_to_read:
            worker = ParquetReadWorker(columns=cols_to_read, index_column=self.index_column)
            new_df = pd.concat(mapper(worker, self.pfiles))
            if self._cache is None:
                self._cache = new_df
            else:
                self._cache = self._cache.join(new_df)

        return self._cache[columns]
