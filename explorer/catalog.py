import numpy as np
import pandas as pd
import dask.dataframe as dd
from distributed import Future
import fastparquet
import glob, re
import logging

class Catalog(object):
    index_column = 'id'
    def __init__(self, data):
        self.data
        self.columns = data.columns

        self._coords = None

    def _sanitize_columns(self, columns):
        bad_cols = [c for c in columns if c not in self.columns]
        if bad_cols:
            logging.warning('Columns not available: {}'.format(bad_cols))
        return list(set(columns) - set(bad_cols))

    def get_columns(self, columns, check_columns=True, query=None):
        if check_columns:
            columns = self._sanitize_columns(columns)
        return self.data[columns]

class CatalogDifference(Catalog):
    def __init__(self, cat1, cat2):
        self.cat1 = cat1
        self.cat2 = cat2

    

    def get_columns(self, *args, **kwargs):
        df1 = self.cat1.get_columns(*args, **kwargs)
        df2 = self.cat2.get_columns(*args, **kwargs)

        # Join catalogs according to match

        return df_matched


class ParquetCatalog(Catalog):
    def __init__(self, filenames, client=None):
        self.filenames = filenames
        self.client = client
        self._coords = None

        self._df = None

    def _read_data(self, columns):
        if self.client:
            df = self.client.persist(dd.read_parquet(self.filenames, columns=columns))
        else:
            df = dd.read_parquet(self.filenames, columns=columns)

        return df

    @property
    def df(self):
        if isinstance(self._df, Future):
            return self._df.result()
        else:
            return self._df

    def get_columns(self, columns, query=None, use_cache=True):
        
        if use_cache:
            if self._df is None:
                cols_to_get = list(columns)

                if self.client:
                    self._df = self.client.persist(self._read_data(cols_to_get))
                else:
                    self._df = self._read_data(cols_to_get)

            else:
                cols_to_get = list(set(columns) - set(self._df.columns))
                if cols_to_get:
                    new = self._read_data(cols_to_get)
                    if self.client:
                        self._df = self.client.persist(self._df.merge(new))
                    else:
                        self._df = self._df.merge(new)

            if self.client:
                return self.client.persist(self.df[list(columns)])
            else:
                return self.df[list(columns)]

        else:
            cols_to_get = list(columns)
            return self._read_data(cols_to_get)
