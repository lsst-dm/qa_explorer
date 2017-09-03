import numpy as np
import pandas as pd
import dask.dataframe as dd
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
    def __init__(self, filenames, client=None):
        self.filenames = filenames
        self.client = client

    def get_columns(self, columns, check_columns=True, pool=None):
        if self.index_column not in columns:
            columns = tuple(columns) + ('id',)
        if self.client:
            return self.client.persist(dd.read_parquet(self.filenames, columns=columns).set_index(self.index_column))
        else:
            return dd.read_parquet(self.filenames, columns=columns).set_index(self.index_column)

