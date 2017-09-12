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

    @property
    def ra(self):
        if self._coords is None:
            self._coords = self._get_coords()
        return self._coords['ra']

    @property
    def dec(self):
        if self._coords is None:
            self._coords = self._get_coords()
        return self._coords['dec']

    def _get_coords(self):
        return NotImplementedError

class ParquetCatalog(Catalog):
    index_column = 'id'
    def __init__(self, filenames, client=None):
        self.filenames = filenames
        self.client = client
        self._coords = None

        self._df = None

    def _read_data(self, columns):
        if self.client:
            return self.client.persist(dd.read_parquet(self.filenames, columns=columns))
        else:
            return dd.read_parquet(self.filenames, columns=columns)

    def get_columns(self, columns, query=None):

        if self._df is None:
            if self.index_column not in columns:
                cols_to_get = list(columns) + [self.index_column]

            self._df = self._read_data(cols_to_get).set_index(self.index_column)

        else:
            cols_to_get = list(set(columns) - set(self._df.columns))
            if cols_to_get:
                new = self._read_data(cols_to_get + [self.index_column]).set_index(self.index_column)
                self._df = self._df.join(new)

        return self._df[list(columns)]

    def _get_coords(self):
        df = (self.get_columns(['coord_ra', 'coord_dec']) * 180 / np.pi)
        return df.rename(columns={'coord_ra':'ra', 'coord_dec':'dec'})
