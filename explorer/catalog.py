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

    def get_columns(self, columns, check_columns=True):
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

    def get_dataset(self, columns):
        """Returns holoviews Dataset containing desired columns, plus ra+dec
        """

class ParquetCatalog(Catalog):
    index_column = 'id'
    def __init__(self, filenames, client=None):
        self.filenames = filenames
        self.client = client
        self._coords = None

    def get_columns(self, columns):
        if self.index_column not in columns:
            columns = tuple(columns) + ('id',)
        if self.client:
            return self.client.persist(dd.read_parquet(self.filenames, columns=columns).set_index(self.index_column))
        else:
            return dd.read_parquet(self.filenames, columns=columns).set_index(self.index_column)

    def _get_coords(self):
        return self.get_columns(['coord_ra', 'coord_dec']) * 180 / np.pi

