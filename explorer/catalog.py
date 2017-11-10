import os
import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask.array as da
from distributed import Future
import fastparquet
import glob, re
import random
import logging
import hashlib
from functools import reduce
from operator import add

from .match import match_lists
from .functors import Labeller

class Catalog(object):
    index_column = 'id'

    def __init__(self, data, name=None):
        self.data
        self.columns = data.columns
        self.name = name
        self._initialize()

    # def __getstate__(self):
    #     # Check this out.  something's going funny with unpickling matched catalogs...
    #     # self._initialize()
    #     odict = self.__dict__
    #     return odict

    # def __setstate__(self, d):
    #     self.__dict__ = d

    def _initialize(self):
        self._coords = None
        self._md5 = None

    def _stringify(self):
        """Return string form of catalog, for md5 hashing
        """
        raise NotImplementedError

    def _compute_md5(self):
        return hashlib.md5(self._stringify())

    @property
    def md5(self):
        if self._md5 is None:
            self._md5 = self._compute_md5().hexdigest()
        return self._md5

    def __hash__(self):
        return hash(self.md5)

    def _sanitize_columns(self, columns):
        bad_cols = [c for c in columns if c not in self.columns]
        if bad_cols:
            logging.warning('Columns not available: {}'.format(bad_cols))
        return list(set(columns) - set(bad_cols))

    def get_columns(self, columns, check_columns=True, query=None):
        if check_columns:
            columns = self._sanitize_columns(columns)
        return self.data[columns]

    def _get_coords(self):
        df = self.get_columns(['coord_ra', 'coord_dec'], add_flags=False)

        # Hack to avoid phantom 'dir0' column 
        df = df.compute()
        if 'dir0' in df.columns:
            df = df.drop('dir0', axis=1)

        self._coords = (df*180 / np.pi).rename(columns={'coord_ra':'ra',
                                                        'coord_dec':'dec'})

    @property
    def df_all(self):
        return self.get_columns(self.columns, add_flags=False)

    @property
    def coords(self):
        if self._coords is None:
            self._get_coords()
        return self._coords

    @property
    def ra(self):
        return self.coords['ra']

    @property
    def dec(self):
        return self.coords['dec']

    @property
    def index(self):
        return self.coords.index

    def _apply_func(self, func, query=None, client=None):
        df = self.get_columns(func.columns, query=query, client=client)
        vals = func._func(df)
        return vals

class MatchedCatalog(Catalog):
    def __init__(self, cat1, cat2, match_radius=0.5, tags=None, 
                    match_registry=None):
        self.cat1 = cat1
        self.cat2 = cat2

        self.tags = ['1', '2'] if tags is None else tags

        self.match_radius = match_radius

        self.match_registry = match_registry

        self._initialize()

    def _initialize(self):
        self._matched = False
        self._coords = None
        self._match_distance = None
        self._match_inds1 = None
        self._match_inds2 = None
        self._md5 = None

    def _stringify(self):
        return self.cat1._stringify() + self.cat2._stringify()

    @property
    def coords(self):
        return self.cat1.coords

    def match(self, **kwargs):
        return self._match_cats(**kwargs)

    def _read_registry(self):
        if self.match_registry is None:
            raise ValueError
        store = pd.HDFStore(self.match_registry)
        df = store['md5_{}'.format(self.md5)]
        store.close()
        inds1 = df.index
        inds2 = pd.Int64Index(df['id2'], name='id')
        dist = df['distance']


        return inds1, inds2, dist   

    def _write_registry(self, match_df):
        if self.match_registry is None:
            return
        else:
            match_df.to_hdf(self.match_registry, 'md5_{}'.format(self.md5))

    def _test_registry(self):
        id1, id2, dist = self._read_registry()

        self.match(recalc=True)
        assert (id1==self._match_inds1).all()
        assert (id2==self._match_inds2).all()
        assert (dist==self._match_distance).all()

    def _match_cats(self, recalc=False):
        try:
            if recalc:
                raise ValueError
            i1, i2, d = self._read_registry()
        except (KeyError, ValueError):

            ra1, dec1 = self.cat1.ra, self.cat1.dec
            ra2, dec2 = self.cat2.ra, self.cat2.dec
            id1 = ra1.index
            id2 = ra2.index

            dist, inds = match_lists(ra1, dec1, ra2, dec2, self.match_radius/3600)

            good = np.isfinite(dist)

            logging.info('{0} matched within {1} arcsec, {2} did not.'.format(good.sum(), self.match_radius, (~good).sum()))

            # Save indices as labels, not positions, as required by dask
            i1 = id1[good]
            i2 = id2[inds[good]]
            d = pd.Series(dist[good] * 3600, index=id1[good])

            match_df = pd.DataFrame({'id2':i2, 'distance':d}, index=i1)
            self._write_registry(match_df)

        self._match_inds1 = i1
        self._match_inds2 = i2
        self._match_distance = d

        self._matched = True

    @property
    def match_distance(self):
        if self._match_distance is None:
            self._match_cats()
        return self._match_distance

    @property
    def match_inds1(self):
        if self._match_inds1 is None:
            self._match_cats()
        return self._match_inds1

    @property
    def match_inds2(self):
        if self._match_inds2 is None:
            self._match_cats()
        return self._match_inds2

    @property
    def match_inds(self):
        return self.match_inds1, self.match_inds2

    def get_columns(self, *args, **kwargs):

        df1 = self.cat1.get_columns(*args, **kwargs)
        df2 = self.cat2.get_columns(*args, **kwargs)
        # df2.set_index(dd.Series(df1.index))

        return df1, df2


    def _apply_func(self, func, query=None, how='difference', client=None):
        df1, df2 = self.get_columns(func.columns, query=query, client=client)
        if func.allow_difference:
            id1, id2 = self.match_inds
            v1 = func._func(df1).compute().loc[id1].values
            v2 = func._func(df2).compute().loc[id2].values
            if how=='difference':
                vals = pd.Series(v1 - v2, index=id1)
            elif how=='sum':
                vals = pd.Series(v1 + v2, index=id1)
            elif how=='second':
                vals = pd.Series(v2, index=id1)
            elif how=='first':
                vals = pd.Series(v1, index=id1)
        else:
            vals = func._func(df1)

        return vals

class FuncWorker(object):
    def __init__(self, func, **kwargs):
        self.func = func
        self.kwargs = kwargs

    def __call__(self, catalog):
        return self.func(catalog, **self.kwargs)

class AlignWorker(object):
    def __init__(self, coadd_vals):
        self.coadd_vals = coadd_vals

    def __call__(self, vals):
        return self.coadd_vals.align(vals)[1]

class MultiMatchedCatalog(MatchedCatalog):
    def __init__(self, coadd_cat, visit_cats, **kwargs):

        self.coadd_cat = coadd_cat
        # Test each visit cat
        good_visit_cats = []
        for v in visit_cats:
            try:
                v.columns
                good_visit_cats.append(v)
            except:
                continue

        self.visit_cats = good_visit_cats
        self.subcats = [MatchedCatalog(self.coadd_cat, v, **kwargs) 
                            for v in self.visit_cats]
        self._matched = False
        self._initialize()

    def _initialize(self):
        [c._initialize() for c in self.visit_cats]
        [c._initialize() for c in self.subcats]
        self._match_distance = None
        self._md5 = None

    def _test_registry(self):
        [c._test_registry() for c in self.subcats]

    @property
    def cat1(self):
        return self.coadd_cat

    def match(self, raise_exceptions=False, **kwargs):
        for i,c in enumerate(self.subcats):
            try:
                c.match(**kwargs)
            except:
                if raise_exceptions:
                    raise
                logging.warning('Skipping catalog {}.'.format(i))
        self._matched = True

    def get_columns(self, *args, **kwargs):
        """Returns list of dataframes: df1, then N x other dfs
        """
        df1 = self.coadd_cat.get_columns(*args, **kwargs)
        return df1, tuple(c.get_columns(*args, **kwargs) for c in self.visit_cats)

    def _apply_func(self, func, query=None, how='all', client=None):
        if client and not self._matched:
            self.match()

        coadd_vals = func(self.coadd_cat, query=query, client=client)
        if ((isinstance(func, Labeller) or not func.allow_difference) 
            and how != 'all'):
            how = 'coadd'

        if how=='coadd':
            return coadd_vals

        if client:
            func_worker = FuncWorker(func, query=query, how='second')
            visit_vals = client.map(func_worker, self.subcats)

            align_worker = AlignWorker(coadd_vals)
            aligned_vals = client.map(align_worker, visit_vals)
            aligned_vals = [v.result() for v in aligned_vals]
        else:
            visit_vals = [func(c, query=query, how='second', client=client) for c in self.subcats]
            aligned_vals = [coadd_vals.align(v)[1] for v in visit_vals]
        val_df = pd.concat(aligned_vals, axis=1)
        if how=='all':
            return val_df
        elif how=='stats':
            return pd.DataFrame({'mean':val_df.mean(axis=1),
                                 'std':val_df.std(axis=1),
                                 'count':val_df.count(axis=1)})
        elif how=='mean':
            return val_df.mean(axis=1)
        elif how=='std':
            return val_df.std(axis=1)

    @property
    def match_distance(self):
        if self._match_distance is None:
            coadd = pd.Series(index=self.coadd_cat.index)
            aligned_dists = [coadd.align(c.match_distance)[1] for c in self.subcats]
            self._match_distance = pd.concat(aligned_dists, axis=1, 
                                            keys=[('distance', i) for i in range(len(aligned_dists))]).dropna(how='all')
        return self._match_distance

class ParquetCatalog(Catalog):
    def __init__(self, filenames, name=None):
        if type(filenames) not in [list, tuple]:
            self.filenames = [filenames]

        # Ensure sorted list for hash consistency
        self.filenames = list(set(filenames))
        self.filenames.sort()

        self._name = name 
        self._initialize()

    def _initialize(self):
        self._coords = None
        self._df = None
        self._columns = None
        self._flags = None
        self._md5 = None

    def _stringify(self):
        # To be really careful, you could read the whole file, e.g.:
        # return reduce(add, [open(f, 'rb').read() for f in self.filenames])
        
        # Or, to be fast/sloppy, just read the filenames
        return reduce(add, [bytes(os.path.abspath(f), encoding='utf8') for f in self.filenames])

    @property 
    def name(self):
        if self._name is None:
            self._name = ''.join(random.choices(string.ascii_lowercase, k=5))
        return self._name

    @property
    def columns(self):
        if self._columns is None:
            self._columns = list(dd.read_parquet(self.filenames[0]).columns)
        return self._columns

    @property
    def flags(self):
        if self._flags is None:
            self._flags = list(dd.read_parquet(self.filenames[0]).select_dtypes(include=['bool']).columns)
        return self._flags

    def get_flags(self, flags=None):
        flags = self.flags if flags is None else flags

        return self.get_columns(flags)

    def _read_data(self, columns, query=None, add_flags=True, client=None):
        if add_flags:
            columns = columns + self.flags
        if client is not None:
            df = client.persist(dd.read_parquet(self.filenames, columns=columns))
        else:
            df = dd.read_parquet(self.filenames, columns=columns)

        if query:
            df = df.query(query)

        if 'dir0' in df.columns:
            df = df.drop('dir0', axis=1)

        return df

    @property
    def df(self):
        if isinstance(self._df, Future):
            return self._df.result()
        else:
            return self._df

    def get_columns(self, columns, query=None, add_flags=False, client=None):
        
        cols_to_get = list(columns)
        return self._read_data(cols_to_get, query=query, add_flags=add_flags, client=client)
