import os
import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask.array as da
# from distributed import Future
import fastparquet
import glob, re
import random
import logging
import hashlib
from functools import reduce
from operator import add, mul
import string

from .match import match_lists
from .functors import Labeller, CompositeFunctor, RAColumn, DecColumn
from .utils import result

# try:
#     from lsst.pipe.analysis.utils import Filenamer
# except ImportError:
#     logging.warning('Pipe analysis not available.  ButlerCatalog will not work.')

class Catalog(object):
    """Base class for columnwise interface to afwTable

    The idea behind this is to allow for access to only specified 
    columns of one or more `lsst.afw.Table` objects, without necessarily
    having to load the entire table(s) into memory.

    Subclasses must implement at least `__init__`, `get_columns`,
    and `_stringify`, and probably also `_initialize` and `_apply_func`.

    """
    index_column = 'id'

    def _initialize(self):
        """Set lots of properties that will be lazily calculated 
        """
        self._coords = None
        self._md5 = None

    def _stringify(self):
        """Return string representing catalog, for md5 hashing
        """
        raise NotImplementedError

    def _compute_md5(self):
        return hashlib.md5(self._stringify())

    @property
    def md5(self):
        """Hash of the catalog

        Computed from an md5 hash of `._stringify`.
        """
        if self._md5 is None:
            self._md5 = self._compute_md5().hexdigest()
        return self._md5

    def __hash__(self):
        return hash(self.md5)

    def get_columns(self, columns, **kwargs):
        """Returns dataframe of desired columns

        Parameters
        ----------
        columns : list
            List of column names to be returned.
        """
        raise NotImplementedError('Must implement get_columns!')

    def _get_coords(self):
        df = self.get_columns(['coord_ra', 'coord_dec'], add_flags=False)

        # Hack to avoid phantom 'dir0' column 
        df = result(df)
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
        """Defines how to compute the output of a functor

        This method partially defines what happens when you call
        a functor with a catalog; the full behavior is a combination
        of this method and the `__call__` method of the functor.

        Parameters
        ----------
        func : explorer.functors.Functor
            Functor to be calculated.

        See also
        --------
        explorer.functors.Functor
        """
        df = self.get_columns(func.columns, query=query, client=client)
        if len(df.columns)==0:
            vals = pd.Series(np.nan, index=df.index)
        else:
            vals = func._func(df)
        return vals

class MatchedCatalog(Catalog):
    """Interface to two afwTables at a time.

    Matches sources from two catalogs with KDTree, 
    within `match_radius`.  If you provide a `match_registry`
    filename, then the match data will be persisted (keyed by 
    the hash of the catalog), for fast loading in the future.  

    Parameters
    ----------
    cat1, cat2 : `Catalog` objects
        Catalogs to match.

    match_radius : float
        Maximum radius to match sources

    match_registry : str
        HDF file containing persisted match data.
    """

    def __init__(self, cat1, cat2, match_radius=0.5, 
                    match_registry=None):
        self.cat1 = cat1
        self.cat2 = cat2

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

    def _get_coords(self):
        self._coords = self.cat1.coords

    def match(self, **kwargs):
        """Run the catalog matching.
        """
        return self._match_cats(**kwargs)

    def _read_registry(self):
        """Load persisted match data

        Returns
        -------
        inds1, inds2, dist: pandas.Int64Index objects, pandas.Series
            Matched indices and match_distance data.

        """
        if self.match_registry is None:
            raise ValueError
        store = pd.HDFStore(self.match_registry)
        df = store['md5_{}'.format(self.md5)]
        store.close()
        inds1 = df.index
        inds2 = pd.Int64Index(df['id2'], name='id')
        dist = df['distance'].rename('match_distance')


        return inds1, inds2, dist   

    def _write_registry(self, match_df):
        """Write match data to registry

        No-op if `self.match_registry` is not set.

        Parameters
        ----------
        match_df : pandas.DataFrame
            Match data.  Index of DataFrame corrdesponds to index values
            of `self.cat1`; `id2` column is index values of `self.cat2`;
            `distance` column is the match distance.
        """
        if self.match_registry is None:
            return
        else:
            match_df.to_hdf(self.match_registry, 'md5_{}'.format(self.md5))

    def _test_registry(self):
        """Test to make sure match loaded from registry is same as fresh-calculated
        """
        id1, id2, dist = self._read_registry()

        self.match(recalc=True)
        assert (id1==self._match_inds1).all()
        assert (id2==self._match_inds2).all()
        assert (dist==self._match_distance).all()

    def _match_cats(self, recalc=False):
        """Determine which indices in cat2 correspond to the same objects in cat1

        Computes match using `explorer.match.match_lists`, which uses a KDTree-based 
        algorithm.  If `self.match_registry` is defined but the match hasn't been 
        computed before, then the results are written to that file.  If the match has been
        computed and persisted, then it is just loaded.

        Match information is stored in the form of `pandas.Index` objects: `match_inds1` 
        and `match_inds2`, which are *label* indices, not positional.  Note that 
        the `get_columns` method for this object does not return row-matched columns; 
        in order to get properly row-matched columns from the two catalogs, you need to index
        the outputs with `match_inds1` and `match_inds2`, e.g.,

            catalog = MatchedCatalog(cat1, cat2)
            df1, df2 =  catalog.get_columns([cols])
            df1 = df1.loc[catalog.match_inds1]
            df2 = df2.loc[catalog.match_inds2]

        Now, the rows of `df1` and `df2` can be compared as belonging to the "same" (matched)
        objects.  This behavior is implemented in `_apply_func`.

        Parameters
        ----------
        recalc : bool
            If False, then this will attempt to read from the `match_registry` file.
            If True, then even if `match_registry` is defined, the match will be recomputed
        """
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
            d = pd.Series(dist[good] * 3600, index=id1[good], name='match_distance')

            match_df = pd.DataFrame({'id2':i2, 'distance':d}, index=i1)
            self._write_registry(match_df)

        self._match_inds1 = i1
        self._match_inds2 = i2
        self._match_distance = d

        self._matched = True

    @property
    def match_distance(self):
        """Distance between objects identified as matches
        """
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
        """Retrieve columns from both catalogs, without matching
        """
        df1 = self.cat1.get_columns(*args, **kwargs)
        df2 = self.cat2.get_columns(*args, **kwargs)
        # df2.set_index(dd.Series(df1.index))

        return df1, df2


    def _apply_func(self, func, query=None, how='difference', client=None):
        """Get the results of applying a functor

        Returns row-matched computation on a catalog.

        Parameters
        ----------
        func : explorer.functors.Functor
            Functor to be calculated.

        query : str
            [Queries not currently completely or consistently implemented.]

        how : str
            Allowed values:
            * 'difference' (default): returns difference of matched computed values
            * 'sum': returns sum of matched computed values
            * 'first': returns computed values from `self.cat1`
            * 'second': returns computed values from `self.cat2`
            * 'all': returns computed values from both catalogs. 
        """
        df1, df2 = self.get_columns(func.columns, query=query, client=client)

        # Check if either returned empty dataframe
        df1_empty = len(df1.columns)==0
        df2_empty = len(df2.columns)==0

        if func.allow_difference or how in ['all', 'second']:
            id1, id2 = self.match_inds
            if df1_empty:
                v1 = pd.Series(np.nan, index=id1)
            else:
                v1 = result(func._func(df1)).loc[id1].values
    
            if df2_empty:
                v2 = pd.Series(np.nan, index=id1)
            else:
                v2 = result(func._func(df2)).loc[id2].values

            if how=='difference':
                vals = pd.Series(v1 - v2, index=id1)
            elif how=='sum':
                vals = pd.Series(v1 + v2, index=id1)
            elif how=='second':
                vals = pd.Series(v2, index=id1)
            elif how=='first':
                vals = pd.Series(v1, index=id1)
            elif how=='all':
                vals = pd.concat([pd.Series(v1, name=self.cat1.name, index=id1), 
                                  pd.Series(v2, name=self.cat2.name, index=id1)], axis=1)
                # raise NotImplementedError
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
    """Catalog to organize matching multiple visit catalogs to a coadd catalog

    This is the object that can be used to compute quantities like photometric
    or astrometric reproducibility.

    Parameters
    ----------
    coadd_cat : Catalog 
        Coadd `Catalog` object.

    visit_cats : list
        List of visit `Catalog` objects.
    """
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
        self.subcats = self._make_subcats(**kwargs)

        self._initialize()

    def _make_subcats(self, **kwargs):
        """Create MatchedCatalog for each coadd-visit pair
        """
        return [MatchedCatalog(self.coadd_cat, v, **kwargs)
                    for v in self.visit_cats]

    def _stringify(self):
        s = self.coadd_cat._stringify()
        for c in self.visit_cats:
            s += c._stringify()
        return s

    @property
    def visit_names(self):
        return [c.name for c in self.visit_cats]

    @property
    def names(self):
        return self.visit_names

    def _initialize(self):
        [c._initialize() for c in self.visit_cats]
        [c._initialize() for c in self.subcats]
        self._match_distance = None
        self._md5 = None
        self._matched = False
        self._coords = None


    def _test_registry(self):
        [c._test_registry() for c in self.subcats]

    @property
    def cat1(self):
        return self.coadd_cat

    @property
    def columns(self):
        return self.coadd_cat.columns

    def match(self, raise_exceptions=False, **kwargs):
        """Match each of the visit-coadd MatchedCatalogs

        Parameters
        ----------
        raise_exceptions : bool
            If False, then just print warning message if any of the 
            matching operations raises an exception, but keep going.
        """
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

        Note (as in `MatchedCatalog`) that the output of 
        `.get_columns` is not row-matched. 
    
        Returns
        -------
        df1 : pandas.DataFrame
            Columns from coadd catalog.

        visit_cols : tuple
            Tuple of DataFrames from each of the visit catalogs.
        """
        df1 = self.coadd_cat.get_columns(*args, **kwargs)
        return df1, tuple(c.get_columns(*args, **kwargs) for c in self.visit_cats)

    def _apply_func(self, func, query=None, how='all', client=None):
        """Get the results of applying a functor

        Returns row-matched computations on catalog(s).

        Parameters
        ----------
        func : explorer.functors.Functor
            Functor to be calculated.

        query : str
            [Queries not currently completely or consistently implemented.]

        how : str
            Allowed values:
            * 'all' (default): returns computed values for coadd and all visit cats
            * 'coadd': returns computed values only from coadd catalog
            * 'stats': returns mean, std, and count of values computed from all catalogs
            * 'mean': returns mean of values computed from all catalogs
            * 'std': returns standard deviation of values computed from all catalogs. 

        client : distributed.Client (optional)
            If a client is provided, then the computations will be distributed
            over the visit catalogs using `client.map`.
        """
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
            aligned_vals = [result(v) for v in aligned_vals]
        else:
            visit_vals = [func(c, query=query, how='second', client=client) for c in self.subcats]
            aligned_vals = [coadd_vals.align(v)[1] for v in visit_vals]
        val_df = pd.concat([coadd_vals] + aligned_vals, axis=1, keys=['coadd'] + self.visit_names)
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

    def _compute_match_distance(self):
        coadd = pd.Series(index=self.coadd_cat.index)
        aligned_dists = [coadd.align(c.match_distance)[1] for c in self.subcats]
        df = pd.concat(aligned_dists, axis=1, 
                        keys=[('match_distance', n) for n in self.visit_names])
        df[('match_distance', 'coadd')] = 0.
        return df

    @property
    def match_distance(self):
        if self._match_distance is None:
            self._match_distance = self._compute_match_distance()
        return self._match_distance

    @property
    def n_visits(self):
        """Returns number of visit catalogs successfully matched to coadd
        """
        return self.match_distance.count(axis=1).rename('n_visits') - 1

    def _get_coords(self):
        coords_func = CompositeFunctor({'ra':RAColumn(), 'dec':DecColumn()})
        self._coords = coords_func(self, how='all', calculate=True)

class ParquetCatalog(Catalog):
    """Out-of-memory column-store interface to afwTable using parquet

    This `Catalog` implementation uses dask and parquet to allow for
    out-of-memory catalog access, and selective retrieval of column data.  
    It expects to read parquet tables written by the `lsst.pipe.analysis` scripts.

    Parameters
    ----------
    filenames : str or list or tuple
        Either a single parquet file path or a list (or tuple) of filenames
        to be simultaneously loaded.  (If multiple filenames provided, the
        catalogs should share all the same columns---though I don't 
        believe this is explicitly checked).
        Note that this will be saved as a sorted list of absolute paths 
        (for hash consistency), regardless of what is passed.

    name : str or int, optional
        Name of catalog.  If name is not provided, then a six-character name 
        will be generated, using the catalog hash as a random seed, thus
        ensuring consistency of the "default" name.
    """
    def __init__(self, filenames, name=None):
        self.filenames = filenames if type(filenames) in [list, tuple] else [filenames]

        # Ensure sorted list for hash consistency
        if len(self.filenames) > 1:
            self.filenames = list(set([os.path.abspath(f) for f in filenames]))
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
            random.seed(self.md5)
            self._name = ''.join(random.choices(string.ascii_lowercase, k=6))
        return self._name

    @name.setter
    def name(self, new):
        self._name = new

    @property
    def columns(self):
        if self._columns is None:
            self._columns = list(dd.read_parquet(self.filenames[0]).columns)
        return self._columns

    @property
    def flags(self):
        """Names of all columns that have boolean type
        """
        if self._flags is None:
            self._flags = list(dd.read_parquet(self.filenames[0]).select_dtypes(include=['bool']).columns)
        return self._flags

    def get_flags(self, flags=None):
        flags = self.flags if flags is None else flags

        return self.get_columns(flags)

    def _read_data(self, columns, query=None, add_flags=False, client=None):
        """Reads parquet data into dask DataFrame
        """
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
        return result(self._df)

    def get_columns(self, columns, query=None, add_flags=False, client=None):
        """Get desired columns as dask dataframe

        Parameters
        ----------
        columns : list
            List of names of desired columns.  Any names not in catalog will be ignored.

        query : str
            [Queries not really consistently implemented yet; do not use.]

        add_flags : bool
            If True, then include all flags as well as explictly requested columns

        client : distributed.Client, optional
            If client is provided, that will be what is used to read the parquet tables.
            To be honest, I'm not yet sure if/when this is ever an advantage.

        Returns
        -------
        df : dask.DataFrame
        """
        # Drop unwanted columns
        cols_to_get = [c for c in columns if c in self.columns]
        return self._read_data(cols_to_get, query=query, add_flags=add_flags, client=client)


class IDMatchedCatalog(MultiMatchedCatalog):    
    """A multi-matched-catalog that matches by ID rather than position

    This is adapted from `MultiMatchedCatalog`, but cleaned of "coadd" and
    "visit" references, which are no longer relevant in this context.  
    Matching is by ID rather than position.

    Parameters
    ----------
    cats : list
        List of `Catalog` objects to match.

    merge_method : 'intersection' or 'union'
        Operation to determine final set of indices for matched catalog.
    """
    def __init__(self, cats, merge_method='intersection'):
        self.cats = cats
        if merge_method not in ['union', 'intersection']:
            raise ValueError('merge_method must be either "union" or "intersection"')

        self.merge_method = merge_method
        self._initialize()

    def _initialize(self):
        [c._initialize() for c in self.cats]
        self._index = None
        self._matched = False
        

    @property
    def coords(self):
        """All coords should be the same, so just return coords of first

        TODO: check that this is not a bug, as this is not matched in any way...
        Maybe it should be `self.cats[0].coords.loc[self._index]`?
        """
        return self.cats[0].coords
        
    @property
    def coadd_cat(self):
        return self.cats[0]
        
    @property
    def names(self):
        return [c.name for c in self.cats]
        
    @property
    def index(self):
        if self._index is None:
            self.match()
        return self._index
        
    def match(self, **kwargs):
        """Match catalogs by creating a joint index (either union or intersection of all) 
        """
        if self._index is None:
            if self.merge_method == 'union':
                self._index = reduce(lambda i1,i2 : i1.union(i2), [c.index for c in self.cats])
            elif self.merge_method == 'intersection':
                self._index = reduce(lambda i1,i2 : i1.intersection(i2), [c.index for c in self.cats])
            self._matched = True
            
    def get_columns(self, *args, **kwargs):
        """Return tuple of dataframes, one for each catalog
        """
        return tuple([c.get_columns(*args, **kwargs) for c in self.cats])
    
    def _apply_func(self, func, query=None, how='all', client=None):
        """Get the results of applying a functor

        Returns row-matched computations on catalogs.

        Parameters
        ----------
        func : explorer.functors.Functor
            Functor to be calculated.

        query : str
            [Queries not currently completely or consistently implemented.]

        how : str
            Allowed values:
            * 'all' (default): returns computed values for coadd and all visit cats
            * 'stats': returns mean, std, and count of values computed from all catalogs
            * 'mean': returns mean of values computed from all catalogs
            * 'std': returns standard deviation of values computed from all catalogs. 

        client : distributed.Client (optional)
            If a client is provided, then the computations will be distributed
            over the visit catalogs using `client.map`.        
        """
        if client and not self._matched:
            self.match()
    
        if client:
            func_worker = FuncWorker(func, query=query)
            vals = client.map(func_worker, self.cats)
            aligned_vals = [v.result().loc[self.index] for v in vals]            
        else:
            vals = [func(c, query=query) for c in self.cats]
            aligned_vals = [v.loc[self.index] for v in vals]            
        
        val_df = pd.concat(aligned_vals, axis=1, keys=self.names)
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



class MultiBandCatalog(IDMatchedCatalog):
    """Catalog to organize coadd catalogs from multiple bands

    Useful for doing color-color exploration.

    Parameters
    ----------
    catalog_dict : dict
        Dictionary of catalogs, keyed by filter name.

    short_filters : str or list
        Short filter names, in wavelength order, e.g., 'GRIZY'

    reference_filt : str
        Filter to use as the "reference" filter.  This becomes relevant
        when using a `MultiBandCatalog` as the catalog for a `QADataset`, 
        as the `.color_ds` attribute takes the values of `x` and the functor
        calculations from reference band.  This is not really used for anything
        yet, since the main reason to use a `MultiBandCatalog` is to investigate
        colors, which don't need a reference filter.

    """
    filter_order = {'HSC-G':0, 'HSC-R':1, 'HSC-I':2, 'HSC-Z':3, 'HSC-Y':4}

    def __init__(self, catalog_dict, short_filters=None, reference_filt='HSC-I', **kwargs):
        self.catalog_dict = catalog_dict
        self.short_filters = short_filters
        self.reference_filt = reference_filt

        # Make sure filters are in sorted order
        self.filters = list(self.catalog_dict.keys())
        self.filters.sort(key=lambda f:self.filter_order[f])

        cats = []
        for filt in self.filters:
            self.catalog_dict[filt].name = filt
            cats.append(self.catalog_dict[filt])

        super(MultiBandCatalog, self).__init__(cats, **kwargs)
        
    @property
    def n_filters(self):
        return len(self.filters)

    @property
    def colors(self):
        """All "adjacent" colors for filter set

        E.g., for `GRIZ`, this returns `['G_R', 'R_I', 'I_Z']`
        """
        return ['{}_{}'.format(self.short_filters[i], self.short_filters[i+1])
                    for i in range(self.n_filters - 1)]

    @property
    def color_colors(self):
        """All adjacent three-color groups for filter set

        For `GRIZ`, this returns `['GRI', 'RIZ']`
        """
        return ['{}{}{}'.format(c1[0],c1[-1],c2[-1]) 
                for c1, c2 in zip(self.colors[0:-1], self.colors[1:])]

    @property
    def color_groups(self):
        """Groups of adjacent colors for color-color plotting purposes

        For `self.filters=['HSC-G', 'HSC-R', 'HSC-I', 'HSC-Z']`, this returns
        `[('HSC-G', 'HSC-R'), ('HSC-R', 'HSC-I']), ('HSC-I', 'HSC-Z')]`
        """
        return [(self.filters[i], self.filters[i+1]) 
                    for i in range(self.n_filters - 1)]


class ButlerCatalog(ParquetCatalog):
    """Base class for getting QA catalog from dataId(s)

    This currently depends on the butler-filename-hacking done in 
    `lsst.pipe.analysis` to retrieve filenames.  

    Not to be implemented on its own; use subclasses for which 
    `_dataset_name` and `_default_description` are defined, 
    such as `CoaddCatalog`, `VisitCatalog`, or `ColorCatalog`.
    
    Parameters
    ----------
    butler : Butler object
        Data repository from which to get table(s).  The dataset
        requested

    dataIdList : dict, or list of dicts
        One or more dataIds for the requested catalogs.

    description : str (optional)
        Description for butler retrieval.  If not provided, `self._default_description`
        will be used.
    """

    _dataset_name = None # must define for subclasses
    _default_description = None
    def __init__(self, butler, dataIdList, description=None, **kwargs):
        # self.butler = butler
        if type(dataIdList) not in [list, tuple]:
            dataIdList = [dataIdList]
        self.dataIdList = dataIdList
        if description is None:
            description = self._default_description
        self.description = description
        
        filenames = []
        for dataId in dataIdList:
            tableFilenamer = Filenamer(butler, self._dataset_name, dataId)
            filenames.append(tableFilenamer(dataId, description=description))
        super(ButlerCatalog, self).__init__(filenames, **kwargs)

class CoaddCatalog(ButlerCatalog):
    """Coadd QA table dataset retrieval given dataId(s)
    """
    _dataset_name = 'qaTableCoadd'
    _default_description = 'forced'
    
class VisitCatalog(ButlerCatalog):
    """Visit QA table dataset retrieval given dataId(s)
    """
    _dataset_name = 'qaTableVisit'
    _default_description = 'catalog'
    
class ColorCatalog(ButlerCatalog):
    """Color QA table dataset retrieval given dataId(s)
    """
    _dataset_name = 'qaTableColor'
    _default_description = 'forced'

