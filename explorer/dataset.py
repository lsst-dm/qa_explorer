import numpy as np
import pandas as pd
import holoviews as hv
from functools import partial
import pickle
import tempfile
import os, shutil
import fastparquet
import dask.dataframe as dd

from .functors import Functor, CompositeFunctor, Column, RAColumn, DecColumn, Mag
from .functors import StarGalaxyLabeller
from .catalog import MatchedCatalog, MultiMatchedCatalog, IDMatchedCatalog, MultiBandCatalog
from .plots import filter_dset

class QADataset(object):
    def __init__(self, catalog, funcs, flags=None, 
                 xFunc=Mag('base_PsfFlux', allow_difference=False), 
                 labeller=StarGalaxyLabeller(),
                 query=None, client=None,
                 cachedir=None, oom=False):

        self._set_catalog(catalog)
        self._set_funcs(funcs, xFunc, labeller)
        self._set_flags(flags)

        self.client = client
        self._query = query

        if cachedir is None:
            cachedir = tempfile.gettempdir()
        self._cachedir = cachedir
        self._df_file = None

        self.oom = oom

    def save(self, filename, protocol=4):
        pickle.dump(self, open(filename, 'wb'), protocol=protocol)

    @classmethod
    def load(cls, filename, client=None):
        new = pickle.load(open(filename, 'rb'))
        new.client = client
        return new

    def __getstate__(self):
        odict = self.__dict__
        client = self.client
        odict['client'] = None
        return odict

    def __setstate__(self, d):
        self.__dict__ = d

    def __del__(self):
        if self._df_computed and self.oom:
            os.remove(self.df_file)

    def _set_catalog(self, catalog):
        self.catalog = catalog
        self._reset()

    def _set_funcs(self, funcs, xFunc, labeller):
        if isinstance(funcs, list) or isinstance(funcs, tuple):
            self.funcs = {'y{}'.format(i):f for i,f in enumerate(funcs)}
        elif isinstance(funcs, Functor):
            self.funcs = {'y0':funcs}
        else:
            self.funcs = funcs

        self.xFunc = xFunc
        self.labeller = labeller
        self._reset()

    def _set_flags(self, flags):
        if flags is None:
            self.flags = []
        else:
            self.flags = flags # TODO: check to make sure flags are valid                    
        self._reset()

    def _reset(self):
        self._df_computed = False
        self._ds = None

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, new):
        self._query = new
        self._reset()

    @property
    def allfuncs(self):
        allfuncs = self.funcs.copy()

        # Set coordinates and x value
        allfuncs.update({'ra':RAColumn(), 'dec': DecColumn(), 
                         'x':self.xFunc})
        if self.id_name is not None:
            allfuncs.update({self.id_name : Column(self.id_name)})

        # Include flags
        allfuncs.update({f:Column(f) for f in self.flags})

        if self.labeller is not None:
            allfuncs.update({'label':self.labeller})

        return allfuncs        

    @property
    def df(self):
        if not self._df_computed:
            self._make_df()
        return self._df
        # Save below for when trying to do more out-of-memory
        # df = pd.read_hdf(self.df_file, 'df')
        df = pd.read_parquet(self.df_file) # wait for pandas 0.22
        # df = dd.read_parquet(self.df_file)
        return df

    @property
    def is_matched(self):
        return isinstance(self.catalog, MatchedCatalog)

    @property
    def is_multi_matched(self):
        return isinstance(self.catalog, MultiMatchedCatalog)

    @property 
    def is_idmatched(self):
        return isinstance(self.catalog, IDMatchedCatalog)

    @property
    def is_multiband(self):
        return isinstance(self.catalog, MultiBandCatalog)

    @property
    def id_name(self):
        if self.is_idmatched:
            name = 'patchId'
        elif self.is_multi_matched:
            name = 'ccdId'
        elif self.is_matched and not self.is_multi_matched:
            if 'ccdId' in self.catalog.cat1.columns:
                name = 'ccdId'
            elif 'patchId' in self.catalog.cat1.columns:
                name = 'patchId'
            else:
                logging.warning('No id name available (looked for ccdId, patchId)?')
                name = None
        elif 'ccdId' in self.catalog.columns:
            name = 'ccdId'
        elif 'patchId' in self.catalog.columns:
            name = 'patchId'
        else:
            logging.warning('No id name available (looked for ccdId, patchId)?')
            name = None

        return name

    @property
    def df_file(self):
        if self._df_file is None:
            self._df_file = os.path.join(self._cachedir, next(tempfile._get_candidate_names()))
        return self._df_file

    def _make_df(self, **kwargs):
        f = CompositeFunctor(self.allfuncs)
        if self.is_multi_matched:
            kwargs.update(how='all')
        df = f(self.catalog, query=self.query, client=self.client, dropna=False, **kwargs)
        if self.is_matched and not self.is_idmatched:
            df = pd.concat([df, self.catalog.match_distance.dropna(how='all')], axis=1)
        if not self.is_matched: 
            df = df.dropna(how='any')
        df = df.replace([-np.inf, np.inf], np.nan)

        # Add color columns if catalog is a MultiBandCatalog 
        if self.is_multiband:
            cat = self.catalog
            color_dfs = []
            filters = cat.filters
            n_filts = len(filters)
            cols_to_difference = [(cat.filters[i], cat.filters[i+1]) 
                                  for i in range(n_filts - 1)]
            for name, fn in self.funcs.items():
                if isinstance(fn, Mag):
                    col_names = [('{}_color'.format(name), '{}-{}'.format(cat.short_filters[i], cat.short_filters[i+1])) for i in range(n_filts - 1)]
                    mags = df[name]
                    color_df = pd.DataFrame({c : mags[c1] - mags[c2] for c, (c1, c2) in zip(col_names, cols_to_difference)})
                    color_df.dropna(how='any', inplace=True)
                    df = pd.concat([df, color_df], axis=1)

        if self.oom:
            # df.to_hdf(self.df_file, 'df') #must be format='table' if categoricals included
            df.to_parquet(self.df_file) # wait for pandas 0.22
            # fastparquet.write(self.df_file, df) # Doesn't work with multiindexing

        self._df_computed = True

        self._df = df  

    @property
    def ds(self):
        if self._ds is None:
            self._make_ds()
        return self._ds

    def get_ds(self, key):
        if self._ds is None:
            self._make_ds()
        return self._ds_dict[key]

    def _get_kdims(self):
        kdims = ['ra', 'dec', hv.Dimension('x', label=self.xFunc.name), 'label']
        if self.id_name is not None:
            kdims.append(self.id_name)
        kdims += self.flags
        return kdims        

    def _make_ds(self, **kwargs):
        kdims = self._get_kdims()
        vdims = []
        for k,v in self.allfuncs.items():
            if k in ('ra', 'dec', 'x', 'label', self.id_name) or k in self.flags:
                continue
            label = v.name
            if v.allow_difference:
                if self.is_multi_matched:
                    label = 'std({})'.format(label)
                elif self.is_matched:
                    label = 'diff({})'.format(label)
            vdims.append(hv.Dimension(k, label=label))

        if self.is_matched and not self.is_idmatched:
            vdims += [hv.Dimension('match_distance', label='Match Distance [arcsec]')]

        if self.is_multi_matched:

            # reduce df appropriately here
            coadd_cols = ['ra', 'dec', 'x', 'label'] + self.flags
            visit_cols = list(set(self.df.columns.levels[0]) - set(coadd_cols))

            df_swap = self.df.swaplevel(axis=1)
            coadd_df = df_swap.loc[:, 'coadd'][coadd_cols]
            visit_df = self.df[visit_cols].drop('coadd', axis=1, level=1)
            dfs = [coadd_df, visit_df.std(axis=1, level=0).dropna(how='any')]

            # This dropna thing is a problem when there are NaNs in flags.
            #  Solution: use subset=[...] to define the subset of columns to look for NaNs
            subset_to_check = [c for c in df_swap['coadd'].columns if c not in [self.id_name] + self.flags]
            df_dict = {k:df_swap[k].dropna(how='any', subset=subset_to_check).reset_index() 
                            for k in ['coadd'] + self.catalog.visit_names}
            self._ds_dict = {k:hv.Dataset(df_dict[k], kdims=kdims, vdims=vdims) for k in df_dict}

            # Keep only rows that aren't nan in visit values
            df = pd.concat(dfs, axis=1, join='inner')
        else:
            df = self.df.dropna(how='any')

        ds = hv.Dataset(df.reset_index(), kdims=kdims, vdims=vdims)
        self._ds = ds        

    def color_ds(self, mag):
        if not self.is_multiband:
            return NotImplementedError('Can only get color_ds if catalog is a MultiBandCatalog')
        if not isinstance(self.allfuncs[mag], Mag):
            raise ValueError('Requested column must be a magnitude: {} requested'.format(mag))

        filt = self.catalog.filters[0]
        swap_df = self.df.swaplevel(axis=1)

        color_df = pd.concat([self.df[['ra', 'dec']], 
                              swap_df[filt], 
                              self.df['psf_color']], axis=1)
        color_df = color_df.rename(columns={('ra', 'ra'):'ra', ('dec', 'dec'): 'dec'})    

        return hv.Dataset(color_df, kdims=self._get_kdims())


    def visit_points(self, vdim, visit, x_max, label,
                     filter_range=None, flags=None, bad_flags=None):

        # Hack to deal with integer visit values, if they are actually strings

        if self.is_multi_matched:
            try:
                dset = self.get_ds(visit)
            except KeyError: 
                dset = self.get_ds(str(visit))
        else:
            if visit != 'coadd':
                raise ValueError('visit name must be "coadd"!')
            dset = self.ds

        dset = dset.select(x=(None, x_max), label=label)
        # filter_range = {} if filter_range is None else filter_range
        # flags = [] if flags is None else flags
        # bad_flags = [] if bad_flags is None else bad_flags
        dset = filter_dset(dset, filter_range=filter_range, flags=flags, bad_flags=bad_flags)
        # dset = dset.redim(**{vdim:'y'})
        vdims = [vdim, 'id', 'x']
        if self.id_name is not None:
            vdims.append(self.id_name)
        pts = hv.Points(dset, kdims=['ra', 'dec'], vdims=vdims)
        return pts.opts(plot={'color_index':vdim})

    def visit_explore(self, vdim, x_range=np.arange(15,24.1,0.5), filter_stream=None,
                      range_override=None):
        fn = partial(QADataset.visit_points, self=self, vdim=vdim)
        dmap = hv.DynamicMap(fn, kdims=['visit', 'x_max', 'label'],
                             streams=[filter_stream])

        y_min = self.df[vdim].drop('coadd', axis=1).quantile(0.005).min()
        y_max = self.df[vdim].drop('coadd', axis=1).quantile(0.995).max()

        ra_min, ra_max = self.catalog.coadd_cat.ra.quantile([0, 1])
        dec_min, dec_max = self.catalog.coadd_cat.dec.quantile([0, 1])

        ranges = {vdim : (y_min, y_max),
                  'ra' : (ra_min, ra_max),
                  'dec' : (dec_min, dec_max)}
        if range_override is not None:
            ranges.update(range_override)

        # Force visit names to be integers, if possible
        try:
            visit_names = [int(v) for v in self.catalog.visit_names]
            visit_names.sort()
        except:
            visit_names = self.catalog.visit_names

        dmap = dmap.redim.values(visit=visit_names, 
                                 # vdim=list(self.funcs.keys()) + ['match_distance'], 
                                 # vdim=[vdim], 
                                 label=['galaxy', 'star'],
                                 x_max=x_range).redim.range(**ranges)
        return dmap

    def coadd_points(self, vdim, x_max, label, **kwargs):
        return self.visit_points(vdim, 'coadd', x_max, label, **kwargs)

    def coadd_explore(self, vdim, x_range=np.arange(15,24.1,0.5), filter_stream=None,
                        range_override=None):
        fn = partial(QADataset.coadd_points, self=self, vdim=vdim)
        dmap = hv.DynamicMap(fn, kdims=['x_max', 'label'],
                             streams=[filter_stream])

        if self.is_multi_matched:
            y_min = self.df[(vdim, 'coadd')].quantile(0.005)
            y_max = self.df[(vdim, 'coadd')].quantile(0.995)
            ra_min, ra_max = self.catalog.coadd_cat.ra.quantile([0, 1])
            dec_min, dec_max = self.catalog.coadd_cat.dec.quantile([0, 1])
        else:
            y_min = self.df[vdim].quantile(0.005)
            y_max = self.df[vdim].quantile(0.995)
            ra_min, ra_max = self.catalog.ra.quantile([0, 1])
            dec_min, dec_max = self.catalog.dec.quantile([0, 1])


        ranges = {vdim : (y_min, y_max),
                  'ra' : (ra_min, ra_max),
                  'dec' : (dec_min, dec_max)}
        if range_override is not None:
            ranges.update(range_override)

        # Force visit names to be integers, if possible

        dmap = dmap.redim.values(label=['galaxy', 'star'],
                                 x_max=x_range).redim.range(**ranges)
        return dmap


