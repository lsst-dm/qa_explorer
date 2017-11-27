import numpy as np
import pandas as pd
import holoviews as hv
from functools import partial

from .functors import Functor, CompositeFunctor, Column, RAColumn, DecColumn, Mag
from .functors import StarGalaxyLabeller
from .catalog import MatchedCatalog, MultiMatchedCatalog
from .plots import filter_dset

class QADataset(object):
    def __init__(self, catalog, funcs, flags=None, 
                 xFunc=Mag('base_PsfFlux', allow_difference=False), 
                 labeller=StarGalaxyLabeller(),
                 query=None, client=None):

        self._set_catalog(catalog)
        self._set_funcs(funcs, xFunc, labeller)
        self._set_flags(flags)

        self.client = client
        self._query = query

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
        self._df = None
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
                         'x':self.xFunc, 'ccdId':Column('ccdId')})

        # Include flags
        allfuncs.update({f:Column(f) for f in self.flags})

        if self.labeller is not None:
            allfuncs.update({'label':self.labeller})

        return allfuncs        

    @property
    def df(self):
        if self._df is None:
            self._make_df()
        return self._df

    @property
    def is_matched(self):
        return isinstance(self.catalog, MatchedCatalog)

    @property
    def is_multi_matched(self):
        return isinstance(self.catalog, MultiMatchedCatalog)

    def _make_df(self, **kwargs):
        f = CompositeFunctor(self.allfuncs)
        if self.is_multi_matched:
            kwargs.update(how='all')
        df = f(self.catalog, query=self.query, client=self.client, dropna=False, **kwargs)
        if self.is_matched:
            df = pd.concat([df, self.catalog.match_distance.dropna(how='all')], axis=1)
        if not self.is_matched: 
            df = df.dropna(how='any')
        df = df.replace([-np.inf, np.inf], np.nan)

        # ids = df.index
        # if self.is_matched:
        #     flags, _ = self.catalog.get_columns(self.flags)
        # else:
        #     flags = self.catalog.get_columns(self.flags)
        # flags = flags.compute().loc[ids]
        # df = df.join(flags)
        self._df = df        

    @property
    def ds(self):
        if self._ds is None:
            self._make_ds()
        return self._ds

    def get_ds(self, key):
        return self._ds_dict[key]

    def _make_ds(self, **kwargs):
        kdims = ['ra', 'dec', hv.Dimension('x', label=self.xFunc.name), 'label', 'ccdId']
        kdims += self.flags
        vdims = []
        for k,v in self.allfuncs.items():
            if k in ('ra', 'dec', 'x', 'label') or k in self.flags:
                continue
            label = v.name
            if v.allow_difference:
                if self.is_multi_matched:
                    label = 'std({})'.format(label)
                elif self.is_matched:
                    label = 'diff({})'.format(label)
            vdims.append(hv.Dimension(k, label=label))

        if self.is_matched:
            vdims += [hv.Dimension('match_distance', label='Match Distance [arcsec]')]

        if self.is_multi_matched:
            # reduce df appropriately here
            coadd_cols = ['ra', 'dec', 'x', 'label'] + self.flags
            visit_cols = list(set(self.df.columns.levels[0]) - set(coadd_cols))

            df_swap = self.df.swaplevel(axis=1)
            coadd_df = df_swap.loc[:, 'coadd'][coadd_cols]
            visit_df = self.df[visit_cols].drop('coadd', axis=1, level=1)
            dfs = dfs = [coadd_df, visit_df.std(axis=1, level=0).dropna(how='any')]

            df_dict = {k:df_swap[k].dropna(how='any').reset_index() 
                            for k in ['coadd'] + self.catalog.visit_names}
            self._ds_dict = {k:hv.Dataset(df_dict[k], kdims=kdims, vdims=vdims) for k in df_dict}

            # Keep only rows that aren't nan in visit values
            df = pd.concat(dfs, axis=1, join='inner')
        else:
            df = self.df.dropna(how='any')

        ds = hv.Dataset(df.reset_index(), kdims=kdims, vdims=vdims)
        self._ds = ds        

    def visit_points(self, vdim, visit, x_max, label,
                     filter_range=None, flags=None, bad_flags=None):

        dset = self.get_ds(visit).select(x=(None, x_max), label=label)
        # filter_range = {} if filter_range is None else filter_range
        # flags = [] if flags is None else flags
        # bad_flags = [] if bad_flags is None else bad_flags
        dset = filter_dset(dset, filter_range=filter_range, flags=flags, bad_flags=bad_flags)
        # dset = dset.redim(**{vdim:'y'})
        pts = hv.Points(dset, kdims=['ra', 'dec'], vdims=[vdim, 'id', 'ccdId'])
        return pts.opts(plot={'color_index':vdim})

    def visit_explore(self, vdim, x_range=np.arange(15,24.1,0.5), filter_stream=None):
        fn = partial(QADataset.visit_points, self=self, vdim=vdim)
        dmap = hv.DynamicMap(fn, kdims=['visit', 'x_max', 'label'],
                             streams=[filter_stream])

        y_min = self.df[vdim].drop('coadd', axis=1).quantile(0.001).min()
        y_max = self.df[vdim].drop('coadd', axis=1).quantile(0.999).max()

        ra_min, ra_max = self.catalog.coadd_cat.ra.quantile([0, 1])
        dec_min, dec_max = self.catalog.coadd_cat.dec.quantile([0, 1])

        ranges = {vdim : (y_min, y_max),
                  'ra' : (ra_min, ra_max),
                  'dec' : (dec_min, dec_max)}

        dmap = dmap.redim.values(visit=self.catalog.visit_names, 
                                 # vdim=list(self.funcs.keys()) + ['match_distance'], 
                                 # vdim=[vdim], 
                                 label=['galaxy', 'star'],
                                 x_max=x_range).redim.range(**ranges)
        return dmap
