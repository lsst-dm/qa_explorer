import numpy as np
import pandas as pd
import holoviews as hv

from .functors import Functor, CompositeFunctor, Column, RAColumn, DecColumn, Mag
from .functors import StarGalaxyLabeller
from .catalog import MatchedCatalog

class QADataset(object):
    def __init__(self, catalog, funcs, xFunc=Mag('base_PsfFlux', allow_difference=False), 
                 labeller=StarGalaxyLabeller(),
                 query=None):
        self.catalog = catalog

        if isinstance(funcs, list) or isinstance(funcs, tuple):
            self.funcs = {'y{}'.format(i):f for i,f in enumerate(funcs)}
        elif isinstance(funcs, Functor):
            self.funcs = {'y0':funcs}
        else:
            self.funcs = funcs

        self.xFunc = xFunc
        self.labeller = labeller

        self._df = None
        self._ds = None
        self._query = query

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
        allfuncs.update({'ra':RAColumn(), 'dec': DecColumn(), 
                         'x':self.xFunc})

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

    def _make_df(self, **kwargs):
        f = CompositeFunctor(self.allfuncs)
        df = f(self.catalog, query=self.query, **kwargs)
        if self.is_matched:
            df['match_distance'] = self.catalog.match_distance
        df = df.dropna(how='any')
        self._df = df        

    @property
    def ds(self):
        if self._ds is None:
            self._make_ds()
        return self._ds

    def _make_ds(self):
        kdims = ['ra', 'dec', hv.Dimension('x', label=self.xFunc.name)]
        vdims = []
        for k,v in self.allfuncs.items():
            if k in ('ra', 'dec', 'x'):
                continue
            label = v.name
            if v.allow_difference and self.is_matched:
                label = 'diff({})'.format(label)
            vdims.append(hv.Dimension(k, label=label))

        if self.is_matched:
            vdims += [hv.Dimension('match_distance', label='Match Distance [arcsec]')]
        ds = hv.Dataset(self.df, kdims=kdims, vdims=vdims)
        self._ds = ds        

