import os
import numpy as np
import holoviews as hv
import pandas as pd

import logging

from bokeh.application.handlers import FunctionHandler
from bokeh.application import Application
from bokeh.io import show
from bokeh.layouts import layout
from bokeh.plotting import curdoc
from bokeh.models.widgets import Panel, Tabs, Select, RadioButtonGroup, TextInput, PreText
from bokeh.palettes import Spectral4, Category10, Dark2

import holoviews as hv

import datashader as ds
from holoviews.operation.datashader import aggregate, shade, datashade, dynspread
from holoviews.operation import decimate, histogram
import colorcet as cc

from holoviews.plotting.bokeh.callbacks import callbacks, Callback

# hv.extension('bokeh')

import param
import parambokeh

from .utils import Mag, CustomFunctor, DeconvolvedMoments
from .utils import StarGalaxyLabeller
from .utils import RAColumn, DecColumn

default_xFuncs = {'base_PsfFlux' : Mag('base_PsfFlux'),
                  'modelfit_CModel' : Mag('modelfit_CModel')}
default_yFuncs = {'modelfit_CModel - base_PsfFlux' : CustomFunctor('mag(modelfit_CModel) - mag(base_PsfFlux)'),
                  'Deconvolved Moments' : DeconvolvedMoments()}
default_labellers = {'default':StarGalaxyLabeller()}

def getFunc(funcName):
    if funcName in default_xFuncs:
        return default_xFuncs[funcName]
    elif funcName in default_yFuncs:
        return default_yFuncs[funcName]
    else:
        return CustomFunctor(funcName)

def getLabeller(labellerName):
    return default_labellers[labellerName]


def write_selected(explorer, filename):
    print(explorer._selected.head())

class QAExplorer(hv.streams.Stream):


    catalog_path = param.Path(default='forced_big.h5', search_paths=['data'])

    query = param.String(default='')

    id_list = param.String(default='')

    x_data = param.ObjectSelector(default='base_PsfFlux', 
                                      objects=list(default_xFuncs.keys()))
    x_data_custom = param.String(default='')

    y_data = param.ObjectSelector(default='modelfit_CModel - base_PsfFlux', 
                                      objects=list(default_yFuncs.keys()))
    y_data_custom = param.String(default='')

    labeller = param.ObjectSelector(default='default',
                                    objects = list(default_labellers.keys()))

    object_type = param.ObjectSelector(default='all',
                                       objects=['all', 'star', 'galaxy'])

    nbins = param.Integer(default=50, bounds=(10,100))

    # write_selected = param.Action(default=write_selected)

    output = parambokeh.view.Plot()

    def __init__(self, rootdir='.', *args, **kwargs):
        super(QAExplorer, self).__init__(*args, **kwargs)

        self.rootdir = rootdir

        self._catalog_path = None
        self._selected = None
        # Sets self.ds property
        self.set_data(self.catalog_path, self.query, self.id_list, 
                      self.x_data, self.x_data_custom, 
                      self.y_data, self.y_data_custom, self.labeller)

    def set_data(self, catalog_path, query, id_list, 
                 x_data, x_data_custom, y_data, y_data_custom, 
                 labeller, **kwargs):

        if catalog_path != self._catalog_path:
            self.catalog = pd.read_hdf(catalog_path)
            self.catalog.index = self.catalog.id
            self._catalog_path = catalog_path

        if id_list:
            ids = self.get_saved_ids(id_list)
            cat = self.catalog.loc[ids]
        else:
            cat = self.catalog

        if query:
            cat = cat.query(query)

        if x_data_custom:
            xFunc = getFunc(x_data_custom)
        else:
            xFunc = getFunc(x_data)
        x = xFunc(cat)
        self.xlabel = xFunc.name

        if y_data_custom:
            yFunc = getFunc(y_data_custom)
        else:
            yFunc = getFunc(y_data)
        y = yFunc(cat)
        self.ylabel = yFunc.name

        label = getLabeller(labeller)(cat)

        ra = RAColumn()(cat)
        dec = DecColumn()(cat)

        data_id = cat.id

        ok = np.isfinite(x) & np.isfinite(y)

        df = pd.DataFrame({'x' : x[ok],
                           'y' : y[ok],
                           'label' : label[ok],
                           'ra' : ra[ok],
                           'dec': dec[ok],
                           'id' : data_id[ok]})

        self.ds = hv.Dataset(df)

    @property
    def selected(self):
        return self._selected 

    @property
    def id_path(self):
        return os.path.join(self.rootdir, 'data', 'ids')

    def save_selected(self, name):
        filename = os.path.join(self.id_path, '{}.h5'.format(name))
        logging.info('writing {} ids to {}'.format(len(self.selected), filename))
        self.selected.to_hdf(filename, 'ids', mode='w')

    @property
    def saved_ids(self):
        """Returns list of names of selected IDs
        """
        return [os.path.splitext(f)[0] for f in os.listdir(self.id_path)]

    def get_saved_ids(self, id_list):
        id_list = id_list.split(',')

        files = [os.path.join(self.id_path, '{}.h5'.format(f.strip())) for f in id_list]

        return pd.concat([pd.read_hdf(f, 'ids') for f in files]).unique()

    def make_scatter(self, object_type, x_range=None, y_range=None, **kwargs):
        self.set_data(**kwargs)

        if object_type == 'all':
            dset = self.ds
        else:
            dset = self.ds.select(label=object_type)

        xdim = hv.Dimension('x', label=self.xlabel)
        ydim = hv.Dimension('y', label=self.ylabel)

        pts = dset.to(hv.Points, kdims=[xdim, ydim], vdims=['label'], groupby=[])
        scatter = dynspread(datashade(pts, x_range=x_range, y_range=y_range, dynamic=False, normalization='log'))
        scatter = scatter.opts('RGB [width=600, height=400]').relabel('{} ({})'.format(object_type, len(dset)))
        return scatter

    def make_sky(self, object_type, ra_range=None, dec_range=None, x_range=None, y_range=None, **kwargs):

        if object_type == 'all':
            dset = self.ds
        else:
            dset = self.ds.select(label=object_type)

        if x_range is not None and y_range is not None:
            dset = dset.select(x=x_range, y=y_range)

        self._selected = dset.data.id

        pts = dset.to(hv.Points, kdims=['ra', 'dec'], vdims=['y'], groupby=[])
        agg = aggregate(pts, width=100, height=100, x_range=x_range, y_range=y_range, aggregator=ds.mean('y'), dynamic=False)
        hover = hv.QuadMesh(agg).opts('[tools=["hover"]] (alpha=0 hover_alpha=0.2)')
        shaded = dynspread(datashade(pts, x_range=ra_range, y_range=dec_range, dynamic=False, 
                                     cmap=cc.palette['coolwarm'], aggregator=ds.mean('y')))
        shaded = shaded.opts('RGB [width=400, height=400]')

        return (shaded*hover).relabel('{} ({})'.format(object_type, len(dset)))

    def _make_hist(self, dim, rng, **kwargs):
        if kwargs['object_type'] == 'all':
            dset = self.ds
        else:
            dset = self.ds.select(label=kwargs['object_type'])

        if rng is None:
            lo, hi = dset.data[dim].quantile([0.005, 0.995])
            rng = [lo, hi]

        opts = 'Histogram [yaxis=None] (alpha=0.3)' + \
               ' {+framewise +axiswise} '
        h = hv.operation.histogram(dset, num_bins=kwargs['nbins'],
                                    dimension=dim, normed='height',
                                    bin_range=rng).opts(opts)

        return h

    def make_xhist(self, **kwargs):
        x_range = kwargs.pop('x_range')
        return self._make_hist('x', x_range, **kwargs)

    def make_yhist(self, **kwargs):
        y_range = kwargs.pop('y_range')
        return self._make_hist('y', y_range, **kwargs)

    def view(self):

        self.range_xy = hv.streams.RangeXY()
        self.range_sky = hv.streams.RangeXY().rename(x_range='ra_range', y_range='dec_range')

        scatter = hv.DynamicMap(self.make_scatter, streams=[self, self.range_xy])
        sky = hv.DynamicMap(self.make_sky, streams=[self, self.range_sky, self.range_xy])

        xhist = hv.DynamicMap(self.make_xhist, kdims=[], streams=[self, self.range_xy])
        yhist = hv.DynamicMap(self.make_yhist, kdims=[], streams=[self, self.range_xy])

        l = (scatter + sky + yhist + xhist).cols(2)
        return l


