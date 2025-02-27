import os
import numpy as np
import holoviews as hv
import pandas as pd

import logging

from bokeh.models import HoverTool

import holoviews as hv

import datashader as ds
from holoviews.operation.datashader import aggregate, datashade, dynspread
import colorcet as cc

import param
import parambokeh

from lsst.pipe.tasks.functors import (Mag, CustomFunctor, DeconvolvedMoments,
                                      RAColumn, DecColumn,
                                      Column, SdssTraceSize, PsfSdssTraceSizeDiff,
                                      HsmTraceSize, PsfHsmTraceSizeDiff, CompositeFunctor)
from .functors import StarGalaxyLabeller

default_xFuncs = {'base_PsfFlux' : Mag('base_PsfFlux'),
                  'modelfit_CModel' : Mag('modelfit_CModel')}
default_yFuncs = {'modelfit_CModel - base_PsfFlux' : CustomFunctor('mag(modelfit_CModel) - mag(base_PsfFlux)'),
                  'Deconvolved Moments' : DeconvolvedMoments(),
                  'Footprint NPix' : Column('base_Footprint_nPix'),
                  'ext_photometryKron_KronFlux - base_PsfFlux' : \
                        CustomFunctor('mag(ext_photometryKron_KronFlux) - mag(base_PsfFlux)'),
                  'base_GaussianFlux - base_PsfFlux' : CustomFunctor('mag(base_GaussianFlux) - mag(base_PsfFlux)'),
                  'SDSS Trace Size' : SdssTraceSize(),
                  'PSF - SDSS Trace Size' : PsfSdssTraceSizeDiff(),
                  'HSM Trace Size' : HsmTraceSize(),
                  'PSF - HSM Trace Size': PsfHsmTraceSizeDiff()}

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

def get_default_range(x, y):
    x = pd.Series(x).dropna()
    y = pd.Series(y).dropna()
    xMed = np.median(x)
    yMed = np.median(y)
    xMAD = np.median(np.absolute(x - xMed))
    yMAD = np.median(np.absolute(y - yMed))

    ylo = yMed - 10*yMAD
    yhi = yMed + 10*yMAD

    xlo, xhi = x.quantile([0., 0.99])
    xBuffer = xMAD/4.
    xlo -= xBuffer
    xhi += xBuffer

    return (xlo, xhi), (ylo, yhi)

class QAExplorer(hv.streams.Stream):


    catalog = param.Path(default='forced_big.parq', search_paths=['.','data'])

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

    nbins = param.Integer(default=20, bounds=(10,100))

    # write_selected = param.Action(default=write_selected)

    output = parambokeh.view.Plot()

    def __init__(self, rootdir='.', *args, **kwargs):
        super(QAExplorer, self).__init__(*args, **kwargs)

        self.rootdir = rootdir

        self._ds = None
        self._selected = None

        # Sets self.ds property

        # self._set_data(self.catalog, self.query, self.id_list, 
        #               self.x_data, self.x_data_custom, 
        #               self.y_data, self.y_data_custom, self.labeller)

    @property
    def funcs(self):
        return self._get_funcs(self.x_data, self.x_data_custom,
                               self.y_data, self.y_data_custom,
                               self.labeller)

    def _get_funcs(self, x_data, x_data_custom, y_data, y_data_custom,
                    labeller):
        if self.x_data_custom:
            xFunc = getFunc(self.x_data_custom)
        else:
            xFunc = getFunc(self.x_data)

        if self.y_data_custom:
            yFunc = getFunc(self.y_data_custom)
        else:
            yFunc = getFunc(self.y_data)

        labeller = getLabeller(self.labeller)

        return CompositeFunctor({'x' : xFunc,
                                 'y' : yFunc,
                                 'label' : labeller,
                                 'id' : Column('id'),
                                 'ra' : RAColumn(),
                                 'dec': DecColumn()})

    def _set_data(self, catalog, query, id_list, 
                 x_data, x_data_custom, y_data, y_data_custom, 
                 labeller, **kwargs):
        funcs = self._get_funcs(x_data, x_data_custom,
                                y_data, y_data_custom,
                                labeller)

        df = funcs(catalog, query=query)
        df.index = df['id']

        if id_list:
            ids = self.get_saved_ids(id_list)
            df = df.loc[ids]

        ok = np.isfinite(df.x) & np.isfinite(df.y)
        xdim = hv.Dimension('x', label=funcs['x'].name)
        ydim = hv.Dimension('y', label=funcs['y'].name)
        self._ds = hv.Dataset(df[ok], kdims=[xdim, ydim, 'ra', 'dec', 'id'], vdims=['label'])

    @property
    def ds(self):
        if self._ds is None:
            self._set_data(self.catalog, self.query, self.id_list,
                           self.x_data, self.x_data_custom,
                           self.y_data, self.y_data_custom,
                           self.labeller)
        return self._ds

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
        self._set_data(**kwargs)
        logging.info('x_range={}, y_range={}'.format(x_range, y_range))

        if object_type == 'all':
            dset = self.ds
        else:
            dset = self.ds.select(label=object_type)

        pts = dset.to(hv.Points, kdims=['x', 'y'], vdims=['label'], groupby=[])
        print(pts.dimensions())

        scatter = dynspread(datashade(pts, x_range=x_range, y_range=y_range, dynamic=False, normalization='log'))
        hover = HoverTool(tooltips=[("(x,y)", "($x, $y)")])
        # hv.opts({'RGB': {'plot' : {'tools' : [hover]}}}, scatter)
        # scatter = scatter.opts(plot=dict(tools=[hover]))

        title = '{} ({}) {}'.format(object_type, len(dset), pts.get_dimension('y').label)
        scatter = scatter.opts('RGB [width=600, height=400]').relabel(title)
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
        agg = aggregate(pts, width=100, height=100, x_range=ra_range, y_range=dec_range, aggregator=ds.mean('y'), dynamic=False)
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

    @property
    def default_range(self):
        x = self.ds.data.x.dropna()
        y = self.ds.data.y.dropna()
        xMed = np.median(x)
        yMed = np.median(y)
        xMAD = np.median(np.absolute(x - xMed))
        yMAD = np.median(np.absolute(y - yMed))

        ylo = yMed - 10*yMAD
        yhi = yMed + 10*yMAD

        xlo, xhi = x.quantile([0., 0.99])
        xBuffer = xMAD/4.
        xlo -= xBuffer
        xhi += xBuffer

        # print(xlo, xhi, ylo, yhi)
        return (xlo, xhi), (ylo, yhi)

    def view(self):

        x_range, y_range = self.default_range

        range_xy = hv.streams.RangeXY()
        range_sky = hv.streams.RangeXY().rename(x_range='ra_range', y_range='dec_range')

        scatter = hv.DynamicMap(self.make_scatter, streams=[self, range_xy])
        sky = hv.DynamicMap(self.make_sky, streams=[self, range_sky, range_xy])

        xhist = hv.DynamicMap(self.make_xhist, kdims=[], streams=[self, range_xy])
        yhist = hv.DynamicMap(self.make_yhist, kdims=[], streams=[self, range_xy])

        l = (scatter + sky + yhist + xhist).cols(2)
        return l


