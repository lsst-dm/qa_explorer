from functools import partial

import param
import numpy as np
import pandas as pd
import holoviews as hv
import datashader as ds
import colorcet as cc

from param import ParameterizedFunction, ParamOverrides
from holoviews.core.operation import Operation
from holoviews.streams import Stream, BoundsXY, LinkedStream
from holoviews.plotting.bokeh.callbacks import Callback
from holoviews.operation.datashader import datashade, dynspread

from bokeh.palettes import Greys9

class filterpoints(hv.Operation):

    groupby = param.Parameter(default=None, doc="""
        Groupby argument to pass to Points creation.""")

    filter_range = param.Parameter(default={}, doc="""
        Dictionary of filter bounds.""")

    xdim = param.String(default='x')
    ydim = param.String(default='y')

    def _process(self, dset, key=None):

        if self.p.filter_range is not None:
            dset = dset.select(**self.p.filter_range)

        vdims = [dim for dim in dset.dimensions() if dim.name not in [self.p.xdim, self.p.ydim]]
        pts = dset.to(hv.Points, kdims=[self.p.xdim, self.p.ydim], 
                        vdims=vdims, 
                        groupby=self.p.groupby)

        return pts

# Define Stream class that stores filters for various Dimensions 
class FilterStream(Stream):
    """
    Stream to apply arbitrary filtering on a Dataset.
    """

    filter_range = param.Dict(default={})
    
class ResetCallback(Callback):

    models = ['plot']
    on_events = ['reset']

class Reset(LinkedStream):
    def __init__(self, *args, **params):
        super(Reset, self).__init__(self, *args, **dict(params, transient=True))

Stream._callbacks['bokeh'][Reset] = ResetCallback

# Define Operation that filters based on FilterStream state (which provides the filter_range)
class filterpoints(Operation):

    filter_range = param.Dict(default={}, doc="""
        Dictionary of filter bounds.""")
    xdim = param.String(default='x')
    ydim = param.String(default='y')

    def _process(self, dset, key=None):
        if self.p.filter_range is not None:
            dset = dset.select(**self.p.filter_range)
        kdims = [self.p.xdim, self.p.ydim]
        vdims = [dim for dim in dset.dimensions() if dim.name not in kdims]
        return hv.Points(dset, kdims=kdims, vdims=vdims)
    

def notify_stream(bounds, filter_stream, xdim, ydim):
    """
    Function to attach to bounds stream as subscriber to notify FilterStream.
    """
    l, b, r, t = bounds
    filter_range = dict(filter_stream.filter_range)
    for dim, (low, high) in [(xdim, (l, r)), (ydim, (b, t))]:
        if dim in filter_range:
            old_low, old_high = filter_range[dim]
            filter_range[dim]= (max(old_low, low), min(old_high, high))
        else:
            filter_range[dim] = (low, high)
    filter_stream.event(filter_range=filter_range)

def reset_stream(filter_stream):
    filter_stream.event(filter_range={})

class scattersky(ParameterizedFunction):
    """
    Creates two datashaded views from a Dataset.
    """

    xdim = param.String(default='x')
    ydim = param.String(default='y')
    scatter_cmap = param.String(default='fire')
    sky_cmap = param.String(default='coolwarm')
    height = param.Number(default=300)
    width = param.Number(default=900)
    filter_stream = param.ClassSelector(default=FilterStream(), class_=FilterStream)

    def __call__(self, dset, **params):
        self.p = ParamOverrides(self, params)
        if self.p.ydim not in dset.dimensions():
            raise ValueError('{} not in Dataset.'.format(self.p.ydim))

        # Set up scatter plot
        scatter_filterpoints = filterpoints.instance(xdim=self.p.xdim, ydim=self.p.ydim)
        scatter_pts = hv.util.Dynamic(dset, operation=scatter_filterpoints,
                                      streams=[self.p.filter_stream])
        scatter_opts = dict(plot={'height':self.p.height, 'width':self.p.width - self.p.height},
                           norm=dict(framewise=True, axiswise=True))
        scatter_shaded = datashade(scatter_pts, cmap=cc.palette[self.p.scatter_cmap])
        scatter = dynspread(scatter_shaded).opts(**scatter_opts)
        
        # Set up sky plot
        sky_filterpoints = filterpoints.instance(xdim='ra', ydim='dec')
        sky_pts = hv.util.Dynamic(dset, operation=sky_filterpoints,
                                  streams=[self.p.filter_stream])
        sky_opts = dict(plot={'height':self.p.height, 'width':self.p.height},
                        norm=dict(framewise=True, axiswise=True))
        sky_shaded = datashade(sky_pts, cmap=cc.palette[self.p.sky_cmap],
                               aggregator=ds.mean(self.p.ydim), height=self.p.height,
                               width=self.p.width)
        sky = dynspread(sky_shaded).opts(**sky_opts)
        
        # Set up BoundsXY streams to listen to box_select events and notify FilterStream
        scatter_select = BoundsXY(source=scatter)
        scatter_notifier = partial(notify_stream, filter_stream=self.p.filter_stream,
                                   xdim=self.p.xdim, ydim=self.p.ydim)
        scatter_select.add_subscriber(scatter_notifier)
        
        sky_select = BoundsXY(source=sky)
        sky_notifier = partial(notify_stream, filter_stream=self.p.filter_stream,
                               xdim='ra', ydim='dec')
        sky_select.add_subscriber(sky_notifier)
        
        # Reset
        reset = Reset(source=scatter)
        reset.add_subscriber(partial(reset_stream, filter_stream))
        
        raw_scatter = datashade(scatter_filterpoints(dset), cmap=Greys9[::-1][:5], dynamic=False)
        raw_sky = datashade(sky_filterpoints(dset), cmap=Greys9[::-1][:5], dynamic=False)
        
        return raw_scatter*scatter + raw_sky*sky

class multi_scattersky(ParameterizedFunction):
    
    filter_stream = param.ClassSelector(default=FilterStream(), class_=FilterStream)

    ignored_dimensions = param.List(default=['x', 'ra', 'dec', 'label'])
    
    def _get_ydims(self, dset):
        # Get dimensions from first Dataset type found in input
        return [dim.name for dim in dset.traverse(lambda x: x, [hv.Dataset])[0].dimensions()
                if dim.name not in self.p.ignored_dimensions]
    
    def __call__(self, dset, **params):
        self.p = param.ParamOverrides(self, params)
        return hv.Layout([scattersky(dset, filter_stream=filter_stream,
                                  ydim=ydim) 
                       for ydim in self._get_ydims(dset)]).cols(2)
