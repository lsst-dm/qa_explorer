import holoviews as hv
import pandas as pd
from holoviews.operation.datashader import datashade, dynspread
from holoviews.operation import decimate
decimate.max_samples=5000
dynspread.max_px = 10
import datashader
import colorcet as cc
from bokeh.models import HoverTool


from .functors import CompositeFunctor, Column, RAColumn, DecColumn, Mag

class QAPlot(object):
    def __init__(self, catalog, dask=False, **kwargs):
        self.catalog = catalog
        self.dask = dask
        self.kwargs = kwargs

        self._figure = None
        self._ds = None

    @property
    def figure(self):
        if self._figure is None:
            self._make_figure()
        return self._figure

    @property
    def selected(self):
        return self._get_selected()

    def _get_selected(self):
        raise NotImplementedError

    @property
    def ds(self):
        if self._ds is None:
            self._make_ds()
        return self._ds

    def _make_ds(self):
        raise NotImplementedError

    def _make_figure(self):
        raise NotImplementedError

class MultiFuncQAPlot(QAPlot):
    def __init__(self, catalog, funcs, width=400, **kwargs):
        super(MultiFuncQAPlot, self).__init__(catalog, **kwargs)

        if not isinstance(funcs, dict):
            self.funcs = {'y':funcs}
        else:
            self.funcs = funcs

        self.width = width

    @property
    def allfuncs(self):
        allfuncs = self.funcs.copy()
        allfuncs.update({'ra':RAColumn(), 'dec': DecColumn()})
        return allfuncs        

    def _make_ds(self):
        f = CompositeFunctor(self.allfuncs)
        df = f(self.catalog, dask=self.dask)
        ds = hv.Dataset(df)
        self._ds = ds        

class ScatterSkyPlot(MultiFuncQAPlot):

    def __init__(self, catalog, funcs, xfunc=Mag('base_PsfFlux'), **kwargs):
        super(ScatterSkyPlot, self).__init__(catalog, funcs, **kwargs)
        self.xfunc = xfunc

    @property
    def allfuncs(self):
        allfuncs = super(ScatterSkyPlot, self).allfuncs
        allfuncs.update({'x':self.xfunc})
        return allfuncs

    def _make_figure(self):

        rgb_opts = dict(plot={'width':700, 'height':300})
        rgb_sky_opts = dict(plot={'width':300, 'height':300})
        decimate_opts = dict(plot={'tools':['hover', 'box_select']}, 
                            style={'alpha':0, 'size':5, 'nonselection_alpha':0})

        plots = []
        for k,v in self.funcs.items():
            pts = self.ds.to(hv.Points, kdims=['x', k], groupby=[])
            dshade = dynspread(datashade(pts)).opts(**rgb_opts)

            pts_sky = self.ds.to(hv.Points, kdims=['ra', 'dec'],
                                 vdims=['x', k], groupby=[])
            dshade_sky = dynspread(datashade(pts_sky, aggregator=datashader.mean(k), 
                                    cmap=cc.palette['coolwarm'])).opts(**rgb_sky_opts)
            # dec = decimate(pts).opts(**decimate_opts)
            # o = (dshade * dec).relabel(v.name)
            dshade = dshade.relabel(v.name)
            plots += [dshade, dshade_sky]

        self._figure = hv.Layout(plots).cols(2)


class SkyPlot(MultiFuncQAPlot):
    """Makes linked sky plots of desired quantities.

    funcs: dictionary of Functors
    """
    def __init__(self, catalog, funcs, cmap=cc.palette['coolwarm'], **kwargs):
        super(SkyPlot, self).__init__(catalog, funcs, **kwargs)

        self.cmap = cmap

        self._pts = None
        self._box = None

    def _make_figure(self):

        pts = self.ds.to(hv.Points, kdims=['ra', 'dec'], 
                    vdims=list(self.funcs.keys()), groupby=[])
        
        mean_ra = pts.data.ra.mean()
        mean_dec = pts.data.dec.mean()
        try:
            mean_ra = mean_ra.compute()
            mean_dec = mean_dec.compute()
        except AttributeError:
            pass

        pointer = hv.streams.PointerXY(x=mean_ra, y=mean_dec)
        cross_opts = dict(style={'line_width':1, 'color':'black'})
        cross_dmap = hv.DynamicMap(lambda x, y: (hv.VLine(x).opts(**cross_opts) * 
                                                 hv.HLine(y).opts(**cross_opts)), streams=[pointer])    
        

        # sel = hv.streams.Selection1D(source=pts)
        box = hv.streams.BoundsXY(source=pts, bounds=(mean_ra, mean_dec, mean_ra, mean_dec))
        bounds_opts = dict(style={'line_color':'black', 'line_width':2})
        bounds = hv.DynamicMap(lambda bounds : hv.Bounds(bounds), streams=[box]).opts(**bounds_opts)

        rgb_opts = dict(plot={'width':self.width, 'height':self.width})

        # hover = HoverTool(names=list(self.funcs.keys()))
        # hover.tooltips.append(('index', '$index'))

        decimate_opts = dict(plot={'tools':['hover', 'box_select']}, 
                            style={'alpha':0, 'size':5, 'nonselection_alpha':0})
        plots = []
        for k,v in self.funcs.items():
            dshade = dynspread(datashade(pts, aggregator=datashader.mean(k), cmap=self.cmap)).opts(**rgb_opts)
            dec = decimate(pts).opts(**decimate_opts)
            o = (dshade * dec * cross_dmap * bounds).relabel(v.name)
            plots.append(o)

        self._box = box

        self._figure = hv.Layout(plots).cols(2)

    def _get_selected(self):
        selected = self.ds.select(ra=self._box.bounds[0::2], dec=self._box.bounds[1::2])
        return selected.data