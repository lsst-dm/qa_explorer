import holoviews as hv
import param
import pandas as pd
from holoviews.operation.datashader import datashade, dynspread
from holoviews.operation import decimate
decimate.max_samples=5000
dynspread.max_px = 10
import datashader
import colorcet as cc
from bokeh.models import HoverTool


from .functors import Functor, CompositeFunctor, Column, RAColumn, DecColumn, Mag
from .functors import StarGalaxyLabeller

class QAPlot(hv.streams.Stream):
    query = param.String(default='')

    def __init__(self, catalog, dask=False, **kwargs):
        self.catalog = catalog
        self.dask = dask
        self.kwargs = kwargs

        self._figure = None
        self._df = None
        self._ds = None

        super(QAPlot, self).__init__()

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

    def set_query(self, q):
        self.event(query=q)

    @property
    def df(self):
        if self._df is None:
            self._make_df()
        return self._df

    @property
    def ds(self):
        if self._ds is None:
            self._make_ds()

        ds = self._ds
        if self.query:
            df = ds.data.query(self.query)
            ds = hv.Dataset(df, kdims=self._ds.dimensions())

        return ds

    def _make_ds(self):
        raise NotImplementedError

    def _make_figure(self):
        raise NotImplementedError

class MultiFuncQAPlot(QAPlot):
    def __init__(self, catalog, funcs, labeller=StarGalaxyLabeller(), width=400, 
                 group_labels=False, **kwargs):
        super(MultiFuncQAPlot, self).__init__(catalog, **kwargs)

        if isinstance(funcs, list) or isinstance(funcs, tuple):
            self.funcs = {'y{}'.format(i):f for i,f in enumerate(funcs)}
        elif isinstance(funcs, Functor):
            self.funcs = {'y':funcs}
        else:
            self.funcs = funcs

        self.labeller = labeller
        self.group_labels = group_labels
        self.width = width

    @property
    def allfuncs(self):
        allfuncs = self.funcs.copy()
        allfuncs.update({'ra':RAColumn(), 'dec': DecColumn(), 'label':self.labeller})
        return allfuncs        

    @property
    def groupby(self):
        return ['label'] if self.group_labels else []

    def _make_df(self):
        f = CompositeFunctor(self.allfuncs)
        df = f(self.catalog, dask=self.dask)
        self._df = df        

    def _make_ds(self):
        dims = [hv.Dimension(k, label=v.name) for k,v in self.allfuncs.items()]
        ds = hv.Dataset(self.df, kdims=dims)
        self._ds = ds        

class ScatterSkyPlot(MultiFuncQAPlot):

    def __init__(self, catalog, funcs, xfunc=Mag('base_PsfFlux'), linked=False, **kwargs):
        super(ScatterSkyPlot, self).__init__(catalog, funcs, **kwargs)

        self.linked = linked
        self.xfunc = xfunc

    @property
    def groupby(self):
        return []

    @property
    def allfuncs(self):
        allfuncs = super(ScatterSkyPlot, self).allfuncs
        allfuncs.update({'x':self.xfunc})
        return allfuncs

    def _make_sky(self, ra_range, dec_range, ydim, **kwargs):
        # bounds = kwargs['bounds_{}'.format(ydim)]

        # if bounds is not None:
        #     select_dict = {'x':bounds[0::2], ydim:bounds[1::2]}
        #     dset = self.ds.select(**select_dict)
        # else:
        #     dset = self.ds
            
        dset = self._get_selected_dset(ydim, ignore_ydim=False, **kwargs)

        pts = dset.to(hv.Points, kdims=['ra', 'dec'], 
                        vdims=['x'] + list(self.funcs.keys()), groupby=self.groupby)

        shaded = dynspread(datashade(pts, x_range=ra_range, y_range=dec_range, dynamic=False, 
                                         cmap=cc.palette['coolwarm'], aggregator=datashader.mean(ydim)))
        shaded = shaded.opts('RGB [width=300, height=300]')

        return shaded

    def _get_selected_dset(self, ydim, ignore_ydim=False, **kwargs):
        dset = self.ds

        if self.linked:
            for k in self.funcs.keys():
                if k==ydim and ignore_ydim:
                    pass
                else:
                    bounds = kwargs['bounds_{}'.format(k)]
                    if bounds is not None:
                        select_dict = {'x':bounds[0::2], k:bounds[1::2]}
                        dset = dset.select(**select_dict)

        return dset

    def _get_selected(self):
        kwargs = {}
        [kwargs.update(b.contents) for b in self._bounds_streams]
        return self._get_selected_dset(None, **kwargs).data

    def _make_scatter(self, x_range, y_range, ydim, **kwargs):
        kwarg_str = ','.join([k for k,v in kwargs.items() if v is not None])

        selected_dset = self._get_selected_dset(ydim, ignore_ydim=True, **kwargs)

        pts = selected_dset.to(hv.Points, kdims=['x', ydim], 
                        vdims=list(self.funcs.keys()), groupby=self.groupby)

        shaded = dynspread(datashade(pts, x_range=x_range, y_range=y_range, dynamic=False,
                                    cmap=cc.palette['fire']))
        shaded = shaded.opts('RGB [width=600, height=300, tools=["box_select"]]')

        bounds = kwargs['bounds_{}'.format(ydim)]
        if bounds is None:
            bounds = (0,0,0,0)
            
        box = hv.Bounds(bounds)

        # dset = self._get_selected_dset(ydim, **kwargs)


        # decimate_opts = dict(plot={'tools':['hover', 'box_select']}, 
        #                     style={'alpha':0, 'size':5, 'nonselection_alpha':0})
        # dec = decimate(pts).opts(**decimate_opts)

        # If a selection is made, also include all points at lower alpha

        label = '{} objects'.format(len(selected_dset))

        if len(selected_dset) < len(self.ds) and False:
            all_pts = self.ds.to(hv.Points, kdims=['x', ydim], 
                        vdims=list(self.funcs.keys()), groupby=self.groupby)

            all_shaded = dynspread(datashade(all_pts, x_range=x_range, y_range=y_range, 
                                                    dynamic=False, cmap='grey'))
            all_shaded = all_shaded.opts('RGB [width=600, height=300]')

            return (all_shaded * shaded * box).relabel(label)

        else:
            return (shaded * box).relabel(label)

    def _make_figure(self):

        DimName = hv.streams.Stream.define('DimName', ydim='y')

        bounds_streams = []

        scatters = []
        sky_plots = []
        for k,v in self.funcs.items():
            dimname = DimName(ydim=k)
            range_xy = hv.streams.RangeXY()
            range_sky = hv.streams.RangeXY().rename(x_range='ra_range', y_range='dec_range')
            bounds_xy = hv.streams.Bounds().rename(bounds='bounds_{}'.format(k))

            scatter = hv.DynamicMap(self._make_scatter, kdims=[], 
                                    streams=[bounds_xy, range_xy, dimname, self])
            sky = hv.DynamicMap(self._make_sky, kdims=[], 
                                streams=[bounds_xy, range_sky, dimname, self])

            scatters.append(scatter)
            sky_plots.append(sky)
            bounds_streams.append(bounds_xy)

        for bounds, scatter in zip(bounds_streams, scatters):
            bounds.source = scatter

        plots = []
        for i, (scatter, sky) in enumerate(zip(scatters, sky_plots)):
            if self.linked:
                for j, bounds in enumerate(bounds_streams):
                    if i != j:
                        scatter.streams.append(bounds)
                        sky.streams.append(bounds)
            plots += [scatter, sky]

        self._bounds_streams = bounds_streams
        self._figure = hv.Layout(plots).cols(2)

    def reset_bounds(self):
        for b in self._bounds_streams:
            b.event(bounds=None)




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
                    vdims=list(self.funcs.keys()), groupby=self.groupby)
        
        mean_ra = self.ds.data.ra.mean()
        mean_dec = self.ds.data.dec.mean()
        try:
            mean_ra = mean_ra.compute()
            mean_dec = mean_dec.compute()
        except AttributeError:
            pass

        pointer = hv.streams.PointerXY(x=mean_ra, y=mean_dec)
        cross_opts = dict(style={'line_width':1, 'color':'black'})
        cross_dmap = hv.DynamicMap(lambda x, y: (hv.VLine(x).opts(**cross_opts) * 
                                                 hv.HLine(y).opts(**cross_opts)), streams=[pointer])    
        
        rgb_opts = dict(plot={'width':self.width, 'height':self.width})

        # hover = HoverTool(names=list(self.funcs.keys()))
        # hover.tooltips.append(('index', '$index'))

        decimate_opts = dict(plot={'tools':['hover', 'box_select']}, 
                            style={'alpha':0, 'size':5, 'nonselection_alpha':0})
        plots = []
        for k,v in self.funcs.items():
            dshade = dynspread(datashade(pts, aggregator=datashader.mean(k), cmap=self.cmap)).opts(**rgb_opts)
            dec = decimate(pts).opts(**decimate_opts)
            o = (dshade * dec * cross_dmap).relabel(v.name)
            plots.append(o)

        self._figure = hv.Layout(plots).cols(2).opts('Layout {+axiswise}')

    def _get_selected(self):
        selected = self.ds.select(ra=self._box.bounds[0::2], dec=self._box.bounds[1::2])
        return selected.data