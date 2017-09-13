import holoviews as hv
from holoviews.operation.datashader import datashade, dynspread
from holoviews.operation import decimate
decimate.max_samples=5000
dynspread.max_px = 10
import datashader
import colorcet as cc

from .functors import CompositeFunctor, Column, RAColumn, DecColumn

def sky_compare(catalog, funcs, cmap=cc.palette['coolwarm'], width=400, dask=False):
    """Makes linked sky plots of desired quantities.

    funcs: dictionary of Functors
    """

    allfuncs = funcs.copy()
    allfuncs.update({'ra':RAColumn(), 'dec': DecColumn()})
    f = CompositeFunctor(allfuncs)
    ds = hv.Dataset(f(catalog, dropna=True, dask=dask))
    pts = ds.to(hv.Points, kdims=['ra', 'dec'], vdims=list(funcs.keys()), groupby=[])
    
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
    
    rgb_opts = dict(plot={'width':width, 'height':width})
    decimate_opts = dict(plot={'tools':["hover"]}, style={'alpha':0, 'size':4})
    plots = []
    for k,v in funcs.items():
        dshade = dynspread(datashade(pts, aggregator=datashader.mean(k), cmap=cmap)).opts(**rgb_opts)
        dec = decimate(pts).opts(**decimate_opts)
        o = (dshade*dec*cross_dmap).relabel(v.name)
        plots.append(o)
    return hv.Layout(plots).cols(2)