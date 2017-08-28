import holoviews as hv
from holoviews.operation.datashader import datashade, dynspread
from holoviews.operation import decimate
decimate.max_samples=5000
dynspread.max_px = 10
import datashader
import colorcet as cc

from .functors import CompositeFunctor, Column, RAColumn, DecColumn

def sky_compare(catalog, funcs, cmap=cc.palette['coolwarm'], width=400):
    """Makes linked sky plots of desired quantities.

    funcs: dictionary of Functors
    """
    allfuncs = funcs.copy()
    allfuncs.update({'id':Column('id'), 'ra':RAColumn(), 'dec': DecColumn()})
    f = CompositeFunctor(allfuncs)
    ds = hv.Dataset(f(catalog, dropna=True))
    pts = ds.to(hv.Points, kdims=['ra', 'dec'], vdims=['id'] + list(funcs.keys()), groupby=[])
    
    pointer = hv.streams.PointerXY(x=pts.data.ra.mean(), y=pts.data.dec.mean())
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