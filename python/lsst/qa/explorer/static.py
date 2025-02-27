"""
Utilties for loading static pipe_analysis PNGs into holoviews DynamicMaps


See 'qa-image-browser.ipynb' demo notebook for examples of usage.  
"""
from __future__ import print_function, division
from functools import partial
import numpy as np
import holoviews as hv
import logging

try:
    from lsst.pipe.analysis.utils import Filenamer
except ImportError:
    logging.warning('Pipe analysis not available.')

from .rc import wide_filters, cosmos_filters
from .utils import get_tracts, get_visits    

def get_color_plot(butler, tract=8766, description='color_wPerp', style='psfMagHist', scale=None):
    dataId = {'tract':tract}
    filenamer = Filenamer(butler, 'plotColor', dataId)
    filename = filenamer(description=description, dataId=dataId, style=style)
    try:
        rgb = hv.RGB.load_image(filename, bare=True)

        # back out the aspect ratio from bounds
        l,b,r,t = rgb.bounds.lbrt()
        aspect = (r-l)/(t-b)
        h = 480
        w = int(h * aspect)
        rgb = rgb.opts(plot={'width':w, 'height':h})
        if scale is not None:
            rgb = rgb.opts(plot={'width':int(w*scale), 'height':int(h*scale)})

        return rgb
    except FileNotFoundError:
        return hv.RGB(np.zeros((2,2))).opts(plot={'width':640, 'height':480})
    
def color_tract_layout(butler, description, style='psfMagHist', tracts=None, scale=1.0):
    if tracts is None:
        tracts = get_tracts(butler)
    return hv.Layout([get_color_plot(butler, tract, description=description, style=style, scale=scale) 
                         for tract in tracts])
    
def color_dmap(butler, tracts=None, descriptions=['color_wPerp', 'color_xPerp', 'color_yPerp'], 
               styles=['psfMagHist', 'sky-stars'], scale=1.0):
    if tracts is None:
        tracts = get_tracts(butler)
    dmap = hv.DynamicMap(partial(color_tract_layout, butler=butler, tracts=tracts, scale=scale), kdims=['description', 'style'])
    dmap = dmap.redim.values(description=descriptions, style=styles)
    return dmap

def get_plot_filename(butler, tract, filt, description, style, visit=None, kind='coadd'):
    dataId = {'tract':tract, 'filter':filt}
    if visit is not None:
        dataId.update({'visit':visit})
        
    filenamer = Filenamer(butler, 'plot{}'.format(kind.capitalize()), dataId)
    filename = filenamer(description=description, style=style, dataId=dataId)
    return filename    

def get_plot(butler, tract, filt, description, style, visit=None, kind='coadd', scale=None):
    filename = get_plot_filename(butler, tract, filt, description, style, visit=visit, kind=kind)
    try:
        rgb = hv.RGB.load_image(filename, bare=True)

        # back out the aspect ratio from bounds
        l,b,r,t = rgb.bounds.lbrt()
        aspect = (r-l)/(t-b)
        h = 480
        w = int(h * aspect)
        rgb = rgb.opts(plot={'width':w, 'height':h})
        if scale is not None:
            rgb = rgb.opts(plot={'width':int(w*scale), 'height':int(h*scale)})
        return rgb
    except FileNotFoundError:
        return hv.RGB(np.zeros((2,2))).opts(plot={'width':640, 'height':480})
    
def filter_layout(butler, tract=9813, description='mag_modelfit_CModel', style='psfMagHist', 
                    visit=None, kind='coadd', scale=0.66, columns=3):
    if tract==9813:
        filters = cosmos_filters
    else:
        filters = wide_filters
        
    return hv.Layout([get_plot(butler, tract, f, description, style, kind, scale=scale)
                               for f in filters]).cols(columns)
    
def description_layout(butler, descriptions, tract=9813, filt='HSC-I', style='psfMagHist', 
                        visit=None, kind='coadd', scale=0.66, columns=3):
    return hv.Layout([get_plot(butler, tract, filt, desc, style, visit=visit, kind=kind, scale=scale) 
                               for desc in descriptions]).cols(columns)
    
def filter_layout_dmap_coadd(butler, descriptions, tracts=None, 
                            styles=['psfMagHist', 'sky-stars', 'sky-gals'], scale=0.66):
    if tracts is None:
        tracts = get_tracts(butler)

    if len(tracts) > 1:
        layout_fn = partial(filter_layout, butler=butler, visit=None, kind='coadd', scale=scale)
        values = dict(tract=tracts, description=descriptions, style=styles)
        dmap = hv.DynamicMap(layout_fn, kdims=['tract', 'description', 'style'])
    else:
        layout_fn = partial(filter_layout, butler=butler, tract=tracts[0],
                            visit=None, kind='coadd', scale=scale)
        values = dict(description=descriptions, style=styles)
        dmap = hv.DynamicMap(layout_fn, kdims=['description', 'style'])
    dmap = dmap.redim.values(**values) 

    return dmap
    
def description_layout_dmap_visit(butler, tract, descriptions, filt='HSC-I', styles=['psfMagHist', 'sky-stars', 'sky-gals'], scale=0.66):
    # visits = get_visits(field_name(tract), filt)
    if tract is None:
        tract = get_tracts(butler)[0]
    visits = get_visits(butler, tract, filt)
    dmap = hv.DynamicMap(partial(description_layout, descriptions=descriptions, butler=butler, tract=tract, filt=filt, kind='visit', scale=scale), 
                     kdims=['visit', 'style'])
    dmap = dmap.redim.values(visit=visits, style=styles)
    return dmap

    
