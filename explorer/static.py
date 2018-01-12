from itertools import product
from functools import partial
import numpy as np
import holoviews as hv
import logging

try:
    from lsst.pipe.analysis.utils import Filenamer
except ImportError:
    logging.warning('Pipe analysis not available.')

from .rc import wide_filters, cosmos_filters, get_visits, field_name

def get_color_plot(butler, tract=8766, description='color_wPerp', style='psfMagHist', scale=1.0):
    dataId = {'tract':tract}
    filenamer = Filenamer(butler, 'plotColor', dataId)
    filename = filenamer(description=description, dataId=dataId, style=style)

    try:
        return hv.RGB.load_image(filename).opts(plot={'xaxis':None, 'yaxis':None,
                                                      'width':int(640*scale), 'height':int(480*scale)})
    except FileNotFoundError:
        return hv.RGB(np.zeros((2,2)))
    
def color_tract_layout(butler, description, style='psfMagHist', tracts=[8766, 8767, 9813], scale=1.0):
    return hv.Layout([get_color_plot(butler, tract, description=description, style=style, scale=scale) 
                         for tract in tracts])
    
def color_dmap(butler, tracts=[8766, 8767, 9813], descriptions=['color_wPerp', 'color_xPerp', 'color_yPerp'], 
               styles=['psfMagHist', 'sky-stars'], scale=1.0):
    
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
        rgb = hv.RGB.load_image(filename).opts(plot={'xaxis':None, 'yaxis':None})
        if scale is not None:
            rgb = rgb.opts(plot={'width':int(640*scale), 'height':int(480*scale)})
        return rgb
    except FileNotFoundError:
        return hv.RGB(np.zeros((2,2)))
    
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
    
def filter_layout_dmap_coadd(butler, descriptions, tracts=[8766, 8767, 9813], 
                            styles=['psfMagHist', 'sky-stars', 'sky-gals'], scale=0.66):
    dmap = hv.DynamicMap(partial(filter_layout, butler=butler, visit=None, kind='coadd', scale=scale), 
                     kdims=['tract', 'description', 'style'])
    dmap = dmap.redim.values(tract=tracts, description=descriptions, style=styles) 

    return dmap
    
def description_layout_dmap_visit(butler, tract, descriptions, filt='HSC-I', styles=['psfMagHist', 'sky-stars', 'sky-gals'], scale=0.66):
    visits = get_visits(field_name(tract), filt)
    dmap = hv.DynamicMap(partial(description_layout, descriptions=descriptions, butler=butler, tract=tract, filt=filt, kind='visit', scale=scale), 
                     kdims=['visit', 'style'])
    dmap = dmap.redim.values(visit=visits, style=styles)
    return dmap

    