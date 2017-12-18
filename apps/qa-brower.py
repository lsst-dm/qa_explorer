import numpy as np
import holoviews as hv

from bokeh.application.handlers import FunctionHandler
from bokeh.application import Application
from bokeh.io import show
from bokeh.layouts import layout
from bokeh.models import Slider, Button

from explorer.static import filter_layout_dmap_coadd

from lsst.daf.persistence import Butler

rerun44 = '/project/tmorton/DM-12873/w44'
rerun46 = '/project/tmorton/DM-12873/w46'

butler44 = Butler(rerun44)
butler46 = Butler(rerun46)

butler = butler44

renderer = hv.renderer('bokeh').instance(mode='server')

descriptions = ['mag_modelfit_CModel', 'mag_base_GaussianFlux', 'mag_ext_photometryKron_KronFlux']
dmap = filter_layout_dmap_coadd(butler, descriptions=descriptions)

def modify_doc(doc):
    # Create HoloViews plot and attach the document
    hvplot = renderer.get_plot(dmap, doc)

    plot = layout([hvplot.state], sizing_mode='fixed')
    
    doc.add_root(plot)
    return doc


doc = modify_doc(curdoc()) 