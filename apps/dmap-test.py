import numpy as np
import holoviews as hv

from bokeh.io import curdoc
from bokeh.layouts import layout

import holoviews as hv
renderer = hv.renderer('bokeh').instance(mode='server')

xvals = np.linspace(-4,0,202)
yvals = np.linspace(4,0,202)
xs,ys = np.meshgrid(xvals, yvals)

def waves_image(alpha, beta):
    return hv.Image(np.sin(((ys/alpha)**alpha+beta)*xs))

dmap = hv.DynamicMap(waves_image, kdims=['alpha', 'beta'])
dmap = dmap.redim.range(alpha=np.linspace(0,5.0,10), beta=np.linspace(1,5.0,10))

def modify_doc(doc):
    # Create HoloViews plot and attach the document
    hvplot = renderer.get_widget(dmap, None, doc)

    plot = layout([hvplot.state], sizing_mode='fixed')
    doc.add_root(plot)
    return doc


doc = modify_doc(curdoc()) 