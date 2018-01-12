import numpy as np

import holoviews as hv

from bokeh.io import curdoc
from bokeh.layouts import layout
from bokeh.models.widgets import Panel, Tabs

import holoviews as hv
renderer = hv.renderer('bokeh').instance(mode='server')

xvals = np.linspace(-4,0,202)
yvals = np.linspace(4,0,202)
xs,ys = np.meshgrid(xvals, yvals)

def waves_image(alpha, beta):
    return hv.Image(np.sin(((ys/alpha)**alpha+beta)*xs))#.opts(plot={'width':640, 'height':480})

def waves_layout(alpha, beta):
    images = [waves_image(alpha, beta) for i in range(6)]
    return hv.Layout(images).cols(3)

n_dmaps = 4

dmaps = [hv.DynamicMap(waves_layout, kdims=['alpha', 'beta']).redim.range(alpha=(0.,5), beta=(1,5)) 
            for i in range(n_dmaps)]

n_dmaps2 = 2

dmaps2 = [hv.DynamicMap(waves_layout, kdims=['alpha', 'beta']).redim.range(alpha=(0.,5), beta=(1,5)) 
            for i in range(n_dmaps2)]

def modify_doc(doc):
    # Create HoloViews plot and attach the document
    # hvplot = renderer.get_widget(dmap, None, doc)
    n_tabs = 3
    hvplots = [renderer.get_widget(dmap, None, doc) for dmap in dmaps]
    hvplots2 = [renderer.get_widget(dmap, None, doc) for dmap in dmaps2]

    plots = [layout([hvplot.state], sizing_mode='fixed') for hvplot in hvplots]
    plots2 = [layout([hvplot.state], sizing_mode='fixed') for hvplot in hvplots2]

    tabs = Tabs(tabs=[Panel(child=plot, title='{}'.format(i))
                 for i,plot in enumerate(plots)])
    tabs2 = Tabs(tabs=[Panel(child=plot, title='{}'.format(i))
                 for i,plot in enumerate(plots2)])

    uber_tabs = Tabs(tabs=[Panel(child=t, title='{}'.format(i))
                           for i,t in enumerate([tabs, tabs2])])
    doc.add_root(uber_tabs)
    return doc


doc = modify_doc(curdoc()) 