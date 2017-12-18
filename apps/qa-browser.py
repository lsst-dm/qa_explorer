import numpy as np
import holoviews as hv

from bokeh.application.handlers import FunctionHandler
from bokeh.application import Application
from bokeh.io import show, curdoc
from bokeh.layouts import layout
from bokeh.models import Slider, Button, TextInput

import holoviews as hv

from explorer.static import filter_layout_dmap_coadd

from lsst.daf.persistence import Butler

rerun44 = '/project/tmorton/DM-12873/w44'
rerun46 = '/project/tmorton/DM-12873/w46'

butler44 = Butler(rerun44)
butler46 = Butler(rerun46)

renderer = hv.renderer('bokeh').instance(mode='server')

descriptions = ['mag_modelfit_CModel', 'mag_base_GaussianFlux', 'mag_ext_photometryKron_KronFlux']

# stream = hv.streams.Stream.define('Butler', butler=butler44)()

dmap = filter_layout_dmap_coadd(butler=butler44, descriptions=descriptions)

def modify_doc(doc):
    # Create HoloViews plot and attach the document
    hvplot = renderer.get_widget(dmap, None, doc)

    repo_box = TextInput(value='/project/tmorton/DM-12873/w44', title='rerun', width=600)

    def update_repo(attr, old, new):
        butler = Butler(new)
        dmap = filter_layout_dmap_coadd(butler=butler, descriptions=descriptions)
        new_plot = renderer.get_widget(dmap, None, doc)
        l.children[1] = new_plot.state

    repo_box.on_change('value', update_repo)

    l = layout([[repo_box], [hvplot.state]], sizing_mode='fixed')
    doc.add_root(l)
    # doc.add_root(repo_box)
    # doc.add_root(hvplot.state)
    return doc


doc = modify_doc(curdoc()) 