import numpy as np
import holoviews as hv

from bokeh.application.handlers import FunctionHandler
from bokeh.application import Application
from bokeh.io import show, curdoc
from bokeh.layouts import layout
from bokeh.models import Slider, Button, TextInput
from bokeh.models.widgets import Panel, Tabs

import holoviews as hv

from explorer.static import filter_layout_dmap_coadd
from explorer.static import description_layout_dmap_visit

from lsst.daf.persistence import Butler

rerun44 = '/project/tmorton/DM-12873/w44'
rerun46 = '/project/tmorton/DM-12873/w46'

butler44 = Butler(rerun44)
butler46 = Butler(rerun46)

renderer = hv.renderer('bokeh').instance(mode='server')

descriptions1 = ['mag_modelfit_CModel', 'mag_base_GaussianFlux', 'mag_ext_photometryKron_KronFlux']
descriptions2 = ['e1ResidsHsm', 'e2ResidsHsm']

# stream = hv.streams.Stream.define('Butler', butler=butler44)()

dmap1 = filter_layout_dmap_coadd(butler=butler44, descriptions=descriptions1, scale=1.)
dmap2 = filter_layout_dmap_coadd(butler=butler44, descriptions=descriptions2, 
                                 styles=['psfMagHist'], scale=1.)

def modify_doc(doc):
    # Create HoloViews plot and attach the document
    hvplot1 = renderer.get_widget(dmap1, None, doc)
    hvplot2 = renderer.get_widget(dmap2, None, doc)

    repo_box = TextInput(value='/project/tmorton/DM-12873/w44', title='rerun',
                         css_classes=['customTextInput'])

    def update_repo(attr, old, new):
        butler = Butler(new)
        dmap = filter_layout_dmap_coadd(butler=butler, descriptions=descriptions)
        new_plot1 = renderer.get_widget(dmap1, None, doc)
        new_plot2 = renderer.get_widget(dmap2, None, doc)
        plot1.children[0] = new_plot1.state
        plot2.children[0] = new_plot2.state

    repo_box.on_change('value', update_repo)

    plot1 = layout([hvplot1.state], sizing_mode='fixed')
    plot2 = layout([hvplot2.state], sizing_mode='fixed')

    tab1 = Panel(child=plot1, title='tab1')
    tab2 = Panel(child=plot2, title='tab2')
    tabs = Tabs(tabs=[tab1, tab2])

    doc.add_root(repo_box)
    # doc.add_root(plot)
    doc.add_root(tabs)
    return doc


doc = modify_doc(curdoc()) 