import os
from pkg_resources import resource_filename
import yaml
import numpy as np
import holoviews as hv

from bokeh.application.handlers import FunctionHandler
from bokeh.application import Application
from bokeh.io import show, curdoc
from bokeh.layouts import layout
from bokeh.models import Slider, Button, TextInput
from bokeh.models.widgets import Panel, Tabs

from explorer.static import filter_layout_dmap_coadd
from explorer.static import description_layout_dmap_visit

from lsst.daf.persistence import Butler

rerun44 = '/project/tmorton/DM-12873/w44'
rerun46 = '/project/tmorton/DM-12873/w46'

butler44 = Butler(rerun44)
# butler46 = Butler(rerun46)

config_file = resource_filename('explorer', os.path.join('data',
                                              'browser_config.yaml'))

with open(config_file) as fin:
    config = yaml.load(fin)

# stream = hv.streams.Stream.define('Butler', butler=butler44)()

def get_kwargs(section, category, default_styles=['psfMagHist', 'sky-stars', 'sky-gals']):
    d = config[section][category]
    print(d)
    descriptions = d['descriptions']
    print(descriptions)
    styles = default_styles if 'styles' not in d else d['styles']
    print(styles)
    return {'descriptions' : descriptions, 'styles' : styles}

def get_object_dmaps(butler):
    kwargs = get_kwargs('object', cat)
    print(kwargs)
    return [filter_layout_dmap_coadd(butler=butler, **kwargs)
                 for cat in config['object'].keys()]

object_dmaps = get_object_dmaps(butler44)


renderer = hv.renderer('bokeh').instance(mode='server')

def modify_doc(doc):
    repo_box = TextInput(value='/project/tmorton/DM-12873/w44', title='rerun',
                         css_classes=['customTextInput'])

    # Create HoloViews plot and attach the document
    object_hvplots = [renderer.get_widget(dmap, None, doc) for dmap in object_dmaps]

    def update_repo(attr, old, new):
        butler = Butler(new)
        object_dmaps = get_object_dmaps(butler=butler)
        new_object_plots = [renderer.get_widget(dmap, None, doc) for dmap in object_dmaps]
        for plot,new_plot in zip(object_plots, new_object_plots):
            plot.children[0] = new_plot.state

    repo_box.on_change('value', update_repo)

    object_plots = [layout([hvplot.state], sizing_mode='fixed') for hvplot in object_hvplots]
    object_tabs = Tabs(tabs=[Panel(child=plot, title=name) 
                            for plot,name in zip(object_plots, config['object'].keys())])

    doc.add_root(repo_box)
    # doc.add_root(plot)
    doc.add_root(object_tabs)
    return doc


doc = modify_doc(curdoc()) 