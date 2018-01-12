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
from bokeh.models.widgets import Panel, Tabs, RadioButtonGroup

from explorer.static import get_plot
from explorer.static import filter_layout_dmap_coadd
from explorer.static import description_layout_dmap_visit
from explorer.rc import wide_filters

from lsst.daf.persistence import Butler

rerun44 = '/project/tmorton/DM-12873/w44'

butler = Butler(rerun44)
# butler46 = Butler(rerun46)

config_file = resource_filename('explorer', os.path.join('data',
                                              'browser_config.yaml'))

with open(config_file) as fin:
    config = yaml.load(fin)

renderer = hv.renderer('bokeh').instance(mode='server')

# stream = hv.streams.Stream.define('Butler', butler=butler44)()

def get_kwargs(section, category, default_styles=['psfMagHist', 'sky-stars', 'sky-gals'],
                scale=None, **kwargs):
    d = config[category]
    descriptions = d['descriptions']
    styles = default_styles if 'styles' not in d else d['styles']
    kws = {'descriptions' : descriptions, 'styles' : styles,
            'scale':scale}
    kws.update(kwargs)
    return kws

def get_object_dmaps(butler):
    categories = config['sections']['object']
    kwargs = [get_kwargs('object', cat) for cat in categories]
    return [filter_layout_dmap_coadd(butler=butler, **kws)
                 for cat, kws in zip(categories, kwargs)]

def get_source_dmaps(butler, tract=8766, filt='HSC-I'):
    categories = config['sections']['source']
    kwargs = [get_kwargs('source', cat, filt=filt) for cat in categories]
    return [description_layout_dmap_visit(butler, tract, **kws)
            for cat, kws in zip(categories, kwargs)]

object_dmaps = get_object_dmaps(butler)
source_dmaps = get_source_dmaps(butler)

def modify_doc(doc):
    repo_box = TextInput(value='/project/tmorton/DM-12873/w44', title='rerun',
                         css_classes=['customTextInput'])

    # Create HoloViews plot and attach the document
    object_hvplots = [renderer.get_widget(dmap, None, doc) for dmap in object_dmaps]
    source_hvplots = [renderer.get_widget(dmap, None, doc) for dmap in source_dmaps]

    object_plots = [layout([hvplot.state], sizing_mode='fixed') for hvplot in object_hvplots]
    object_tabs = Tabs(tabs=[Panel(child=plot, title=name) 
                            for plot,name in zip(object_plots, config['sections']['object'])])
    object_panel = Panel(child=object_tabs, title='Object Catalogs')

    source_plots = [layout([hvplot.state], sizing_mode='fixed') for hvplot in source_hvplots]
    source_tabs = Tabs(tabs=[Panel(child=plot, title=name) 
                            for plot,name in zip(source_plots, config['sections']['source'])])
    source_tract_select = RadioButtonGroup(labels=['8766', '8767', '9813'], active=0)
    source_filt_select = RadioButtonGroup(labels=wide_filters, active=2)
    source_layout = layout([[source_tract_select, source_filt_select], [source_tabs]], sizing_mode='fixed')
    source_panel = Panel(child=source_layout, title='Source Catalogs')

    def update_repo(attr, old, new):
        global butler
        butler = Butler(new)
        object_dmaps = get_object_dmaps(butler=butler)

        new_object_hvplots = [renderer.get_widget(dmap, None, doc) for dmap in object_dmaps]

        for plot,new_plot in zip(object_plots, new_object_hvplots):
            plot.children[0] = new_plot.state

        update_source(attr, old, new)
        
    def update_source(attr, old, new):
        new_tract = int(source_tract_select.labels[source_tract_select.active])
        new_filt = int(source_filt_select.labels[source_filt_select.active])
        source_dmaps = get_source_dmaps(butler=butler, tract=new_tract, filt=new_filt)
        new_source_hvplots = [renderer.get_widget(dmap, None, doc) for dmap in source_dmaps]
        for plot,new_plot in zip(source_plots, new_source_hvplots):
            plot.children[0] = new_plot.state


    repo_box.on_change('value', update_repo)
    source_tract_select.on_change('active', update_source)
    source_filter_select.on_change('active', update_source)

    uber_tabs = Tabs(tabs=[object_panel, source_panel])
                           

    doc.add_root(repo_box)
    doc.add_root(uber_tabs)
    return doc


doc = modify_doc(curdoc()) 