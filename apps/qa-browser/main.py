import os
from pkg_resources import resource_filename
import yaml
import numpy as np
import holoviews as hv

import logging

from bokeh.application.handlers import FunctionHandler
from bokeh.application import Application
from bokeh.io import show, curdoc
from bokeh.layouts import layout, gridplot
from bokeh.models import Slider, Button, TextInput
from bokeh.models.widgets import Panel, Tabs, RadioButtonGroup

from explorer.static import get_plot, get_tracts
from explorer.static import filter_layout_dmap_coadd
from explorer.static import description_layout_dmap_visit
from explorer.static import color_dmap
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

def get_source_dmap(butler, category, tract=None, filt='HSC-I'):
    kws = get_kwargs('source', category, filt=filt)
    return description_layout_dmap_visit(butler, tract, **kws)

def get_source_dmaps(butler, tract=None, filt='HSC-I'):
    categories = config['sections']['source']
    d = {}
    for cat in categories:
        d[cat] = get_source_dmap(butler, cat, tract=tract, filt=filt)
    return d

def get_color_dmaps(butler):
    categories = config['sections']['color']
    d = {}
    for cat in categories:
        kws = get_kwargs('color', cat)
        d[cat] = color_dmap(butler, **kws)
    return d


# This is a list
object_dmaps = get_object_dmaps(butler) 

# These are dictionaries
source_dmaps = get_source_dmaps(butler)
color_dmaps = get_color_dmaps(butler)

def modify_doc(doc):
    repo_box = TextInput(value='/project/tmorton/DM-12873/w44', title='rerun',
                         css_classes=['customTextInput'])

    # Object plots
    object_hvplots = [renderer.get_widget(dmap, None, doc) for dmap in object_dmaps]

    object_plots = [layout([hvplot.state], sizing_mode='fixed') for hvplot in object_hvplots]
    object_tabs = Tabs(tabs=[Panel(child=plot, title=name) 
                            for plot,name in zip(object_plots, config['sections']['object'])])
    object_panel = Panel(child=object_tabs, title='Object Catalogs')

    # Source plots
    source_categories = config['sections']['source']
    # source_hvplots = {c : renderer.get_widget(source_dmaps[c], None, doc) 
    #                     for c in source_categories}

    # # source_plots = {c : layout([source_hvplots[c].state], sizing_mode='fixed') 
    # #                 for c in source_categories}
    # source_plots = {c : gridplot([[None]]) for c in source_categories}
    # source_tract_select = {c : RadioButtonGroup(labels=[str(t) for t in get_tracts(butler)], active=0)
    #                             for c in source_categories}
    # source_filt_select = {c : RadioButtonGroup(labels=wide_filters, active=2)
    #                             for c in source_categories}

    # def update_source(category):
    #     def update(attr, old, new):
    #         t_sel = source_tract_select[category]
    #         f_sel = source_filt_select[category]
    #         new_tract = int(t_sel.labels[t_sel.active])
    #         new_filt = f_sel.labels[f_sel.active]
    #         logging.info('updating {} to tract={}, filt={}...'.format(category, new_tract, new_filt))
    #         dmap = get_source_dmap(butler, category, tract=new_tract, filt=new_filt)
    #         new_hvplot = renderer.get_widget(dmap, None, doc)
    #         source_plots[category].children[0] = new_hvplot.state
    #         logging.info('update complete.')
    #     return update

    # source_tab_panels = []
    # for category in source_categories:
    #     tract_select = source_tract_select[category]
    #     filt_select = source_filt_select[category]
    #     plot = source_plots[category]

    #     tract_select.on_change('active', update_source(category))
    #     filt_select.on_change('active', update_source(category))

    #     l = layout([[tract_select, filt_select], [plot]], sizing_mode='fixed')
    #     source_tab_panels.append(Panel(child=l, title=category))


    # source_tabs = Tabs(tabs=source_tab_panels)
    # source_panel = Panel(child=source_tabs, title='Source Catalogs')

    # Color plots
    color_categories = config['sections']['color']
    color_hvplots = {c : renderer.get_widget(color_dmaps[c], None, doc) 
                        for c in color_categories}
    color_plots = {c : layout([color_hvplots[c].state], sizing_mode='fixed') 
                    for c in color_categories}

    color_tabs = Tabs(tabs = [Panel(child=color_plots[c], title=c) for c in color_categories])
    color_panel = Panel(child=color_tabs, title='Color')

    def update_repo(attr, old, new):
        global butler
        butler = Butler(new)
        logging.info('Changing butler to {}'.format(new))

        # Update Object plots
        logging.info('Updating object plots...')
        object_dmaps = get_object_dmaps(butler=butler)

        new_object_hvplots = [renderer.get_widget(dmap, None, doc) for dmap in object_dmaps]

        for plot,new_plot in zip(object_plots, new_object_hvplots):
            plot.children[0] = new_plot.state

        # Update Source plots
        # for c in source_categories:
        #     source_tract_select[c].labels = [str(t) for t in get_tracts(butler)]

        # # # THIS MUST BE FIXED.  PERHAPS SOURCE PLOTS SHOULD BE EMPTY UNTIL ACTIVATED
        # logging.info('Updating source plots...')
        # source_plots = {c : gridplot([[None]]) for c in source_categories}

        # source_dmaps = get_source_dmaps(butler=butler)
        # new_source_hvplots = {c : renderer.get_widget(source_dmaps[c], None, doc) 
        #                       for c in source_categories}
        # for plot,new_plot in zip(source_plots, new_source_hvplots):
        #     plot.children[0] = new_plot.state

        # Update Color plots
        logging.info('Updating color plots...')
        color_dmaps = get_color_dmaps(butler=butler)
        new_color_hvplots = {c : renderer.get_widget(color_dmaps[c], None, doc) 
                              for c in color_categories}
        for plot,new_plot in zip(color_plots, new_color_hvplots):
            plot.children[0] = new_plot.state


    repo_box.on_change('value', update_repo)

    # uber_tabs = Tabs(tabs=[object_panel, source_panel, color_panel])
    uber_tabs = Tabs(tabs=[object_panel, color_panel])
                           

    doc.add_root(repo_box)
    doc.add_root(uber_tabs)
    return doc


doc = modify_doc(curdoc()) 