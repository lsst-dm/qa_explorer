import os, glob
import holoviews as hv
import pandas as pd
import parambokeh
from bokeh.io import curdoc
from explorer.explorer import QAExplorer

from bokeh.models.widgets import Button, TextInput, PreText

hv.extension('bokeh')

def modify_doc(doc):

    explorer = QAExplorer(rootdir='.')
    explorer.output = explorer.view()

    doc = parambokeh.Widgets(explorer, continuous_update=False, callback=explorer.event, 
                        view_position='right', mode='server')


    name_input = TextInput(value='selected', title="Save IDs as")
    save_button = Button(label='Save IDs', width=100)
    clear_button = Button(label='Clear saved IDs', width=100)
    saved_ids = PreText(text=','.join(explorer.saved_ids))

    def save_selected():
        explorer.save_selected(name_input.value)
        saved_ids.text = ','.join(explorer.saved_ids)

    def clear_ids():
        for f in [os.path.join(explorer.id_path, '{}.h5'.format(i)) for i in explorer.saved_ids]:
            os.remove(f)
        saved_ids.txt = ''

    save_button.on_click(save_selected)
    clear_button.on_click(clear_ids)

    doc.add_root(name_input)
    doc.add_root(save_button)
    doc.add_root(saved_ids)
    doc.add_root(clear_button)

    return doc

doc = modify_doc(curdoc()) 