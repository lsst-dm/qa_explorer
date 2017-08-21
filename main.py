import os, glob
import holoviews as hv
import pandas as pd
import parambokeh
from bokeh.io import curdoc
from explorer.explorer import QAExplorer

from bokeh.models.widgets import Button, TextInput

hv.extension('bokeh')

# catalog = pd.read_hdf('data/forced_big.h5')

explorer = QAExplorer()
explorer.output = explorer.view()

doc = parambokeh.Widgets(explorer, continuous_update=False, callback=explorer.event, 
                        view_position='right', mode='server')


def modify_doc(doc):

    name_input = TextInput(value='selected', title="Save IDs as")
    save_button = Button(label='Save IDs', width=100)
    clear_button = Button(label='Clear saved IDs', width=100)

    def write_selected():
        return explorer.write_selected(name_input.value)

    def clear_ids(id_dir='data/ids'):
        for f in glob.glob('{}/*.h5'.format(id_dir)):
            os.remove(f)

    save_button.on_click(write_selected)
    clear_button.on_click(clear_ids)

    doc.add_root(name_input)
    doc.add_root(save_button)
    doc.add_root(clear_button)

    return doc

doc = modify_doc(curdoc()) 