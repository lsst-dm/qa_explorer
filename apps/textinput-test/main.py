from bokeh.io import curdoc
from bokeh.models import TextInput

from lsst.daf.persistence import Butler

rerun44 = '/project/tmorton/DM-12873/w44'
rerun46 = '/project/tmorton/DM-12873/w46'

butler44 = Butler(rerun44)

def modify_doc(doc):
    text_box = TextInput(value='this text is longer than 300px', title='input',
                         css_classes=['customTextInput'])

    doc.add_root(text_box)
    raise ValueError
    return doc


doc = modify_doc(curdoc()) 