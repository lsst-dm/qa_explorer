from bokeh.io import curdoc
from bokeh.models import TextInput

def modify_doc(doc):
    text_box = TextInput(value='this text is longer than 300px', title='input',
                         css_classes=['customTextInput'])

    doc.add_root(text_box)
    raise ValueError
    return doc


doc = modify_doc(curdoc()) 