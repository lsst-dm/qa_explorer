import nbformat as nbf

def hv_import_cell(**kwargs):
    """Cell to import holoviews and setup notebook extension
    """
    code = """\
import holoviews as hv
hv.notebook_extension('bokeh')"""
    return nbf.v4.new_code_cell(code)
    
def butler_init_cell(**kwargs):
    """Cell to initialize butler
    
    Parameters
    ----------
    repo : str
        Data repository root directory
    """
    if 'repo' not in kwargs:
        raise ValueError('Must pass repo')
        
    code = """\
from lsst.daf.persistence import Butler
butler = Butler('{repo}')""".format(**kwargs)
    return nbf.v4.new_code_cell(code)
    
def define_coadd_catalog_cell(**kwargs):
    """Define catalog
    
    Parameters
    ----------
    tract : int
    filt : str
    """
    if 'tract' not in kwargs or 'filt' not in kwargs:
        raise ValueError('Must pass tract and filt')
        
    code = """\
from explorer.catalog import CoaddCatalog, VisitCatalog, MultiMatchedCatalog
from explorer.utils import get_visits

tract = {tract}
filt = "{filt}"
dataId = {{'tract':tract, 'filter':filt}}
catalog = CoaddCatalog(butler, dataId)""".format(**kwargs)
    return nbf.v4.new_code_cell(code)

def define_functors_cell(**kwargs):
    """A generic examples of functors to define
    """
    code = """\
from explorer.functors import MagDiff, Seeing, DeconvolvedMoments, Column

magdiff = MagDiff('modelfit_CModel', 'base_PsfFlux')
magdiff_gauss = MagDiff('base_GaussianFlux', 'base_PsfFlux')

funcs = {'cmodel':magdiff, 'gauss':magdiff_gauss, 'count': Column('base_InputCount_value'), 'seeing':Seeing()}"""
    return nbf.v4.new_code_cell(code)
    
def define_dataset_cell(**kwargs):
    code = """\
from explorer.dataset import QADataset

flags = ['calib_psfUsed', 'qaBad_flag',
 'merge_measurement_i',
 'merge_measurement_r',
 'merge_measurement_z',
 'merge_measurement_y',
 'merge_measurement_g',
 'base_Footprint_nPix_flag',
 'base_PixelFlags_flag_inexact_psfCenter']

data = QADataset(catalog, funcs, flags=flags)"""
    return nbf.v4.new_code_cell(code)

def calculate_df_cell(**kwargs):
    code = """\
# Calculate dataframe; see how long it takes.
%time data.df.head()"""
    return nbf.v4.new_code_cell(code)
    
def multi_scattersky_cell(**kwargs):
    code = """\
from explorer.plots import FilterStream, multi_scattersky
filter_stream = FilterStream()
multi_scattersky(data.ds, filter_stream=filter_stream, width=900, height=300)"""
    return nbf.v4.new_code_cell(code)

def flagsetter_cell(**kwargs):
    code ="""\
from explorer.plots import FlagSetter
import parambokeh

flag_setter = FlagSetter(filter_stream=filter_stream, flags=data.flags, bad_flags=data.flags)
parambokeh.Widgets(flag_setter, callback=flag_setter.event, push=False, on_init=True)"""
    return nbf.v4.new_code_cell(code)

def coadd_explore_cell(**kwargs):
    code = """\
%%output max_frames=10000
%%opts Points [width=500, height=500, tools=['hover'], colorbar=True] (cmap='coolwarm', size=4)

coadd_dmap = data.coadd_explore('seeing', filter_stream=filter_stream)
tap = hv.streams.Tap(source=coadd_dmap, rename={'x':'ra', 'y':'dec'})

coadd_dmap"""
    return nbf.v4.new_code_cell(code)

def coadd_ginga_cell(**kwargs):
    code = """\
import lsst.afw.display
lsst.afw.display.setDefaultBackend("ginga")

from explorer.display import CoaddDisplay
coadd_display = CoaddDisplay(butler, filt, dims=(500,500))
coadd_display.connect_tap(tap)
coadd_display.embed()"""
    return nbf.v4.new_code_cell(code)

class QANotebook(object):
    cells = []
    
    def __init__(self, **params):
        self.nb = nbf.v4.new_notebook()
        self.params = params

        self.generate_cells()
        
    def generate_cells(self):
        for cell in self.cells:
            self.nb.cells.append(cell(**self.params))

    def write(self, filename):
        with open(filename, 'w') as f:
            nbf.write(self.nb, f)
            
class Coadd_QANotebook(QANotebook):
    cells = [hv_import_cell, butler_init_cell,
            define_coadd_catalog_cell, define_functors_cell,
            define_dataset_cell, calculate_df_cell,
            multi_scattersky_cell, coadd_explore_cell,
            coadd_ginga_cell]
    
    def __init__(self, repo, tract, filt):
        params = dict(repo=repo, tract=tract, filt=filt)
        super(Coadd_QANotebook, self).__init__(**params)

        