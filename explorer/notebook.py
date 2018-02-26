import nbformat as nbf
import re

DEFAULT_FLAGS = ['calib_psfUsed', 'qaBad_flag',
                 'merge_measurement_i',
                 'merge_measurement_r',
                 'merge_measurement_z',
                 'merge_measurement_y',
                 'merge_measurement_g',
                 'base_Footprint_nPix_flag',
                 'base_PixelFlags_flag_inexact_psfCenter']

DEFAULT_FUNCTORS = (('cmodel', "MagDiff('modelfit_CModel', 'base_PsfFlux')"),
                ('gauss', "MagDiff('base_GaussianFlux', 'base_PsfFlux')"),
                ('count', "Column('base_InputCount_value')"),
                ('seeing', "Seeing()"))

DEFAULT_MATCHED_FUNCTORS = (('gauss', "MagDiff('base_GaussianFlux', 'base_PsfFlux')"),
                            ('seeing', "Seeing()"))


DEFAULT_COLOR_FUNCTORS = (('psfmag', "Mag('base_PsfFlux')"),
                          ('cmodel', "Mag('modelfit_CModel')"),
                          ('kron',  "Mag('ext_photometryKron_KronFlux')"))

DEFAULT_COLOR_FLAGS = ['calib_psfUsed', 'qaBad_flag', 'base_PixelFlags_flag_inexact_psfCenter']

HSC_FILTERS = ['HSC-G', 'HSC-R', 'HSC-I', 'HSC-Z', 'HSC-Y']
HSC_SHORT_FILTERS = 'GRIZY'
REFERENCE_FILTER = 'HSC-I'


class Cell(object):
    params = []
    code = ''
    def __call__(self, **kwargs):
        for p in self.params:
            if p not in kwargs:
                raise ValueError('Must provide {}'.format(p))
        return nbf.v4.new_code_cell(self.code.format(**kwargs))

class CodeCell(Cell):
    def __init__(self, code):
        self.code = code


class HvImportCell(Cell):
    code = """\
import holoviews as hv
hv.notebook_extension('bokeh')"""

class ButlerInitCell(Cell):
    params = ('repo',)
    code = """\
from lsst.daf.persistence import Butler
butler = Butler('{repo}')"""

class DaskClientCell(Cell):
    def __init__(self, scheduler_file):
        self.scheduler_file = scheduler_file
        
    @property
    def code(self):
        code = 'from distributed import Client\n'
        code += 'client = Client(scheduler_file={})'.format(self.scheduler_file)
        return code
    
class DefineCoaddCatalogCell(Cell):
    params = ('tract', 'filt')
    code = """\
from explorer.catalog import CoaddCatalog, VisitCatalog, MultiMatchedCatalog
from explorer.utils import get_visits

tract = {tract}
filt = "{filt}"
dataId = {{'tract':tract, 'filter':filt}}
catalog = CoaddCatalog(butler, dataId)"""

class DefineMatchedCatalogCell(Cell):
    params = ('tract', 'filt')
    code = """\
from explorer.catalog import CoaddCatalog, VisitCatalog, MultiMatchedCatalog
from explorer.utils import get_visits

tract = {tract}
filt = "{filt}"
dataId = {{'tract':tract, 'filter':filt}}
coaddCat = CoaddCatalog(butler, dataId)
visitCats = [VisitCatalog(butler, {{'tract': tract, 'filter':filt, 'visit':v}}, name=v) for v in get_visits(butler, tract, filt)]
catalog = MultiMatchedCatalog(coaddCat, visitCats, match_registry='QAmatchRegistry.h5')"""
    
class DefineColorCatalogCell(Cell):
    params = ('tract', 'filters', 'short_filters', 'reference_filter')
    code = """\
from explorer.catalog import CoaddCatalog, MultiBandCatalog

tract = 9615
filts = {filters}
short_filts = '{short_filters}'
reference_filt = '{reference_filter}'

catalog = MultiBandCatalog({{filt: CoaddCatalog(butler, {{'tract':tract, 'filter':filt}}, name=filt) for filt in filts}}, 
                           short_filters=short_filts, reference_filt=reference_filt)"""


class DefineFunctorsCell(Cell):
        
    def __init__(self, functors=DEFAULT_FUNCTORS):
        self.functors = functors
        
    @property
    def import_command(self):
        to_import = set()
        for k, v in self.functors:
            m = re.search('(\w+)\(', v)
            to_import.add(m.group(1))
        return 'from explorer.functors import ' + ','.join(to_import) + '\n'
    
    @property
    def code(self):
        code = self.import_command
        for k, v in self.functors:
            code += '{} = {}\n'.format(k,v)
        
        code += '\nfuncs = {{'
        for k, v in self.functors:
            code += "'{0}':{0},".format(k)
        code += '}}'
        return code
    
    
class DefineDatasetCell(Cell):
    def __init__(self, flags=DEFAULT_FLAGS, client=False):
        self.flags = flags
        self.client = client
    
    @property
    def code(self):
        code = "from explorer.dataset import QADataset\n"
        code += "flags = [" + ',\n'.join(["'{}'".format(f) for f in self.flags]) + ']\n\n'
        if self.client:
            code += "data = QADataset(catalog, funcs, flags=flags, client=client)"
        else:
            code += "data = QADataset(catalog, funcs, flags=flags)"
        return code

class CalculateDFCell(Cell):
    code = """\
# Calculate dataframe; see how long it takes.
%time data.df.head()"""
    
class MultiScatterskyCell(Cell):
    code = """\
from explorer.plots import FilterStream, multi_scattersky
filter_stream = FilterStream()
multi_scattersky(data.ds, filter_stream=filter_stream, width=900, height=300)"""
    
class FlagSetterCell(Cell):
    code ="""\
from explorer.plots import FlagSetter
import parambokeh

flag_setter = FlagSetter(filter_stream=filter_stream, flags=data.flags, bad_flags=data.flags)
parambokeh.Widgets(flag_setter, callback=flag_setter.event, push=False, on_init=True)"""

class ExploreCell(Cell):
    def __init__(self, dimension):
        self.dimension = dimension

    @property
    def prefix(self):
        raise NotImplementedError('Must define prefix for cell type')
    
    @property
    def code(self):
        code = """\
%%output max_frames=10000
%%opts Points [width=500, height=500, tools=['hover'], colorbar=True] (cmap='coolwarm', size=4)

# Change dimension to whichever you would like to explore
dimension = '{1}'
{0}_dmap = data.{0}_explore(dimension, filter_stream=filter_stream).relabel(dimension)
tap = hv.streams.Tap(source={0}_dmap, rename={{{{'x':'ra', 'y':'dec'}}}})

{0}_dmap""".format(self.prefix, self.dimension)
        return code
    
class CoaddExploreCell(ExploreCell):
    prefix = 'coadd'

class VisitExploreCell(ExploreCell):
    prefix = 'visit'
    
class GingaCell(Cell):
    @property
    def prefix(self):
        raise NotImplementedError('Must define prefix for cell type')

    @property
    def code(self):
        code = """\
import lsst.afw.display
lsst.afw.display.setDefaultBackend("ginga")

from explorer.display import {1}Display
{0}_display = {2}
{0}_display.connect_tap(tap)
{0}_display.embed()""".format(self.prefix, self.prefix.capitalize(), self.displayInit)
        return code
        
class CoaddGingaCell(GingaCell):
    prefix = 'coadd'
    displayInit = 'CoaddDisplay(butler, filt, dims=(500,500))'

class VisitGingaCell(GingaCell):
    prefix = 'visit'
    displayInit = 'VisitDisplay(butler, filt, tract, dims=(500,500))'
    
class CommentCell(Cell):
    def __init__(self, comment):
        self.comment = comment
        
    @property
    def code(self):
        return "# {}".format(self.comment)


class QANotebook(object):
    def __init__(self, repo, flags=DEFAULT_FLAGS, functors=DEFAULT_FUNCTORS, client=False, 
                 scheduler_file=None, **params):
        self.nb = nbf.v4.new_notebook()
        self.params = params
        self.params.update({'repo':repo})

        self.flags = flags
        self.functors = functors
        self.client = client
        self.scheduler_file = scheduler_file
                
    @property
    def setup_cells(self):
        cells = [HvImportCell(), ButlerInitCell()]
        if self.client:
            cells.append(DaskClientCell(scheduler_file=self.scheduler_file))
        return cells

    @property
    def define_catalog_cell(self):
        return CommentCell('Define `catalog` here`')
    
    @property
    def definition_cells(self):
        return [self.define_catalog_cell,
                DefineFunctorsCell(functors=self.functors),
                DefineDatasetCell(client=self.client, flags=self.flags), CalculateDFCell()]

    @property
    def plotting_cells(self):
        return [MultiScatterskyCell(), FlagSetterCell()]
    
    @property
    def cells(self):
        return self.setup_cells + self.definition_cells + self.plotting_cells
    
    def generate_cells(self):
        for cell in self.cells:
            self.nb.cells.append(cell(**self.params))

    def write(self, filename):
        self.generate_cells()        
        with open(filename, 'w') as f:
            nbf.write(self.nb, f)
            
class Coadd_QANotebook(QANotebook):
            
    def __init__(self, repo, tract, filt, **kwargs):
        kwargs.update(dict(tract=tract, filt=filt))
        super(Coadd_QANotebook, self).__init__(repo=repo, **kwargs)
        
    @property
    def define_catalog_cell(self):
        return DefineCoaddCatalogCell()

    @property
    def plotting_cells(self):
        return [MultiScatterskyCell(), FlagSetterCell(),
               CoaddExploreCell(self.functors[0][0]), CoaddGingaCell()]

class VisitMatch_QANotebook(QANotebook):
    def __init__(self, repo, tract, filt, functors=DEFAULT_MATCHED_FUNCTORS, **kwargs):
        kwargs.update(dict(tract=tract, filt=filt))
        super(VisitMatch_QANotebook, self).__init__(repo=repo, functors=functors, **kwargs)
        
    @property
    def define_catalog_cell(self):
        return DefineMatchedCatalogCell()
    
    @property
    def plotting_cells(self):
        return [MultiScatterskyCell(), FlagSetterCell(),
               VisitExploreCell(self.functors[0][0]), VisitGingaCell()]
    
class ColorColor_QANotebook(QANotebook):
    def __init__(self, repo, tract, 
                 filters=HSC_FILTERS, short_filters=HSC_SHORT_FILTERS, 
                 reference_filter=REFERENCE_FILTER, 
                 flags=DEFAULT_COLOR_FLAGS, functors=DEFAULT_COLOR_FUNCTORS, **kwargs):
        kwargs.update(dict(tract=tract, filters=filters, short_filters=short_filters,
                          reference_filter=reference_filter))
        super(ColorColor_QANotebook, self).__init__(repo=repo, flags=flags, functors=functors, **kwargs)
        
    @property
    def define_catalog_cell(self):
        return DefineColorCatalogCell()

    @property
    def plotting_cells(self):
        return [CodeCell('data.color_explore()'),
               CodeCell('%%opts Points [width=600, height=600]\ndata.color_fit_explore()')]