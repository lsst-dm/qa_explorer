"""Command-line task and associated config for writing QA tables.

The deepCoadd_qa table is a table with QA columns of interest computed
for all filters for which the deepCoadd_obj tables are written.
"""
from lsst.daf.persistence.butler import Butler
from lsst.pex.config import (Config, Field, ConfigField, ListField, DictField, ConfigDictField,
                             ConfigurableField)
from lsst.pipe.base import Task, CmdLineTask, ArgumentParser, TaskRunner, TaskError
from lsst.coadd.utils import TractDataIdContainer
from lsst.pipe.tasks.multiBand import MergeSourcesTask, MergeSourcesConfig
from lsst.pipe.tasks.multiBand import _makeGetSchemaCatalogs
from lsst.coadd.utils.coaddDataIdContainer import ExistingCoaddDataIdContainer
from lsst.utils import getPackageDir

import functools
import re, os
import pandas as pd

from .parquetTable import ParquetTable
from .functors import CompositeFunctor, RAColumn, DecColumn, Column

class PostprocessAnalysis(object):
    """Calculate columns from ParquetTable

    Parameters
    ----------
    parq : `lsst.qa.explorer.ParquetTable` (or list of such)
        Source catalog(s) for computation

    functors : `list`, `dict`, or `lsst.qa.explorer.functors.CompositeFunctor`
        Computations to do (functors that act on `parq`).
        If a dict, the output
        DataFrame will have columns keyed accordingly.
        If a list, the column keys will come from the
        `.shortname` attribute of each functor.

    filt : `str` (optional)
        Filter in which to calculate.  If provided,
        this will overwrite any existing `.filt` attribute
        of the provided functors.

    flags : `list` (optional)
        List of flags to include in output table.
    """
    _defaultFlags = ('calib_psfUsed', 'detect_isPrimary')

    def __init__(self, parq, functors, filt=None, flags=None):
        self.parq = parq
        self.functors = functors

        self.filt = filt
        self.flags = list(self._defaultFlags)
        if flags is not None:
            self.flags += list(flags)

        self._df = None

    @property
    def func(self):
        additionalFuncs = {'ra' : RAColumn(),
                           'dec' : DecColumn(),}
        additionalFuncs.update({flag : Column(flag) for flag in self.flags})

        if isinstance(self.functors, CompositeFunctor):
            func = self.functors
        else:
            func = CompositeFunctor(self.functors)

        func.funcDict.update(additionalFuncs)
        func.filt = self.filt

        return func

    @property
    def df(self):
        if self._df is None:
            self.compute()
        return self._df

    def compute(self, dropna=False, pool=None):
        # map over multiple parquet tables
        if type(self.parq) in (list, tuple):
            if pool is None:
                dflist = [self.func(parq, dropna=dropna) for parq in self.parq]
            else:
                # TODO: Figure out why this doesn't work (pyarrow pickling issues?)
                dflist = pool.map(functools.partial(self.func, dropna=dropna), self.parq)
            self._df = pd.concat(dflist)
        else:
            self._df = self.func(self.parq, dropna=dropna)

        return self._df


class PostprocessConfig(Config):
    coaddName = Field(dtype=str, default="deep", doc="Name of coadd")
    functorFile = Field(dtype=str,
                        doc='Filename of YAML functor specification',
                        default=None)


class PostprocessTask(CmdLineTask):
    """Base class for postprocessing calculations on coadd catalogs

    """
    _DefaultName = "Postprocess"
    ConfigClass = PostprocessConfig

    inputDataset = 'deepCoadd_obj'

    @classmethod
    def _makeArgumentParser(cls):
        """Create a suitable ArgumentParser.

        """
        parser = ArgumentParser(name=cls._DefaultName)
        parser.add_id_argument("--id", cls.inputDataset,
                               ContainerClass=ExistingCoaddDataIdContainer,
                               help="data ID, e.g. --id tract=12345 patch=1,2")
        return parser


    def run(self, patchRef):
        """Do calculations; write result
        """
        parq = patchRef.get()
        dataId = patchRef.dataId
        df = self.doCalculations(parq, dataId)
        self.write(df, patchRef)
        return df

    def doCalculations(self, parq, dataId):
        """Do postprocessing calculations

        Takes a dataRef pointing to deepCoadd_obj;
        returns a dataframe with results of postprocessing calculations.

        Parameters
        ----------
        parq : `lsst.qa.explorer.parquetTable.ParquetTable`
            ParquetTable from which calculations are done.

        Return
        ------
        df : `pandas.DataFrame`

        """
        filt = dataId.get('filter', None)
        funcs = CompositeFunctor.from_yaml(self.config.functorFile)
        analysis = PostprocessAnalysis(parq, funcs, filt=filt)
        df = analysis.df
        df['patchId'] = dataId['patch']
        return df


    def write(self, df, parqRef):
        parqRef.put(ParquetTable(dataFrame=df), self.outputDataset)

    def writeMetadata(self, dataRef):
        """No metadata to write.
        """
        pass

class MultibandPostprocessTask(PostprocessTask):

    def doCalculations(self, parq, dataId):
        funcs = CompositeFunctor.from_yaml(self.config.functorFile)
        dfDict = {}
        for filt in parq.columnLevelNames['filter']:
            analysis = PostprocessAnalysis(parq, funcs, filt=filt)
            df = analysis.df
            df['patchId'] = dataId['patch']
            dfDict[filt] = df

        # This makes a multilevel column index, with filter as first level
        df = pd.concat(dfDict, axis=1, names=['filter', 'column'])
        return df

class WriteQATableConfig(PostprocessConfig):
    def setDefaults(self):
        self.functorFile = os.path.join(getPackageDir("qa_explorer"),
                                             'data','QAfunctors.yaml')

class WriteQATableTask(MultibandPostprocessTask):
    """Compute columns of QA interest from coadd object tables

    """
    _DefaultName = "writeQATable"
    ConfigClass = WriteQATableConfig

    inputDataset = 'deepCoadd_obj'
    outputDataset = 'deepCoadd_qa'
