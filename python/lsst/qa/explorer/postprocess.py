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

    This object manages and organizes an arbitrary set of computations
    on a catalog.  The catalog is defined by a
    `lsst.qa.explorer.parquetTable.ParquetTable` object (or list thereof), such as a
    `deepCoadd_obj` dataset, and the computations are defined by a collection
    of `lsst.qa.explorer.functor.Functor` objects (or, equivalently,
    a `CompositeFunctor`).

    After the object is initialized, accessing the `.df` attribute (which
    holds the `pandas.DataFrame` containing the results of the calculations) triggers
    computation of said dataframe.

    One of the conveniences of using this object is the ability to define a desired common
    filter for all functors.  This enables the same functor collection to be passed to
    several different `PostprocessAnalysis` objects without having to change the original
    functor collection, since the `filt` keyword argument of this object triggers an
    overwrite of the `filt` property for all functors in the collection.

    This object also allows a list of flags to be passed, and defines a set of default
    flags that are always included even if not requested.

    If a list of `ParquetTable` object is passed, rather than a single one, then the
    calculations will be mapped over all the input catalogs.  In principle, it should
    be straightforward to parallelize this activity, but initial tests have failed
    (see TODO in code comments).

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
                        doc='Path to YAML file specifying functors to be computed',
                        default=None)


class PostprocessTask(CmdLineTask):
    """Base class for postprocessing calculations on coadd catalogs

    The purpose of this task is to perform a set of computations on
    an input `ParquetTable` dataset (such as `deepCoadd_obj`) and write the
    results to a new dataset (which needs to be declared in an `outputDataset`
    attribute).

    The calculations to be performed are defined in a YAML file that specifies
    a set of functors to be computed, provided as
    a `--functorFile` config parameter.  An example of such a YAML file
    is the following:

        funcs:
            psfMag:
                functor: Mag
                args:
                    - base_PsfFlux
                filt: HSC-G
                dataset: meas
            cmodel_magDiff:
                functor: MagDiff
                args:
                    - modelfit_CModel
                    - base_PsfFlux
                filt: HSC-G
            gauss_magDiff:
                functor: MagDiff
                args:
                    - base_GaussianFlux
                    - base_PsfFlux
                filt: HSC-G
            count:
                functor: Column
                args:
                    - base_InputCount_value
                filt: HSC-G
            deconvolved_moments:
                functor: DeconvolvedMoments
                filt: HSC-G
                dataset: forced_src
        flags:
            - calib_psfUsed
            - merge_measurement_i
            - merge_measurement_r
            - merge_measurement_z
            - merge_measurement_y
            - merge_measurement_g
            - base_PixelFlags_flag_inexact_psfCenter
            - detect_isPrimary

    The names for each entry under "func" will become the names of columns in the
    output dataset.  All the functors referenced are defined in `lsst.qa.explorer.functors`.
    Positional arguments to be passed to each functor are in the `args` list,
    and any additional entries for each column other than "functor" or "args" (e.g., `'filt'`,
    `'dataset'`) are treated as keyword arguments to be passed to the functor initialization.

    The "flags" entry is shortcut for a bunch of `Column` functors with the original column and
    taken from the `'ref'` dataset.

    Note, if `'filter'` is provided as part of the `dataId` when running this task (even though
    `deepCoadd_obj` does not use `'filter'`), then this will override the `filt` kwargs
    provided in the YAML file, and the calculations will be done in that filter.

    This task uses the `lsst.qa.explorer.postprocess.PostprocessAnalysis` object
    to organize and excecute the calculations.


    """
    _DefaultName = "Postprocess"
    ConfigClass = PostprocessConfig

    inputDataset = 'deepCoadd_obj'

    @property
    def outputDataset(self):
        raise NotImplementedError('Subclass must define "outputDataset" attribute')

    @classmethod
    def _makeArgumentParser(cls):
        """Create a suitable ArgumentParser.

        """
        parser = ArgumentParser(name=cls._DefaultName)
        parser.add_id_argument("--id", cls.inputDataset,
                               ContainerClass=ExistingCoaddDataIdContainer,
                               help="data ID, e.g. --id tract=12345 patch=1,2")
        return parser


    def runDataRef(self, patchRef):
        """Do calculations; write result
        """
        parq = patchRef.get()
        dataId = patchRef.dataId
        df = self.run(parq, dataId)
        self.write(df, patchRef)
        return df

    def run(self, parq, dataId):
        """Do postprocessing calculations

        Takes a `ParquetTable` object and dataId,
        returns a dataframe with results of postprocessing calculations.

        Parameters
        ----------
        parq : `lsst.qa.explorer.parquetTable.ParquetTable`
            ParquetTable from which calculations are done.

        dataId : dict
            Used to add a `patchId` column to the output dataframe.

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
    """Do the same set of postprocessing calculations on all bands

    This is identical to `PostprocessTask`, except for that it does the
    specified functor calculations for all filters present in the
    input `deepCoadd_obj` table.  Any specific `"filt"` keywords specified
    by the YAML file will be superceded.
    """

    def run(self, parq, dataId):
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

    Only difference from MultibandPostprocessTask is the default
    YAML functor file.
    """
    _DefaultName = "writeQATable"
    ConfigClass = WriteQATableConfig

    inputDataset = 'deepCoadd_obj'
    outputDataset = 'deepCoadd_qa'