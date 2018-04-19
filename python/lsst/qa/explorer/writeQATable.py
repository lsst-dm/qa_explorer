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

import functools
import re, os
import pandas as pd

from .parquetTable import ParquetTable
from .catalog import MultilevelParquetCatalog
from .functors import CompositeFunctor, RAColumn, DecColumn

# Question: is there a way that LSST packages store data files?
ROOT = os.path.abspath(os.path.dirname(__file__))

class PostprocessCatalogTask(CmdLineTask):
    """Base class for postprocessing calculations on catalogs

    """    

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
        df = self.doCalculations(patchRef)
        self.write(df, patchRef)
        return df

    def doCalculations(self, dataRef):
        """Do postprocessing calculations

        Takes a dataRef pointing to deepCoadd_obj; 
        returns a dataframe with results of postprocessing calculations.

        Parameters
        ----------
        dataRef : data reference

        Return
        ------
        df : `pandas.DataFrame`

        """
        raise NotImplementedError('Subclasses must implement doCalculations')

    def write(self, df, parqRef):
        parqRef.put(ParquetTable(dataFrame=df), self.outputDataset)

    def writeMetadata(self, dataRef):
        """No metadata to write.
        """
        pass

class WriteQATableConfig(Config):
    coaddName = Field(dtype=str, default="deep", doc="Name of coadd")

class WriteQATableTask(PostprocessCatalogTask):
    """Compute columns of QA interest from coadd object tables

    This computes columns based on a YAML specification file,
    `functors.yaml`, which lives next to the code.  The specification

    """
    _DefaultName = "writeQATable"
    ConfigClass = WriteQATableConfig

    inputDataset = 'deepCoadd_obj'
    outputDataset = 'deepCoadd_qa'

    def doCalculations(self, parqRef):
        parq = parqRef.get()
        funcs = CompositeFunctor.from_yaml(os.path.join(ROOT, 'functors.yaml'))
        funcs.funcDict.update({'ra':RAColumn(), 'dec':DecColumn()})
        dfDict = {}
        for filt in parq.columnLevelNames['filter']:
            catalog = MultilevelParquetCatalog(parq, filt=filt)
            df = funcs(catalog)
            df['patchId'] = parqRef.dataId['patch']
            dfDict[filt] = df

        # This makes a multilevel column index, with filter as first level
        df = pd.concat(dfDict, axis=1, names=['filter', 'column'])
        return df


