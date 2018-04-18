"""Command-line task and associated config for consolidating QA tables.


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
from lsst.coadd.utils import TractDataIdContainer

from collections import defaultdict
import functools
import re, os
import pandas as pd

from .parquetTable import ParquetTable
from .tractQADataIdContainer import TractQADataIdContainer

# Question: is there a way that LSST packages store data files?
ROOT = os.path.abspath(os.path.dirname(__file__))

class ConsolidateQATableConfig(Config):
    coaddName = Field(dtype=str, default="deep", doc="Name of coadd")

class ConsolidateQATableTask(CmdLineTask):
    """Write filter-merged source tables to parquet
    """
    _DefaultName = "consolidateQATable"
    ConfigClass = ConsolidateQATableConfig

    inputDataset = 'deepCoadd_qa'
    outputDataset = 'deepCoadd_qa_tract'

    @classmethod
    def _makeArgumentParser(cls):
        parser = ArgumentParser(name=cls._DefaultName)

        parser.add_id_argument("--id", cls.inputDataset,
                               help="data ID, e.g. --id tract=12345",
                               ContainerClass=TractDataIdContainer)
        return parser

    def run(self, patchRefList):
        df = pd.concat([patchRef.get().to_df() for patchRef in patchRefList])
        patchRefList[0].put(ParquetTable(dataFrame=df), self.outputDataset)

    def writeMetadata(self, dataRef):
        """No metadata to write.
        """
        pass
