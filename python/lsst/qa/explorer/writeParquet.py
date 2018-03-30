from lsst.daf.persistence.butler import Butler
from lsst.pex.config import (Config, Field, ConfigField, ListField, DictField, ConfigDictField,
                             ConfigurableField)
from lsst.pipe.base import Task, CmdLineTask, ArgumentParser, TaskRunner, TaskError
from lsst.coadd.utils import TractDataIdContainer
from lsst.pipe.tasks.multiBand import MergeSourcesTask, MergeSourcesConfig
from lsst.pipe.tasks.multiBand import _makeGetSchemaCatalogs
from lsst.coadd.utils.coaddDataIdContainer import ExistingCoaddDataIdContainer


import functools
import re
import pandas as pd

from .table import ParquetTable


class WriteObjectTableConfig(MergeSourcesConfig):
    priorityList = ListField(dtype=str, default=['HSC-G', 'HSC-R', 'HSC-I', 'HSC-Z', 'HSC-Y'],
                             doc="Priority-ordered list of bands for the merge.")    
    engine = Field(dtype=str, default="pyarrow", doc="Parquet engine for writing (pyarrow or fastparquet)")

class WriteObjectTableTask(MergeSourcesTask):
    """Write filter-merged source tables to parquet
    """
    _DefaultName = "writeObjectTable"
    ConfigClass = WriteObjectTableConfig

    inputDatasets = ('forced_src', 'meas', 'ref')
    outputDataset = 'obj'

    # Hack, since we're not using this...
    getSchemaCatalogs = lambda x : {}

    @classmethod
    def _makeArgumentParser(cls):
        """Create a suitable ArgumentParser.

        We will use the ArgumentParser to get a provide a list of data
        references for patches; the RunnerClass will sort them into lists
        of data references for the same patch. 

        References first of self.inputDatasets, rather than
        self.inputDataset (which parent class does.)
        """
        parser = ArgumentParser(name=cls._DefaultName)
        parser.add_id_argument("--id", "deepCoadd_" + cls.inputDatasets[0],
                               ContainerClass=ExistingCoaddDataIdContainer,
                               help="data ID, e.g. --id tract=12345 patch=1,2 filter=g^r^i")
        return parser


    def readCatalog(self, patchRef):
        """Read input catalogs

        Read all the input datasets provided by the 'inputDatasets'
        class variable.

        Parameters
        ----------
        patchRef : 
            Data reference for patch

        Returns
        -------
        Tuple consisting of filter name and a dict of catalogs, keyed by dataset
        name
        """
        filterName = patchRef.dataId["filter"]
        d = {}
        for dataset in self.inputDatasets:
            catalog = patchRef.get(self.config.coaddName + "Coadd_" + dataset, immediate=True)
            self.log.info("Read %d sources from %s for filter %s: %s" % (len(catalog), dataset, filterName, patchRef.dataId))
            d[dataset] = catalog
        return filterName, d

    def mergeCatalogs(self, catalogs, patchRef):
        """Merge multiple catalogs.

        Parameters
        ----------
        catalogs : dict
            Mapping from filter names to dict of catalogs.

        Returns
        -------
        catalog : lsst.qa.explorer.table.ParquetTable
            Merged dataframe, with each column prefixed by
            `filter_tag(filt)`, wrapped in the parquet writer shim class.
        """

        dfs = []
        for filt, tableDict in catalogs.items():
            for dataset, table in tableDict.items():
                # Convert afwTable to pandas DataFrame
                df = table.asAstropy().to_pandas().set_index('id', drop=True)

                # Sort columns by name, to ensure matching schema among patches
                df = df.reindex(sorted(df.columns), axis=1)

                # Make columns a 3-level MultiIndex
                df.columns = pd.MultiIndex.from_tuples([(dataset, filt, c) for c in df.columns], 
                                                       names=('dataset', 'filter', 'column'))
                dfs.append(df)

        catalog = functools.reduce(lambda d1,d2 : d1.join(d2), dfs)
        return catalog
