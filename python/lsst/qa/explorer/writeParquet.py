from lsst.daf.persistence.butler import Butler
from lsst.pex.config import (Config, Field, ConfigField, ListField, DictField, ConfigDictField,
                             ConfigurableField)
from lsst.pipe.base import Task, CmdLineTask, ArgumentParser, TaskRunner, TaskError
from lsst.coadd.utils import TractDataIdContainer
from lsst.pipe.tasks.multiBand import MergeSourcesTask, MergeSourcesConfig
from lsst.pipe.tasks.multiBand import _makeGetSchemaCatalogs

import functools
import re

from .table import ParquetTable


def filter_tag(filt):
    return re.sub('[^a-zA-Z0-9_]+', '', filt).lower()

class WriteObjectTableConfig(MergeSourcesConfig):
    priorityList = ListField(dtype=str, default=['HSC-G', 'HSC-R', 'HSC-I', 'HSC-Z', 'HSC-Y'],
                             doc="Priority-ordered list of bands for the merge.")    

class WriteObjectTableTask(MergeSourcesTask):
    """Write filter-merged source tables to parquet
    """
    _DefaultName = "writeObjectTable"
    ConfigClass = WriteObjectTableConfig

    inputDatasets = ('forced_src', 'meas', 'ref')
    outputDataset = 'obj'

    # Hack, since we're not using this...
    getSchemaCatalogs = lambda x : {}

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
            catalog = patchRef.get(self.config.coaddName + "Coadd_" + self.inputDataset, immediate=True)
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

                # Add filter tag name to every column
                tag = filter_tag(filt) + '_' + dataset
                df = df.rename(columns={c : tag + '_' + c for c in df.columns})
                dfs.append(df)

        catalog = functools.reduce(lambda d1,d2 : d1.join(d2), dfs)
        return ParquetTable(catalog)

class Write