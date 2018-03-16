from lsst.daf.persistence.butler import Butler
from lsst.pex.config import (Config, Field, ConfigField, ListField, DictField, ConfigDictField,
                             ConfigurableField)
from lsst.pipe.base import Task, CmdLineTask, ArgumentParser, TaskRunner, TaskError
from lsst.coadd.utils import TractDataIdContainer
from lsst.pipe.tasks.multiBand import MergeSourcesTask
from lsst.pipe.tasks.multiBand import _makeGetSchemaCatalogs

import functools
import re

from .table import ParquetTable


def filter_tag(filt):
    return re.sub('[^a-zA-Z0-9_]+', '', filt).lower()

class WriteObjectTableTask(MergeSourcesTask):
    """Convert all source tables to parquet format
    """
    _DefaultName = "writeObjectTable"

    inputDataset = 'forced_src'
    outputDataset = 'obj'
    getSchemaCatalogs = lambda x : {}

    def mergeCatalogs(self, catalogs, patchRef):
        """Merge multiple catalogs.

        Parameters
        ----------
        catalogs : dict
            Mapping from filter names to catalogs.

        Returns
        -------
        catalog : lsst.qa.explorer.table.ParquetTable
            Merged dataframe, with each column prefixed by
            `filter_tag(filt)`, wrapped in the parquet writer shim class.
        """

        dfs = []
        for filt, table in catalogs.items():

            # Convert afwTable to pandas DataFrame
            df = table.asAstropy().to_pandas().set_index('id', drop=True)

            # Add filter tag name to every column
            tag = filter_tag(filt)
            df = df.rename(columns={c : tag + '_' + c for c in df.columns})
            dfs.append(df)

        catalog = functools.reduce(lambda d1,d2 : d1.join(d2), dfs)
        return ParquetTable(catalog)