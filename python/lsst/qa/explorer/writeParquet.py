from lsst.daf.persistence.butler import Butler
from lsst.pex.config import (Config, Field, ConfigField, ListField, DictField, ConfigDictField,
                             ConfigurableField)
from lsst.pipe.base import Task, CmdLineTask, ArgumentParser, TaskRunner, TaskError
from lsst.coadd.utils import TractDataIdContainer
from lsst.pipe.tasks.multiBand import MergeSourcesTask

import functools
import re

from .table import ParquetTable

class WriteObjectTableConfig(Config):
    pass


class WriteObjectTableRunner(TaskRunner):
    pass


def filter_tag(filt):
    return re.sub('[^a-zA-Z0-9_]+', '', filt).lower()

class WriteObjectTableTask(MergeSourcesTask):
    """Convert all source tables to parquet format
    """
    _DefaultName = "writeObjectTable"
    ConfigClass = WriteObjectTableConfig
    RunnerClass = WriteObjectTableRunner

    inputDataset = 'forced_src'
    outputDataset = 'obj'

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