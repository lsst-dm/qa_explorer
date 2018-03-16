from lsst.daf.persistence.butler import Butler
from lsst.pex.config import (Config, Field, ConfigField, ListField, DictField, ConfigDictField,
                             ConfigurableField)
from lsst.pipe.base import Task, CmdLineTask, ArgumentParser, TaskRunner, TaskError
from lsst.coadd.utils import TractDataIdContainer
from lsst.pipe.tasks.multiBand import MergeSourcesTask

from .table import ParquetTable

class WriteObjectTableConfig(Config):
    pass


class WriteObjectTableRunner(TaskRunner):
    pass


class WriteObjectTableTask(MergeSourcesTask):
    """Convert all source tables to parquet format
    """
    _DefaultName = "writeObjectTable"
    ConfigClass = WriteObjectTableConfig
    RunnerClass = 

    inputDataset = 'forced_src'
    outputDataset = 'obj'


    def write(self, patchRef, catalog):
        df = catalog.asAstropy().to_pandas()
        df = df.set_index('id', drop=True)
        table = ParquetTable(df)
        patchRef.put(table, self.config.coaddName + "Coadd_" + self.outputDataset)

        # since the filter isn't actually part of the data ID for the dataset we're saving,
        # it's confusing to see it in the log message, even if the butler simply ignores it.        
        mergeDataId = patchRef.dataId.copy()
        del mergeDataId["filter"]
        self.log.info("Wrote merged {} catalog to parquet: {}".format(self.inputDataset, mergeDataId))