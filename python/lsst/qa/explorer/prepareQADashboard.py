"""Command-line task and associated config for preparing data to be ingested by the QA dashboard.

Writes the following single parquet tables (potentially multi-part):



"""
import os
import pandas as pd
import functools

from lsst.pex.config import Config, Field
from lsst.pipe.base import CmdLineTask, ArgumentParser
from lsst.qa.explorer.functors import StarGalaxyLabeller, Magnitude, RAColumn, DecColumn, CompositeFunctor
from lsst.pipe.drivers.utils import TractDataIdContainer
from lsst.pipe.tasks.multiBandUtils import MergeSourcesRunner
from lsst.daf.persistence.butlerExceptions import NoResults

from .parquetTable import ParquetTable
from .writeObjectTable import WriteObjectTableTask

# Question: is there a way that LSST packages store data files?
ROOT = os.path.abspath(os.path.dirname(__file__))


class TractMergeSourcesRunner(MergeSourcesRunner):
    """Runner to use for a merging task when using TractDataIdContainer
    """

    @staticmethod
    def buildRefDict(parsedCmd):
        """Build a hierarchical dictionary of patch references
        Parameters
        ----------
        parsedCmd:
            The parsed command
        Returns
        -------
        refDict: dict
            A reference dictionary of the form {tract: {filter: dataRef}}
            Only one reference per tract will be saved.
            )
        """

        refDict = {}  # Will index this as refDict[tract][filter] = ref

        # Loop over tract refLists
        for refList in parsedCmd.id.refList:
            for ref in refList:
                tract = ref.dataId["tract"]
                # patch = ref.dataId["patch"]
                filter = ref.dataId["filter"]
                if tract not in refDict:
                    refDict[tract] = {}
                if filter not in refDict[tract]:
                    refDict[tract][filter] = ref  # only first patch (patch not used)

        return refDict

    @staticmethod
    def getTargetList(parsedCmd, **kwargs):
        """Provide a list of patch references for each patch, tract, filter combo.

        Parameters
        ----------
        parsedCmd:
            The parsed command
        kwargs:
            Keyword arguments passed to the task
        Returns
        -------
        targetList: list
            List of tuples, where each tuple is a (dataRef, kwargs) pair.
        """
        refDict = TractMergeSourcesRunner.buildRefDict(parsedCmd)
        return [(list(t.values()), kwargs) for t in refDict.values()]
        # return [(list(ref.values()), kwargs) for t in refDict.values() for ref in t.values()]


from lsst.pipe.base import DataIdContainer


class MultiTractDataIdContainer(DataIdContainer):
    def makeDataRefList(self, namespace):
        import pdb

        pdb.set_trace()


class PrepareQADashboardConfig(Config):
    coaddName = Field(dtype=str, default="deep", doc="Name of coadd")


class PrepareQADashboardTask(CmdLineTask):
    """Write patch-merged source tables to a tract-level parquet file
    """

    _DefaultName = "prepareQADashboard"
    ConfigClass = PrepareQADashboardConfig

    inputDataset = "visitMatchTable"

    def runDataRef(self, dataRef):
        """

        dataRef: want to point to one tract/filter combination
        """
        visitMatchTable = dataRef.get()
        print(visitMatchTable)

    @classmethod
    def _makeArgumentParser(cls):
        parser = ArgumentParser(name=cls._DefaultName)

        parser.add_id_argument(
            "--id",
            cls.inputDataset,
            help="data ID, e.g. --id tract=12345",
            ContainerClass=MultiTractDataIdContainer,
        )
        return parser

    def getVisits(self, patchRef, filt, tract=None):

        if tract is None:
            tract = patchRef.dataId["tract"]
        butler = patchRef.getButler()
        visitMatchParq = butler.get("visitMatchTable", tract=tract, filter=filt)
        visits = {int(eval(c)[1]) for c in visitMatchParq.columns if c != "id"}
        visits = list(visits)
        visits.sort()
        return visits

    def writeMeta(self, patchRefList):
        """No metadata to write.
        """
        filters = {p.dataId["filter"] for p in patchRefList}
        tracts = {p.dataId["tract"] for p in patchRefList}
        metrics = self.getMetrics()

        butler = patchRefList[0].getButler()

        try:
            meta = butler.get("qaDashboard_metadata")
        except NoResults:
            meta = {}

        for filt in filters:
            for tract in tracts:
                visits = self.getVisits(patchRefList[0], filt, tract=tract)
                if "visits" not in meta:
                    meta["visits"] = {}
                if filt not in meta["visits"]:
                    meta["visits"][filt] = {}
                if tract not in meta["visits"][filt]:
                    meta["visits"][filt][tract] = visits

        if "metrics" not in meta:
            meta["metrics"] = metrics
        else:
            if set(meta["metrics"]) != set(metrics):
                raise RuntimeError("Metrics list different than stored!")

        butler.put(meta, "qaDashboard_metadata")
