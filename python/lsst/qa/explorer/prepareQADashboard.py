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
        self.refList = [
            [
                namespace.butler.dataRef(self.datasetType, dataId=dataId)
                for dataId in self.idList
                if namespace.butler.datasetExists(self.datasetType, dataId=dataId)
            ]
        ]


class PrepareQADashboardConfig(Config):
    coaddName = Field(dtype=str, default="deep", doc="Name of coadd")


class PrepareQADashboardTask(CmdLineTask):
    """Write patch-merged source tables to a tract-level parquet file
    """

    _DefaultName = "prepareQADashboard"
    ConfigClass = PrepareQADashboardConfig

    inputDataset = "visitMatchTable"
    # inputDataset = "sourceTable_visit"
    otherDatasets = (
        ("sourceTable_visit", ("visit",)),
        ("objectTable_tract", ("tract",)),
        ("analysisVisitTable", ("filter", "tract", "visit")),
        ("analysisVisitTable_commonZp", ("filter", "tract", "visit")),
        ("analysisCoaddTable_forced", ("filter", "tract")),
        ("analysisCoaddTable_unforced", ("filter", "tract")),
        ("analysisColorTable", ("tract",)),
    )

    def runDataRef(self, dataRefList):
        """

        dataRef: want to point to one tract/filter combination

        qaDashboard_metadata.yaml looks like this:


        visits:
            filter1:
                tract1:
                    -1234
                    -1235
                    -1236
                tract2:
                    -2345
                    -2366
            filter2:
                ...
        """
        butler = dataRefList[0].getButler()

        meta = {} if not butler.datasetExists("qaDashboard_info") else butler.get("qaDashboard_info")

        for dataRef in dataRefList:

            visitMatchParq = dataRef.get()
            visits = {int(eval(c)[1]) for c in visitMatchParq.columns if c != "id"}
            visits = list(visits)
            visits.sort()

            filt = dataRef.dataId["filter"]
            tract = dataRef.dataId["tract"]
            if "visits" not in meta:
                meta["visits"] = {}
            if filt not in meta["visits"]:
                meta["visits"][filt] = {}
            if tract not in meta["visits"][filt]:
                meta["visits"][filt][tract] = visits

        meta[self.inputDataset] = [dict(dataRef.dataId) for dataRef in dataRefList]

        # Write other dataIds that will be of interest
        for dataset, keys in self.otherDatasets:
            meta[dataset] = [
                dataId for dataId in self.iter_dataId(meta, keys) if butler.datasetExists(dataset, **dataId)
            ]

        butler.put(meta, "qaDashboard_info")

    def iter_dataId(self, metadata, keys):
        d = metadata
        seen_already = set()
        for filt in d["visits"].keys():
            for tract in d["visits"][filt]:
                for visit in d["visits"][filt][tract]:
                    dataId = {
                        k: v
                        for k, v in {"filter": filt, "tract": tract, "visit": visit}.items()
                        if k in keys
                    }
                    if tuple(dataId.items()) not in seen_already:
                        yield dataId
                        seen_already.add(tuple(dataId.items()))

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

    def writeMetadata(self, dataRefList):
        """No metadata to write.
        """
        pass
