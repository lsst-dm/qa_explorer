"""Command-line task and associated config for preparing data to be ingested by the QA dashboard.

Writes the following single parquet tables (potentially multi-part):



"""
from lsst.pex.config import Config, Field
from lsst.pipe.base import CmdLineTask, ArgumentParser
from lsst.pipe.drivers.utils import TractDataIdContainer
from lsst.pipe.tasks.multiBandUtils import MergeSourcesRunner
from lsst.daf.persistence.butlerExceptions import NoResults


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


class PrepareQADashboardConfig(Config):
    coaddName = Field(dtype=str, default="deep", doc="Name of coadd")


class PrepareQADashboardTask(CmdLineTask):
    """Write qaDashboardMetadata YAML dataset

    This is the metadata file that gets read in order to write the
    kartothek-repartitioned datasets used by the lsst_data_explorer
    QA dashboard.

    Requires `inputDataset` to exist (though doesn't use it), because this is
    the dataset that will be repartitioned in the dashboard preparation step.
    Also requires `visitMatchTable` to exist, as this is where the list of
    relevant visits is taken for each filter/tract combination.

    Does not actually write any new data tables, only qaDashboardMetadata.yaml,
    which contains lists of visits per tract and per filter.

    """

    _DefaultName = "prepareQADashboard"
    ConfigClass = PrepareQADashboardConfig
    RunnerClass = TractMergeSourcesRunner

    inputDataset = "analysisCoaddTable_forced"

    @classmethod
    def _makeArgumentParser(cls):
        parser = ArgumentParser(name=cls._DefaultName)

        parser.add_id_argument(
            "--id",
            cls.inputDataset,
            help="data ID, e.g. --id tract=12345",
            ContainerClass=TractDataIdContainer,
        )
        return parser

    def runDataRef(self, patchRefList):
        """Calls writeMeta, that writes the dictionary.

        Returns
        -------
        None
        """
        self.writeMeta(patchRefList)

    def getVisits(self, patchRef, filt, tract=None):

        if tract is None:
            tract = patchRef.dataId["tract"]
        butler = patchRef.getButler()
        try:
            visitMatchParq = butler.get("visitMatchTable", tract=tract, filter=filt)
            visits = {int(eval(c)[1]) for c in visitMatchParq.columns if c != "id"}
            visits = list(visits)
            visits.sort()
        except NoResults:
            visits = []
        return visits

    def writeMeta(self, patchRefList):
        """Write qaDashboard_metadata.yaml
        """
        filters = {p.dataId["filter"] for p in patchRefList}
        tracts = {p.dataId["tract"] for p in patchRefList}

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

        butler.put(meta, "qaDashboard_metadata")
