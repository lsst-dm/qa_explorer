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


class PrepareQADashboardConfig(Config):
    coaddName = Field(dtype=str, default="deep", doc="Name of coadd")


class PrepareQADashboardTask(WriteObjectTableTask):
    """Write patch-merged source tables to a tract-level parquet file
    """
    _DefaultName = "prepareQADashboard"
    ConfigClass = PrepareQADashboardConfig
    RunnerClass = TractMergeSourcesRunner

    inputDatasets = ('analysisCoaddTable_forced',)  # 'analysisCoaddTable_unforced')
    # outputDatasets = ('qaDashboardCoaddTable', 'qaDashboardVisitTable')

    def getMetrics(self):
        return ['base_Footprint_nPix',
                   'Gaussian-PSF_magDiff_mmag',
                   'CircAper12pix-PSF_magDiff_mmag',
                   'Kron-PSF_magDiff_mmag',
                   'CModel-PSF_magDiff_mmag',
                   'traceSdss_pixel',
                   'traceSdss_fwhm_pixel',
                   'psfTraceSdssDiff_percent',
                   'e1ResidsSdss_milli',
                   'e2ResidsSdss_milli',
                   'deconvMoments',
                   'compareUnforced_Gaussian_magDiff_mmag',
                   'compareUnforced_CircAper12pix_magDiff_mmag',
                   'compareUnforced_Kron_magDiff_mmag',
                   'compareUnforced_CModel_magDiff_mmag',
                   'traceSdss_pixel',
                   'traceSdss_fwhm_pixel',
                   'psfTraceSdssDiff_percent',
                   'e1ResidsSdss_milli',
                   'e2ResidsSdss_milli',
                   'base_PsfFlux_instFlux',
                   'base_PsfFlux_instFluxErr']

    def getFlags(self):
        return ['calib_psf_used',
                 'calib_psf_candidate',
                 'calib_photometry_reserved',
                 'merge_measurement_i2',
                 'merge_measurement_i',
                 'merge_measurement_r2',
                 'merge_measurement_r',
                 'merge_measurement_z',
                 'merge_measurement_y',
                 'merge_measurement_g',
                 'merge_measurement_N921',
                 'merge_measurement_N816',
                 'merge_measurement_N1010',
                 'merge_measurement_N387',
                 'merge_measurement_N515',
                 'qaBad_flag']

    def getIdCols(self):
        return ['patchId', 'id']

    def getColumnNames(self):
        """Returns names of columns to persist in consolidated table.

        This is a placeholder for a better config-based solution; ideally
        specificied in some .yaml file.
        """
        return self.getMetrics() + self.getFlags() + self.getIdCols()

    def getComputedColumns(self, parq):
        """Returns dataframe with additional computed columns

        e.g., "label", "psfMag", "ra"/"dec" (in degrees)

        These functors should eventually be specified in same .yaml file
        that the rest of the columns are specified in.
        """
        funcs = CompositeFunctor({'label': StarGalaxyLabeller(),
                                  'psfMag': Magnitude('base_PsfFlux_instFlux'),
                                  'ra': RAColumn(),
                                  'dec': DecColumn()})

        newCols = funcs(parq)
        return newCols

    @classmethod
    def _makeArgumentParser(cls):
        parser = ArgumentParser(name=cls._DefaultName)

        parser.add_id_argument("--id", cls.inputDatasets[0],
                               help="data ID, e.g. --id tract=12345",
                               ContainerClass=TractDataIdContainer)
        return parser

    def readCatalog(self, patchRef):
        """Read input catalogs

        Read all the input datasets given by the 'inputDatasets'
        attribute.

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
        catalogDict = {}
        for dataset in self.inputDatasets:
            catalog = patchRef.get(dataset, immediate=True)
            catalogDict[dataset] = catalog
        return filterName, catalogDict

    def runDataRef(self, patchRefList):
        """!
        @brief Merge coadd sources from multiple bands. Calls @ref `run` which must be defined in
        subclasses that inherit from MergeSourcesTask.
        @param[in] patchRefList list of data references for each filter

        Returns
        -------

        """
        catalogs = dict(self.readCatalog(patchRef) for patchRef in patchRefList)
        mergedCoadd, visitDfs = self.run(catalogs, patchRefList[0])
        self.write(patchRefList[0], mergedCoadd, 'qaDashboardCoaddTable')
        self.writeVisitTables(patchRefList[0], visitDfs)
        self.writeMeta(patchRefList[0])
        # self.write(patchRefList[0], mergedVisits, 'qaDashboardVisitTable')

    def getVisits(self, patchRef, filt):

        tract = patchRef.dataId['tract']
        butler = patchRef.getButler()
        visitMatchParq = butler.get('visitMatchTable', tract=tract, filter=filt)
        visits = {int(eval(c)[1]) for c in visitMatchParq.columns if c != 'id'}
        visits = list(visits)
        visits.sort()
        return visits

    def run(self, catalogs, patchRef):
        columns = self.getColumnNames()

        butler = patchRef.getButler()
        tract = patchRef.dataId['tract']
        dfs = []
        visit_dfs = []
        for filt, tableDict in catalogs.items():
            for dataset, table in tableDict.items():
                # Assemble coadd table
                df = table.toDataFrame(columns=columns)
                newCols = self.getComputedColumns(table)
                df = pd.concat([df, newCols], axis=1)

                df['filter'] = filt
                df['dataset'] = dataset
                df['tractId'] = tract

                dfs.append(df)
                self.log.info('Computed coadd table for tract {}, {}.'.format(tract, filt))

            # Assemble visit table
            visits = self.getVisits(patchRef, filt)
            self.log.info('Building visit table for tract {}, {}...'.format(tract, filt))
            n_visits = len(visits)
            for i, visit in enumerate(visits):
                visitParq = butler.get('analysisVisitTable', tract=tract, filter=filt, visit=visit)
                visit_df = visitParq.toDataFrame(columns=columns)
                newCols = self.getComputedColumns(visitParq)
                visit_df = pd.concat([visit_df, newCols], axis=1)
                visit_df['filter'] = filt
                visit_df['tractId'] = tract
                visit_df['visitId'] = visit
                # TODO: add matched coaddId column
                visit_dfs.append(visit_df)
                self.log.info('{} of {}: {} ({} sources)'.format(i+1, n_visits, visit, len(visit_df)))

        catalog = pd.concat(dfs)
        all_visits = pd.concat(visit_dfs)

        # Reshape visit tables into single table per tract and metric column
        visit_dfs = []
        for metric in self.getMetrics():
            cols = [metric] + ['filter', 'tractId', 'visitId'] + self.getFlags()
            cols = [c for c in cols if c in all_visits.columns]
            visit_dfs.append(all_visits[cols])

        return ParquetTable(dataFrame=catalog), visit_dfs  # ParquetTable(dataFrame=all_visits)

    def write(self, patchRef, catalog, dataset):
        """!
        @brief Write the output.
        @param[in]  patchRef   data reference for patch
        @param[in]  catalog    catalog
        We write as the dataset provided by the 'outputDataset'
        class variable.
        """
        patchRef.put(catalog, dataset)
        # since the filter isn't actually part of the data ID for the dataset we're saving,
        # it's confusing to see it in the log message, even if the butler simply ignores it.
        mergeDataId = patchRef.dataId.copy()
        del mergeDataId["filter"]
        self.log.info("Wrote {}: {}".format(dataset, mergeDataId))

    def writeVisitTables(self, patchRef, dfs):
        butler = patchRef.getButler()

        for metric, df in zip(self.getMetrics(), dfs):
            filters = df['filter'].unique()
            for filt in filters:
                subdf = df.query('filter=="{}"'.format(filt))
                dataId = dict(patchRef.dataId)
                dataId['column'] = metric
                dataId['filter'] = filt
                table = ParquetTable(dataFrame=subdf)
                self.log.info('writing {} visit table'.format(dataId))
                butler.put(table, 'qaDashboardVisitTable', dataId=dataId)

    def writeMeta(self, dataRef):
        """No metadata to write.
        """
        tract = dataRef.dataId['tract']
        filt = dataRef.dataId['filter']
        metrics = self.getMetrics()
        visits = self.getVisits(dataRef, filt)

        butler = dataRef.getButler()
        meta = butler.get('qaDashboard_metadata')

        if 'tracts' not in meta:
            meta['tracts'] = dict(tract=visits)
        else:
            meta['tracts'][tract] = visits

        if 'filters' not in meta:
            meta['filters'] = [filt]
        elif filt not in meta['filters']:
            meta['filters'] += [filt]

        if 'metrics' not in meta:
            meta['metrics'] = metrics
        else:
            if set(meta['metrics']) != set(metrics):
                raise RuntimeError('Metrics list different than stored!')

        butler.put(meta, 'qaDashboard_metadata')


