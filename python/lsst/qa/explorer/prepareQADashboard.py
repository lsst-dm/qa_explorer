"""Command-line task and associated config for preparing data to be ingested by the QA dashboard.

Writes the following single parquet tables (potentially multi-part):



"""
import os
import pandas as pd
import functools

from lsst.pex.config import Config, Field
from lsst.pipe.base import CmdLineTask, ArgumentParser
from lsst.qa.explorer.functors import StarGalaxyLabeller, Magnitude, RAColumn, DecColumn, CompositeFunctor


from .parquetTable import ParquetTable
from .writeObjectTable import WriteObjectTableTask
from .tractQADataIdContainer import TractQADataIdContainer

# Question: is there a way that LSST packages store data files?
ROOT = os.path.abspath(os.path.dirname(__file__))


class PrepareQADashboardConfig(Config):
    coaddName = Field(dtype=str, default="deep", doc="Name of coadd")


class PrepareQADashboardTask(WriteObjectTableTask):
    """Write patch-merged source tables to a tract-level parquet file
    """
    _DefaultName = "prepareQADashboard"
    ConfigClass = PrepareQADashboardConfig

    inputDatasets = ('analysisCoaddTable_forced', 'analysisCoaddTable_unforced')
    outputDataset = 'qaDashboardTable'

    def getColumnNames(self):
        """Returns names of columns to persist in consolidated table.

        This is a placeholder for a better config-based solution; ideally
        specificied in some .yaml file.
        """
        metrics = ['base_Footprint_nPix',
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

        flags = ['calib_psf_used',
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

        id_cols = ['patchId', 'id']

        return metrics + flags + id_cols

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
                               ContainerClass=TractQADataIdContainer)
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

    def run(self, catalogs, patchRef):
        columns = self.getColumnNames()

        dfs = []
        for filt, tableDict in catalogs.items():
            for dataset, table in tableDict.items():
                df = table.toDataFrame(columns=columns)
                newCols = self.getComputedColumns(table)
                df = df.concat([df, newCols], axis=1)

        catalog = functools.reduce(lambda d1, d2: d1.join(d2), dfs)
        return ParquetTable(dataFrame=catalog)

    def write(self, patchRef, catalog):
        """!
        @brief Write the output.
        @param[in]  patchRef   data reference for patch
        @param[in]  catalog    catalog
        We write as the dataset provided by the 'outputDataset'
        class variable.
        """
        patchRef.put(catalog, self.outputDataset)
        # since the filter isn't actually part of the data ID for the dataset we're saving,
        # it's confusing to see it in the log message, even if the butler simply ignores it.
        mergeDataId = patchRef.dataId.copy()
        del mergeDataId["filter"]
        self.log.info("Wrote merged catalog: %s" % (mergeDataId,))

    def writeMetadata(self, dataRef):
        """No metadata to write.
        """
        pass