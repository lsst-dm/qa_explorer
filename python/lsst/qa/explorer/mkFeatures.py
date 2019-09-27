# This file is part of qa explorer
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


"""
Makes the features needed to train the star galaxy classifier
"""

import numpy as np

from lsst.meas.algorithms.loadIndexedReferenceObjects import LoadIndexedReferenceObjectsTask
import lsst.afw.geom as afwGeom
import lsst.pex.config as pexConfig
from lsst.pipe.base import Task
from lsst.pipe.tasks.parquetTable import ParquetTable
from .classifyObjects import StarGalaxyClassifierTask
from .match import match_lists

__all__ = ["StarGalaxyFeaturesConfig", "StarGalaxyFeaturesTask"]


class StarGalaxyFeaturesConfig(pexConfig.Config):
    """Config for making the files of features needed to train the star galaxy classifier
    """

    filters = pexConfig.ListField(
        doc="Filters",
        dtype=str,
        default=["HSC-G", "HSC-R", "HSC-I", "HSC-Z", "HSC-Y"]
    )

    seeingColName = pexConfig.Field(
        doc="Column name for seeing columns used in addSeeing",
        dtype=str,
        default="base_SdssShape_psf"
    )

    psfColName = pexConfig.Field(
        doc="Column name for PSF flux columns used in addMagnitudes",
        dtype=str,
        default="base_PsfFlux_flux"
    )

    modelColName = pexConfig.Field(
        doc="Column name for model flux columns used in addSNFlux and addMagnitudes",
        dtype=str,
        default="modelfit_CModel_flux"
    )

    truthRaColName = pexConfig.Field(
        doc="Column name for RA column in the truth table",
        dtype=str,
        default="coord_ra"
    )

    truthDecColName = pexConfig.Field(
        doc="Column name for Declination column in the truth table",
        dtype=str,
        default="coord_dec"
    )

    truthClassColName = pexConfig.Field(
        doc="Column name for Classification column in the truth table, star = 0 and galaxy = 1",
        dtype=str,
        default="MU_CLASS"
    )

    truthMatchRadius = pexConfig.Field(
        doc="Match radius for matching between the truth table and the input catalog default is 0.5 \
             arcseconds.",
        dtype=float,
        default=0.5/3600
    )

    cleanRadius = pexConfig.Field(
        doc="Match radius inside which to remove neighbours. Default is 1.5 arcseconds which was found to \
             remove sources affecting the classifier.",
        dtype=float,
        default=1.5/3600
    )

    magLim = pexConfig.Field(
        doc="Magnitude limit for the output features, the default truth table uses a catalog that is \
             trusted to 25th in i so this limit is used.",
        dtype=float,
        default=25.0
    )


class StarGalaxyFeaturesTask(StarGalaxyClassifierTask):
    """
    Make a table of features that are used to train the star galaxy classifier.

    Create a new table that contains the features needed to train the star galaxy classifier. These columns
    are the same columns as are required to perform the classifications in
    `lsst.qa.explorer.classifyObjects.StarGalaxyClassifierTask` and the functions used to add the columns are
    inherited from that task. The data to be processed is matched to the truth table given and as such these
    need to overlap. The truth table is assumed to contain a classification column that is 0 for stars and 1
    for galaxies.

    `StarGalaxyFeaturesTask` inherits and uses five functions from
    `lsst.qa.explorer.classifyObjects.StarGalaxyClassifierTask` and has four of its own. These are given
    below:

    `addRaDec`
        Add an RA and declination column to the ``featuresSGSep`` index of the input table, this RA and dec
        is taken from the reference catalog.
    `cleanUpCat`
        Apply a cut on the input catalog to clean up the data based on different pre-computed flags, return a
        catalog that is only the featuresSGSep index of the full input catalog.
    `removeNeighbors`
        Remove objects with a neighbor in the input catalgo with in 1.5 arcseconds or with two matches in
        the truth table within 1.5 arseconds.
    `addTruthClass`
        Add the classification from the truth table to the table.

    Notes
    -----
    The current defualt truth table is for the COSMOS field from HST, it is taken from Leauthaud et al 2007.
    Steps were then taken to clean up this catalog including removing objects that had close contaminants
    that did not make it into the truth catalog. The default truth table has RA and Dec. in radians.

    The `removeNeighbors` and `addTruthClass` functions need to be run after the `cleanUpCat` function as
    they expect the cut down single index catalog rather than the large multi index one taken initially. The
    functions from `lsst.qa.explorer.classifyObjects.StarGalaxyClassifierTask` and `addRaDec` need to be run
    before `cleanUpCat` as they require the full multi index catalog.
    """

    _DefaultName = "StarGalaxyFeatures"
    ConfigClass = StarGalaxyFeaturesConfig

    def __init__(self):
        Task.__init__(self)

    def runDataRef(self, dataRef):

        """Add required magnitudes in each band and run the classifier, read in the reference catalog and
        the truth table reference object.

        Parameters
        ----------
        dataRef : `lsst.daf.persistence.butlerSubset.ButlerDataRef`
            Data reference defining the patch to be turned into features for training the classifiers
            Used to access the folowing data products:
                ``deepCoadd_obj`` produced by `writeObjectTask`
                ``deepCoadd_calecp_calib``
                ``HST_truth_table_star_galaxy_refCat``

        Notes
        -----
        Check that all the required magnitudes are in the file, then add the extra magnitudes needed to
        create the features needed to train the classifier for the given patch and tract. The magnitude
        columns are calculated from the ``self.config.modelColName`` columns in each band using the
        calibration from the ``deepCoadd_calexp_calib``. Operates on a signle patch. Read in the associated
        reference catalog and the given truth table. The default is a catalog from Leauthaud et al 2007
        which uses HST COSMOS data.
        """

        cat = dataRef.get("deepCoadd_obj").toDataFrame({"dataset": "meas"})
        refInfo = {"dataset": "ref", "column": ["coord_ra", "coord_dec"], "filter": "HSC-G"}
        refCat = dataRef.get("deepCoadd_obj").toDataFrame(refInfo)

        filters = self.config.filters
        filtersInCat = set(cat.columns.get_level_values(0))
        if not filtersInCat >= set(filters):
            missingFilters = list(set(filters) - filtersInCat)
            raise RuntimeError("Not all required filters are present in the catalog: \
                                missing {}.".format(missingFilters))

        cat = self.addMagnitudes(cat, dataRef, filters)

        config = LoadIndexedReferenceObjectsTask.ConfigClass()
        config.ref_dataset_name = 'HST_truth_table_star_galaxy_refCat'
        butler = dataRef.getButler()
        truthRefObj = LoadIndexedReferenceObjectsTask(butler, config=config)

        cat = self.run(cat, filters, truthRefObj, refCat)
        cat = ParquetTable(dataFrame=cat)
        dataRef.put(cat, "deepCoadd_sg_features")

    def run(self, cat, filters, truthRefObj, refCat):
        """Create the features required to train the star galaxy classifier.

        Parameters
        ----------
        cat : `pandas.core.frame.DataFrame`
        filters : `list`
            List of the filters for the camera used
        truthRefObj : `lsst.meas.algorithms.loadIndexedReferenceObjects.LoadIndexedReferenceObjectsTask`
        refCat : `pandas.core.frame.DataFrame`

        Raises
        ------
        `RuntimeError`
            If there is no overlap between the truth table and the input catalog.

        Returns
        -------
        matchedCat : `pandas.core.frame.DataFrame`
            A thinner, single indexed, dataframe with all the columns required to train the classifier
            including the matched truth classifications from the truth table.

        Notes
        -----
        The features added are psf - cmodel in all the available bands, seeing in all of the bands,
        flux/flux error in all the bands, m, b, all available colors, ra and dec from the reference catalog
        and the truth classification from the truth table. To add the truth classifications the catalogs
        are matched on RA and declination taking the closest match within ``self.config.truthMatchRadius``.
        The truth classifications are 0 for stars and 1 for galaxies.

        The columns added are:
            PSF mangitudes in all the bands (in wavelength order)
                Added by `addMagnitudes`, units: magnitudes,
                column name: ``magPsf + band``
            PSF magntiude errors in all the bands
                Added by `addMagnitudes`, units: magnitudes, column name: ``magPsfErr + band``
            CModel magnitudes in all the bands
                Added by `addMagnitudes`, units: magnitudes, column name: ``magModel + band``
            CModel magnitude error in al the bands
                Added by `addMagnitudes`, units: magnitudes, column name: ``magModelErr + band``
            Seeing in all the bands
                Added by `addSeeing`, units: arcsecs, column name: ``seeing + band``
            Colors from all bands
                Added by `addColors`, units: magnitudes, column name: ``first band - second band``
            Gradient of relationship between psf - cmodel and seeing
                Added by `addMB`, units: magnitudes/arcses, column name: ``m``
            Intercept of relationship between psf - cmodel and seeing
                Added by `addMB`, units: magnitudes, column name: ``b``
            Flux signal to noise in all bands
                Added by `addSNFlux`, units: none, column name: ``fluxSN + band``
            Reference catalog right ascension
                Added by `addRaDec`, units: radians, column name: ``ra``
            Reference catalog declination
                Added by `addRaDec`, units: radians, column name: ``dec``
            Truth classification value
                Added by `addTruthClass`, units: none, column name: ``class``

        These columns are added into a new index called ``featuresSG`` and more information is given in their
        respective functions. addTruth assumes that `cleanUpCat` has been run and that it is being run on a
        single index catalog of just the ``featuresSG`` index. This single index catalog has had the objects
        with close neighbors (default within 1.5 arcseconds) removed in `removeNeighbors` and has been
        narrowed down by applying a series of flag cuts in `cleanUpCat`.
        """

        cat = self.addSeeing(cat, filters)
        cat = self.addColors(cat, filters)
        cat = self.addMB(cat, filters)
        cat = self.addSNFlux(cat, filters)
        cat = self.addRaDec(cat, refCat)

        for flag in ["base_PixelFlags_flag_saturated", "base_SdssShape_flag_psf"]:
            cat = self.cleanOnFlag(cat, flag, filters)
        for fluxCol in ["base_PsfFlux_flux", "modelfit_CModel_flux"]:
            cat = self.goodFlux(cat, fluxCol, filters)
        cat = self.removeBlended(cat, filters)
        cleanCat = self.cleanUpCat(cat)

        medRa = np.median(cleanCat["ra"].values)
        medDec = np.median(cleanCat["dec"].values)
        center = afwGeom.SpherePoint(medRa, medDec, afwGeom.radians)
        radius = (1/np.sqrt(2))*(np.max(cleanCat["ra"].values)-np.min(cleanCat["ra"].values))*afwGeom.radians
        refs = truthRefObj.loadSkyCircle(center, radius, "MAG_BEST").refCat
        truthCat = refs.asAstropy()
        if len(truthCat) == 0:
            raise RuntimeError("Truth catalogue and input data do not overlap.")

        cleanCat = self.removeNeighbors(cleanCat, truthCat)
        matchedCat = self.addTruthClass(cleanCat, truthCat)

        return matchedCat

    def addRaDec(self, cat, refCat):

        """Add RA and Dec column from the reference catalog to the featuresSGSep index.

        Parameters
        ----------
        cat : `pandas.core.frame.DataFrame`
        refCat : `pandas.core.frame.DataFrame`
            Reference RA and Declination.

        Returns
        -------
        cat : `pandas.core.frame.DataFrame`
            Data frame with reference RA and declination added into the featuresSGSep index.
        """

        # Reference catalogue should be in the same order as the meas catalogue (cat) but just in case use
        # the index ids from the meas catalogue.
        cat["featuresSGSep", "ra"] = refCat["coord_ra"][cat.index]
        cat["featuresSGSep", "dec"] = refCat["coord_dec"][cat.index]

        return cat

    def cleanOnFlag(self, cat, flag, filters):
        """Remove objects from the dataframe that do not pass a series of flag cuts designed to clean up the
        data.

        Parameters
        ----------
        cat : `pandas.core.frame.DataFrame`
            The multi index input data frame

        Returns
        -------
        cat : `pandas.core.frame.DataFrame`
            The multi index input data frame with objects that do not pass the flag cuts removed.
        """

        for band in filters:
            clean = (~cat[band][flag])
            cat = cat[clean]

        return cat

    def goodFlux(self, cat, fluxCol, filters):
        """Remove objects from the dataframe that are blended or saturated in any band, those that have
        flux <= 0 in any band (psf or model flux) or have a NaN value.

        Parameters
        ----------
        cat : `pandas.core.frame.DataFrame`
            The multi index input data frame

        Returns
        -------
        cleanCat : `pandas.core.frame.DataFrame`
            The multi index input data frame with objects that do not pass the flux criteria removed.
        """

        for band in filters:
            clean = ((cat[band][fluxCol] > 0.0) & (np.isfinite(cat[band][fluxCol])))
            cat = cat[clean]

        return cat

    def removeBlended(self, cat, filters):
        """Remove objects which do not have ``deblend_nChild == 0``.

        Parameters
        ----------
        cat : `pandas.core.frame.DataFrame`
            The multi index input data frame

        Returns
        -------
        cleanCat : `pandas.core.frame.DataFrame`
            The multi index input data frame with objects that are blended (by this definition) removed.
        """

        for band in filters:
            clean = (cat[band]["deblend_nChild"] == 0)
            cat = cat[clean]

        return cat

    def cleanUpCat(self, cat):
        """Remove objects from the dataframe that have magnitudes of 0 or less in either the PSF or model
        magnitudes. Also removed objects that have any NaN values in their features. Restrict the catalog
        to only those objects brighter than the magnitude limit (`self.config.magLim`). Return only the
        ``featuresSGSep`` index containing only the seleted objects.

        Parameters
        ----------
        cat : `pandas.core.frame.DataFrame`
            The multi index input data frame

        Returns
        -------
        cleanCat : `pandas.core.frame.DataFrame`
            A single index pandas data frame containing only objects selected to be useful for training.
        """
        clean = ((cat["featuresSGSep"]["magPsfHSC-G"] > 0.0) &
                 (cat["featuresSGSep"]["magPsfHSC-R"] > 0.0) &
                 (cat["featuresSGSep"]["magPsfHSC-I"] > 0.0) &
                 (cat["featuresSGSep"]["magPsfHSC-Z"] > 0.0) &
                 (cat["featuresSGSep"]["magPsfHSC-Y"] > 0.0) &
                 (cat["featuresSGSep"]["magModelHSC-G"] > 0.0) &
                 (cat["featuresSGSep"]["magModelHSC-R"] > 0.0) &
                 (cat["featuresSGSep"]["magModelHSC-I"] > 0.0) &
                 (cat["featuresSGSep"]["magModelHSC-Z"] > 0.0) &
                 (cat["featuresSGSep"]["magModelHSC-Y"] > 0.0) &
                 (np.isfinite(np.sum(cat["featuresSGSep"].values, axis=1))) &
                 (cat["featuresSGSep"]["magModelHSC-I"] < self.config.magLim))

        cleanCat = cat["featuresSGSep"][clean]

        return cleanCat

    def removeNeighbors(self, cat, truthCat):
        """Remove objects that have two matches in either the input table or the truth table within 1.5
        arcsecs (default value, can be changed in config).

        Parameters
        ----------
        cat : `pandas.core.frame.DataFrame`
        truthCat : `astropy.table.table.Table`
            The table that contains classifications being used as truth

        Returns
        -------
        cat : `pandas.core.frame.DataFrame`
            A datafame with the objects with close neighbours removed

        Notes
        -----
        Assumes both coordinates but the matcher assumes that they are in degrees and
        ``self.config.cleanRadius`` is in degrees.
        """

        cleanRadius = self.config.cleanRadius
        dists, inds = match_lists(np.rad2deg(cat["ra"].values), np.rad2deg(cat["dec"].values),
                                  np.rad2deg(cat["ra"].values), np.rad2deg(cat["dec"].values),
                                  cleanRadius, numNei=2)

        cat = cat[~np.isfinite(dists[:, 1])]

        dists, inds = match_lists(np.rad2deg(cat["ra"].values), np.rad2deg(cat["dec"].values),
                                  np.rad2deg(truthCat[self.config.truthRaColName]),
                                  np.rad2deg(truthCat[self.config.truthDecColName]), cleanRadius, numNei=2)

        cat = cat[~np.isfinite(dists[:, 1])]

        return cat

    def addTruthClass(self, cat, truthCat):
        """Add the classifications from the truth table

        Parameters
        ----------
        cat : `pandas.core.frame.DataFrame`
            The catalog to add the truth classifications to
        truthCat : `astropy.table.table.Table`
            The truth table

        Returns
        -------
        matchedCat : `pandas.core.frame.DataFrame`
            Catalog with a ``class`` column added

        Notes
        -----
        The ``class`` column is 0 for stars and 1 for galaxies, the truth table needs to abide by this
        convention. RA and Dec. are assumed to be in radians but the matcher takes coordinates in degrees and
        ``self.config.matchRadius`` is in degrees.
        """

        matchRadius = self.config.truthMatchRadius
        dist, inds = match_lists(np.rad2deg(cat["ra"].values), np.rad2deg(cat["dec"].values),
                                 np.rad2deg(truthCat[self.config.truthRaColName]),
                                 np.rad2deg(truthCat[self.config.truthDecColName]), matchRadius)

        match = (inds != len(truthCat))
        matchedTruth = truthCat[inds[match]]
        matchedCat = cat[match].copy()

        star = np.where((matchedTruth[self.config.truthClassColName] == 0))[0]
        classifications = np.ones(len(matchedCat))
        classifications[star] = 0
        matchedCat["class"] = classifications

        return matchedCat
