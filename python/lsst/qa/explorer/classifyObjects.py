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
Applies star galaxy classifier
"""

import pickle

import numpy as np

import lsst.pex.config as pexConfig
from lsst.pipe.base import Task
from .parquetTable import ParquetTable

__all__ = ["StarGalaxyClassifierTask", "StarGalaxyClassifierConfig"]

class StarGalaxyClassifierConfig(pexConfig.Config):
    """Config for star galaxy classifier

    Notes
    -----
    The version name is specifed by user in the mkClassifier task and is used to read in the correct
    classifiers.
    """
    version = pexConfig.Field(
        doc="Version information",
        dtype=str,
        default="HSC-COSMOS-20180528",
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


class StarGalaxyClassifierTask(Task):
    """Add classification (star or galaxy) and associated flag columns to a parquet table.

    Classify each object in the catalog into either a star or a galaxy using two different methods, one that
    uses purely morphological information and one that uses both morphological and color information. Add an
    associated flag column for each type of classifier that is set if any NaNs were present in the input data.
    This is done for the patch specified in the ``DataRef``. To apply the classifiers additional columns are
    needed beyond those in the standard ``deepCoadd_obj``, add these into a new index (featuresSGSep) in the
    parquet table. The classification columns are ``starProbColors``, ``starProbMorph``,
    ``starProbColorsFlag`` and ``starProbMorphFlag`` these contain the probabilites of each object being a
    star according to both classifiers and a flag for each. Details of the new columns are summarised below.

    - starProbColors: probability the object is a star, based on morphology and color information, fraction
                      where 1 = star
    - starProbColorsFlag: set if starProbColors cannot be determined, e.g. if any of the columns needed by the
                      classifier contain NaNs.
    - starProbMorph: probability the object is a star, based on morphology information only, fraction where
                     1 = star
    - starProbMorphFlag: set if starProbMorph cannot be determined, e.g. if any of the columns needed by the
                         classifier contain NaNs.

    `StarGalaxyClassifierTask` has five functions that add the extra columns needed to a new index in the
    parquet table.

    `addMagnitudes`
        Use the information in the ``deepCoadd_calexp_calib`` to add CModel magntidue and error columns for
        each band to the new index in the parquet table
    `addSeeing`
        Add a column of the geometric mean of the seeing (``self.config.seeingColName``) matrix to the new
        index in the parquet table
    `addColors`
        Add a column for each color (e.g. g-r, r-i, i-z and z-Y for HSC) in the dataset to the new index.
        Needs to be run after addMagnitudes.
    `addMB`
        Calculate the gradient and intercept of the relationship between PSF - CModel in each band and the
        seeing as defined by addSeeing. Add these to the new table index. Needs to be run after addMagnitudes.
    `addSNFlux`
        Divide the model flux by its error and add it as a column to the new index

    The function `makeClassifications` applies the given classifier to the data in the catalogue provided
    using the columns provided to create the features used by the classifier.

    Notes
    -----
    The classifier uses an Adaboost algorithum using a decision tree that is trained on a truth table from
    Leauthaud et al 2007 (HST). The task requires the PSF and CModel fluxes and errors in each band and the
    seeing (``self.config.seeingColName``) enteries in each band. These are then used to calculate the
    columns required by the classifier. The columns required for each classifier are stored in the
    classifier pickle.
    """

    _DefaultName = "starGalaxyClassifier"
    ConfigClass = StarGalaxyClassifierConfig

    def __init__(self):
        Task.__init__(self)

    def runDataRef(self, dataRef):
        """Add required magnitudes in each band and run the classifier

        Parameters
        ----------
        dataRef : `lsst.daf.persistence.butlerSubset.ButlerDataRef`
            Data reference defining the patch to be classified
            Used to access the following data products:
                deepCoadd_obj produced by writeObjectTableTask
                deepCoadd_calexp_calib

        Notes
        -----
        Check that all the required filters are in the file, then add the extra magnitude columns needed to
        the table and run the classifier for the given patch and tract. The magnitude columns are calculated
        from the ``self.config.modelColName`` columns in each band using the calibration from the
        ``deepCoadd_calexp_calib``. Operates on a single patch.
        """

        cat = dataRef.get("deepCoadd_obj").toDataFrame({"dataset": "meas"})

        # To Do: DM-14855 - Train classifiers on other datasets
        # For now raise a not implemented error for other cameras, eventually needs other trained classifiers
        cameraName = dataRef.get("camera").getName()
        if cameraName != "HSC":
            raise NotImplementedError("Currently only HSC is supported")

        # To Do: DM-14539 - Move this to somewhere else
        # put this into /datasets/hsc then the butler will get it
        # Add classifier_pickle to obs_base datasets, copy deepCoadd_skyMap.
        # Filters and column headings come from the classifier pickle
        clfDictMorph = dataRef.get("starGalaxy_morphOnlyClassifier", version=self.config.version)
        filters = clfDictMorph["filters"]
        clfMorph = clfDictMorph["clf"]
        colsToUseMorph = clfDictMorph["colsToUse"]

        clfDict = dataRef.get("starGalaxy_classifier", version=self.config.version)
        filters = clfDictMorph["filters"]
        clf = clfDictMorph["clf"]
        colsToUse = clfDictMorph["colsToUse"]

        filtersInCat = set(cat.columns.get_level_values(0))
        if not filtersInCat >= set(filters):
            missingFilters = list(set(filters) - filtersInCat)
            raise RuntimeError("Not all required filters are present in the catalog: \
                                missing {}.".format(missingFilters))

        colsRequired = {self.config.seeingColName + "_xx", self.config.seeingColName + "_xy",
                        self.config.seeingColName + "_yy", self.config.psfColName, self.config.modelColName,
                        self.config.psfColName + "Err", self.config.modelColName + "Err"}
        for band in filters:
            colsInCat = set(cat[band].columns)
            missingCols = list(set(colsRequired) - colsInCat)
            if len(missingCols) > 0:
                raise RuntimeError("Not all required columns are present in catalog: \
                                    missing {} in {}.".format(missingCols, band))

        cat = self.addMagnitudes(cat, dataRef, filters)
        cat = self.run(cat, filters, clfMorph, colsToUseMorph, clf, colsToUse)

        cat = ParquetTable(dataFrame=cat)
        dataRef.put(cat, "deepCoadd_sg")

    def run(self, cat, filters, clfMorph, colsToUseMorph, clf, colsToUse):
        """Add colour based classification, morphology only based classification and their associated flags.

        Parameters
        ----------
        cat : `pandas.core.frame.DataFrame`
        filters : `list`
            List of the filters for the camera used
        clfMorph : `sklearn.calibration.CalibratedClassifierCV`
            Morphology only boosted decision tree classifier trained using the columns (in order) defined in
            colsToUseMorph.
        colsToUseMorph : `list`
            Ordered list of the columns that are required by the morphology only classifier. The columns must
            be in the same order they were when the classifier was trained.
        clf : `sklearn.calibration.CalibratedClassifierCV`
            Boosted decision tree classifier that uses both color and morphology information trained using the
            columns (in order) defined in colsToUse.
        colsToUse : `list`
            Ordered list of the columns required by the classifier that uses color and morphology. The columns
            must be in the same order as they were when the classifier was trained.

        Returns
        -------
        cat : `pandas.core.frame.DataFrame`
            Data frame with the two classification and two flag columns added and a new index containing the
            columns required by the classifier

        Notes
        -----
        For the morphology only classifier the features used are psf - cmodel in the three bands with the
        tightest psf - cmodel relationship (for HSC this is r, i and z), m, b, seeing in all of the bands
        used, flux/flux error in all the bands used psf - cmodel error in the three best bands.
        For the colour and morphology classifier the features used are the cmodel colors in all bands,
        psf - cmodel in all bands, m, b, flux / flux error in all the bands, seeing in all the bands,
        psf - cmodel error in all the bands. The columns must be supplied to the classifier in the correct
        order, this is handled by the two variable colsToUse and colsToUseMorph which are contained in the
        classifier pickles and are used to ensure that the array of features is generated in the correct
        order. Both of the classifiers were trained and saved using `trainClassifier`.

        Apply both of the classifiers to the data

        The columns added are:
            PSF mangitudes in all the bands (in wavelength order)
                Added by `addMagnitudes`, units: magnitudes, column name: ``magPsf + band``
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
        These columns are added into a new index called ``featuresSG`` and more information is given in their
        respective functions.
        """

        # Add required columns
        cat = self.addSeeing(cat, filters)
        cat = self.addColors(cat, filters)
        cat = self.addMB(cat, filters)
        cat = self.addSNFlux(cat, filters)

        probsMorph, flagsMorph = self.makeClassifications(cat, clfMorph, colsToUseMorph)
        probs, flags = self.makeClassifications(cat, clf, colsToUse)

        for band in filters:
            cat[band, "starProbColors"] = probs
            cat[band, "starProbMorph"] = probsMorph
            cat[band, "starProbColorsFlag"] = flags
            cat[band, "starProbMorphFlag"] = flagsMorph

        return cat

    def addMagnitudes(self, cat, dataRef, filters):
        """Add PSF and CModel magntidues, add nans for negative or zero flux

        Parameters
        ----------
        cat : `pandas.core.frame.DataFrame`
        dataRef :
            Data reference for the given tract and patch
        filters : `list`

        Returns
        -------
        cat : `pandas.core.frame.DataFrame`
            Data frame with new index added to store columns used by the classifiers

        Notes
        -----
        The columns added are called ``magPsf + filter name``, ``magPsfErr + filter name``,
        ``magModel + filter name`` and ``magModelErr + filter name``. They are added for every filter given
        in the filters argument.
        """

        colsIn = [self.config.psfColName, self.config.modelColName]
        colsOut = ["magPsf", "magModel"]

        for band in filters:

            calib = dataRef.get("deepCoadd_calexp_calib", filter=band)

            for (i, col) in enumerate(colsIn):

                goodFlux = ((np.isfinite(cat[band][col])) & (cat[band][col] > 0.0))

                magCol = np.array([np.nan]*len(cat))
                magErrCol = np.array([np.nan]*len(cat))

                flux = cat[band][col][goodFlux]
                fluxErr = cat[band][col + "Err"][goodFlux]

                mags, magErrs = calib.getMagnitude(flux.values, fluxErr.values)

                magCol[goodFlux] = mags
                magErrCol[goodFlux] = magErrs

                cat["featuresSGSep", colsOut[i] + band] = magCol
                cat["featuresSGSep", colsOut[i] + "Err" + band] = magErrCol

        return cat

    def addSeeing(self, cat, filters):
        """Add seeing defined by seeing = (xx*yy - xy*xy)**0.25 where xx, yy and xy are enteries in the
        ``self.config.seeingColName`` matrix

        Parameters
        ----------
        cat : `pandas.core.frame.DataFrame`
        filters : `list`

        Returns
        -------
        cat : `pandas.core.frame.DataFrame`
            Data frame with new index added to store columns used by the classifiers

        Notes
        -----
        The new columns added are called ``seeing + filter name`` and are added for every ``filter name`` in
        the list of filters provided.
        """

        seeingCol = self.config.seeingColName

        for band in filters:
            xx = cat[band][seeingCol + "_xx"]
            xy = cat[band][seeingCol + "_xy"]
            yy = cat[band][seeingCol + "_yy"]

            seeing = (xx*yy - xy*xy)**0.25

            cat["featuresSGSep", "seeing" + band] = seeing

        return cat

    def addColors(self, cat, filters):
        """Add all available colors

        Parameters
        ----------
        cat : `pandas.core.frame.DataFrame`
        filters : `list`

        Returns
        -------
        cat : `pandas.core.frame.DataFrame`
            Data frame with new index added to store columns used by the classifiers

        Notes
        -----
        len(filters)-1 new columns are added giving each filter minus the filter after it when the filters
        are arranged in wavelength order. These columns are named ``filter[n]-filter[n+1]``. E.g. for HSC:
        ``HSC-G-HSC-R``, ``HSC-R-HSC-I``, ``HSC-I-HSC-Z`` and ``HSC-Z-HSC-Y``. Needs to be run after
        addMagnitudes so that the magnitude columns are already present.
        """

        i = 0
        while i < len(filters) - 1:
            band = filters[i]
            color = cat["featuresSGSep"]["magModel" + band] - cat["featuresSGSep"]["magModel" + filters[i+1]]
            cat["featuresSGSep", filters[i] + "-" + filters[i+1]] = color
            i += 1

        return cat

    def addMB(self, cat, filters):
        """Add m and b, the gradient and intercept of the line of best fit between the psf - model in each
        band and the seeing

        Parameters
        ----------
        cat : `pandas.core.frame.DataFrame`
        filters : `list`

        Returns
        -------
        cat : `pandas.core.frame.DataFrame`
            Data frame with new index added to store columns used by the classifiers

        Notes
        -----
        These columns are named ``m`` and ``b`` where m is the gradient and b is the intercept. Needs to be run
        after addMagnitudes so that the required columns are present.
        """

        for band in filters:
            cat["featuresSGSep", "magPsf-magModel" + band] = cat["featuresSGSep"]["magPsf" + band] - \
                                                             cat["featuresSGSep"]["magModel" + band]
            errs = np.sqrt(cat["featuresSGSep"]["magPsfErr" + band]**2 +
                           cat["featuresSGSep"]["magModelErr" + band]**2)
            cat["featuresSGSep", "magPsf-magModelErr" + band] = errs

        # The fastest way to iterate through a pandas dataframe is by using iter tuples but it gets rid of the
        # column names so we need to figure out the equivalent column numbers

        seeing_ids = []
        diff_ids = []
        for (i, col) in enumerate(cat["featuresSGSep"].columns):
            if "magPsf-magModel" in col and "Err" not in col:
                diff_ids.append("_" + str(i+1))
            elif "seeing" in col:
                seeing_ids.append("_" + str(i+1))

        ms = []
        bs = []
        n = 0
        for (i, row) in enumerate(cat["featuresSGSep"].itertuples()):
            diffs = [getattr(row, band) for band in diff_ids]
            seeing = [getattr(row, band) for band in seeing_ids]

            if np.all(np.isfinite(diffs)) and np.all(np.isfinite(seeing)):
                m, b = np.polyfit(seeing, diffs, 1)
                ms.append(m)
                bs.append(b)
            else:
                ms.append(np.nan)
                bs.append(np.nan)
            n += 1

        cat["featuresSGSep", "m"] = ms
        cat["featuresSGSep", "b"] = bs

        return cat

    def addSNFlux(self, cat, filters):
        """Add the ratio of cmodel flux over flux error

        Parameters
        ----------
        cat : `pandas.core.frame.DataFrame`
        filters : `list`

        Returns
        -------
        cat : `pandas.core.frame.DataFrame`
            Data frame with new index added to store columns used by the classifiers

        Notes
        -----
        These columns are named ``fluxSN + filter name`` and are added in every filter included in the filters
        list.
        """

        for band in filters:
            cat["featuresSGSep", "fluxSN" + band] = cat[band][self.config.modelColName] / \
                                                    cat[band][self.config.modelColName + "Err"]

        return cat

    def makeClassifications(self, cat, clf, colsToUse):
        """Apply classifier to features defined from ``cat`` by using the columns in ``colsToUse``.

        Parameters
        ----------
        cat : `pandas.core.frame.DataFrame`
        clf : `sklearn.calibration.CalibratedClassifierCV`
            Boosted decision tree classifier trained using the columns (in order) defined in colsToUse.
        colsToUse : `list`
            Ordered list of the columns that are required by the classifier. The columns must be in the same
            order they were when the classifier was trained.

        Returns
        -------
        probsOut : `numpy.array`
            An array of the probabilities to be a star, 1 = star and 0 = galaxy.
        flags : `numpy.array`
            An array of flags, set if any of the input columns are NaNs.
        """

        features = np.zeros((len(colsToUse), len(cat)))

        for (i, col) in enumerate(colsToUse):
            features[i] = cat["featuresSGSep"][col].data

        # Need to take out the nans but remember which rows they are and assign nans to the probability and
        # add a flag
        featureSums = np.sum(features, axis=0)
        cleanIds = np.where((np.isfinite(featureSums)))[0]

        cleanFeatures = features[:, cleanIds]

        flags = np.ones(len(cat))
        flags[cleanIds] = 0

        probsOut = np.array([np.nan]*len(cat))

        probs = clf.predict_proba(cleanFeatures.T)
        probsOut[cleanIds] = probs[:, 0]

        return probsOut, flags
