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
Trains the star galaxy classifier
"""

import numpy as np
from sklearn.ensemble import AdaBoostClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.calibration import CalibratedClassifierCV

import lsst.pex.config as pexConfig
from lsst.pipe.base import Task

__all__ = ["MakeStarGalaxyClassifierConfig", "MakeStarGalaxyClassifierTask"]


class MakeStarGalaxyClassifierConfig(pexConfig.Config):
    """Config for making the star galaxy classifier

    Notes
    -----
    The label is used to save the classifier and is used by StarGalaxyClassifierTask to identify
    the requested classifier.
    """
    label = pexConfig.Field(
        doc="Label provides version information",
        dtype=str,
        default="HSC-COSMOS-20180538",
    )

    classifierType = pexConfig.Field(
        doc="Morphology only (morph), morphology and color only (color) or both (both)",
        dtype=str,
        default="both"
    )

    columnsMorph = pexConfig.ListField(
        doc="Columns for the morphology only classifier",
        dtype=str,
        default=["magPsf-magModelHSC-R", "magPsf-magModelHSC-I", "magPsf-magModelHSC-Z", "m", "b",
                 "seeingHSC-G", "seeingHSC-R", "seeingHSC-I", "seeingHSC-Z", "seeingHSC-Y",
                 "fluxSNHSC-G", "fluxSNHSC-R", "fluxSNHSC-I", "fluxSNHSC-Z", "fluxSNHSC-Y",
                 "magPsf-magModelErrHSC-R", "magPsf-magModelErrHSC-I", "magPsf-magModelErrHSC-Z"]
    )

    columnsColor = pexConfig.ListField(
        doc="Columns for the morphology and color based classifier",
        dtype=str,
        default=["HSC-G-HSC-R", "HSC-R-HSC-I", "HSC-I-HSC-Z", "HSC-Z-HSC-Y", "magPsf-magModelHSC-G",
                 "magPsf-magModelHSC-R", "magPsf-magModelHSC-I", "magPsf-magModelHSC-Z",
                 "magPsf-magModelHSC-Y", "m", "b", "fluxSNHSC-G", "fluxSNHSC-R", "fluxSNHSC-I",
                 "fluxSNHSC-Z", "fluxSNHSC-Y", "seeingHSC-G", "seeingHSC-R", "seeingHSC-I", "seeingHSC-Z",
                 "seeingHSC-Y", "magPsf-magModelErrHSC-G", "magPsf-magModelErrHSC-R",
                 "magPsf-magModelErrHSC-I", "magPsf-magModelErrHSC-Z", "magPsf-magModelErrHSC-Y"]
    )

    filters = pexConfig.ListField(
        doc="Filters used in the classifier should be given in wavelength order",
        dtype=str,
        default=["HSC-G", "HSC-R", "HSC-I", "HSC-Z", "HSC-Y"]
    )


class MakeStarGalaxyClassifierTask(Task):
    """Train the star galaxy classifier on a pre made training set and save the classifier

    Train the star galaxy classifiers (two adaboosted decision tree classifiers - one using all information
    and the other using morphology information only) on a training dataset made by ``joinFeatures``. Saves a
    dictionary containing the classifer, the filters from the relevant camera and the columns used for the
    features.

    Notes
    -----
    The columns saved here are saved in the order needed by the classifier, when the classifier is used on
    data the columns must be given in this order. The defualt truth table is for the COSMOS field from HST,
    it is taken from Leauthaud et al 2007. A magnitude limit of i < 25.0 is used to select trusted
    classifications.

    Through the config option classifierType the morphology only version (morph), or the color and
    morphology version (color) or both (both) can be trained.
    """

    _DefaultName = "makeStarGalaxyClassifier"
    ConfigClass = MakeStarGalaxyClassifierConfig

    def __init__(self):
        Task.__init__(self)

    def runDataRef(self, dataRef):

        """Read in the table of features, train the classifiers and write out the trained classifiers.

        Parameters
        ----------
        dataRef : `lsst.daf.persistence.butlerSubset.ButlerDataRef`
            Data reference defining the tract from which the features needed to train the clssifier were
            derived.
            Used to access the following data products:
                ``deepCoadd_sg_features_tract``
        """
        trainingData = dataRef.get("deepCoadd_sg_features_tract").toDataFrame()
        filters = self.config.filters
        dataRef.dataId["label"] = self.config.label

        if self.config.classifierType == "morph" or self.config.classifierType == "both":
            cols = self.config.columnsMorph
            clf = self.run(trainingData, cols, filters)
            clfDict = {"filters": filters, "clf": clf, "colsToUse": cols}
            dataRef.put(clfDict, "starGalaxy_morphOnlyClassifier")

        if self.config.classifierType == "color" or self.config.classifierType == "both":
            cols = self.config.columnsColor
            clf = self.run(trainingData, cols, filters)
            clfDict = {"filters": filters, "clf": clf, "colsToUse": cols}
            dataRef.put(clfDict, "starGalaxy_classifier")

    def run(self, trainingData, cols, filters):
        """Train the classifier on the given features and calibrate it to return probabilites.

        Parameters
        ----------
        trainingData : `pandas.core.frame.DataFrame`
            Data frame of the features needed to train the classifier
        cols : `list`
            List of the columns that the classifier should be trained on
        filters : `list`
            List of the filters for the camera used

        Returns
        -------
        clfIsotonic : `sklearn.calibration.CalibratedClassifierCV`

        Notes
        -----
        Uses an adaboosted decision tree classifier that is then calibrated using isotonic regression. For
        more information see the scikit learn documentation.
        """

        features = np.zeros((len(cols), len(trainingData)))
        for (i, col) in enumerate(cols):
            features[i] = trainingData[col].values

        inputClass = trainingData["class"].values

        clf = AdaBoostClassifier(DecisionTreeClassifier(max_depth=2), n_estimators=400, learning_rate=1)
        clf.fit(features.T, inputClass)

        clfIsotonic = CalibratedClassifierCV(clf, cv=2, method="isotonic")
        clfIsotonic.fit(features.T, inputClass, None)

        return clfIsotonic
