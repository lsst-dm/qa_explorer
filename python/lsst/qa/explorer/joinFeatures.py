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
Makes one large training set for the classifier.
"""

import pandas as pd

import lsst.pex.config as pexConfig
import lsst.daf.persistence.butlerExceptions
from lsst.pipe.base import Task
from lsst.pipe.tasks.parquetTable import ParquetTable

__all__ = ["JoinFeaturesTask", "JoinFeaturesConfig"]


class JoinFeaturesConfig(pexConfig.Config):
    """Config for the `JoinFeaturesTask`.

    Notes
    -----
    Currently only the options of ``galaxies``, ``stars`` and ``all`` are supported for sourceTypes. These
    allow only stars, only galaxies or all sources to be used from each rerun passed to the `JoinFeaturesTask`
    they must be in the same order as the reruns.
    The default sourceTypes option uses stars twice as only stars were used from two different reruns and so
    need to be included separately.
    """
    sourceTypes = pexConfig.ListField(
        doc="Type of sources used from each rerun.",
        dtype=str,
        default=["galaxies", "stars", "stars", "all"]
    )

    outputRerunName = pexConfig.Field(
        doc="Name of the rerun that the output should be written to. Should be one of the names from the"
            "input dict of dataRefs",
        dtype=str,
        default="weekly28_stars"
    )


class JoinFeaturesTask(Task):

    """Join the ``deepCoadd_sg_features`` files together into one per tract for all the input reruns.

    Create a single file, ``deepCoadd_sg_features_tract`` that combines all the patches from all the input
    reruns that were given. This large file can then be used to train the classifier which needs all the
    information in one place. First all the patches in each input rerun are combined and then the data frames
    from each rerun are combined into one final data frame which is written out a as a parquet file.

    Notes
    -----
    The ``sourceTypes`` config parameter allows the selection of all the sources from the input rerun, only
    the stars or only the galaxies. It takes the arguments ``all``, ``stars`` and ``galaxies`` and must be
    in the same order as the datarefs in the dict of datarefs.
    """

    _DefaultName = "joinFeatures"
    ConfigClass = JoinFeaturesConfig

    def __init__(self):
        Task.__init__(self)

    def runDataRef(self, dataRefs):
        """Read in all the patch files for each dataRef and then concatenate them into one data frame. Then take
        these data frames and concatenate them together from all the input reruns.

        Parameters
        ----------
        dataRefs : `dict`
            Dictionary of names of reruns and their associated dataRefs
            The dataRefs are then used to access the following data products:
                ``deepCoadd_sg_features`` produced by `StarGalaxyFeaturesTask`

        Notes
        -----
        The dict of dataRefs contains the dataRef from each rerun and an associated name that can be used to
        distinguish between the dataRefs if required.
        """

        rerunDataFrames = []
        for dataRefName in dataRefs.keys():
            dataRef = dataRefs[dataRefName]
            skymap = dataRef.get("deepCoadd_skyMap")
            tract = dataRef.dataId["tract"]
            tractInfo = skymap[tract]
            featDataFrames = []
            for patchInfo in tractInfo:
                patch = str(patchInfo.getIndex()[0]) + "," + str(patchInfo.getIndex()[1])
                try:
                    featDataFrame = dataRef.get("deepCoadd_sg_features", patch=patch).toDataFrame()
                except lsst.daf.persistence.butlerExceptions.NoResults:
                    self.log.warn("No 'deepCoadd_sg_features' found for patch: {} in dataRef "
                                  "named: {}".format(patch, dataRefName))
                    continue
                featDataFrames.append(featDataFrame)

            rerunDataFrame = pd.concat(featDataFrames)
            rerunDataFrames.append(rerunDataFrame)

        tractDataFrame = self.run(rerunDataFrames, self.config.sourceTypes)
        tractDataFrame = ParquetTable(dataFrame=tractDataFrame)
        dataRefs[self.config.outputRerunName].put(tractDataFrame, "deepCoadd_sg_features_tract")

    def run(self, rerunDataFrames, sourceTypes):
        """Concatenate all the data frames into one data frame.

        Parameters
        ----------
        rerunDataFrames : `list`
            List of data frames made from all the patches in each rerun
        sourceTypes : `list`
            List of the types of sources needed from each rerun, stars, galaxies or all

        Returns
        -------
        tractDataFrame : `pandas.core.frame.DataFrame`
            Data frame that contains all the data from all the patches from all the reruns

        Raises
        ------
        RuntimeError
            If the sourceType isn't one of stars, galaxies or all.

        """
        toJoin = []
        for (rerunDataFrame, sourceType) in zip(rerunDataFrames, sourceTypes):
            if sourceType == "galaxies":
                gals = (rerunDataFrame["class"] == 1.0)
                toJoin.append(rerunDataFrame[gals])
            elif sourceType == "stars":
                stars = (rerunDataFrame["class"] == 0.0)
                toJoin.append(rerunDataFrame[stars])
            elif sourceType == "all":
                toJoin.append(rerunDataFrame)
            else:
                raise RuntimeError("Unallowed source type")

        tractDataFrame = pd.concat(toJoin)
        return tractDataFrame
