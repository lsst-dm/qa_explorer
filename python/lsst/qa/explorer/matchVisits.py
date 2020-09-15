#
# This file is part of qa_explorer.
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
"""This module implements a task to match visit sources to coadd sources and save a table with that info.

Table is saved as `visitMatchTable` dataset.  


"""
import pandas as pd
import numpy as np

from lsst.pex.config import Config, Field
from lsst.pipe.base import CmdLineTask, ArgumentParser
from lsst.daf.persistence import NoResults
from lsst.qa.explorer.match import match_lists
from lsst.pipe.drivers.utils import TractDataIdContainer

from lsst.pipe.tasks.parquetTable import ParquetTable


__all__ = ["MatchVisitsConfig", "MatchVisitsTask"]


class MatchVisitsConfig(Config):
    coaddName = Field(dtype=str, default="deep", doc="Name of coadd")
    matchRadius = Field(dtype=float, default=0.2, doc="match radius in arcseconds")


class MatchVisitsTask(CmdLineTask):
    """Write a tract-level table of closest-match visit IDs

    Run this task on a tract, and it writes a full-tract `visitMatchTable` (a ParquetTable)
    of coadd -> visit match ids and match distances, organized with a multi-level index.

    Example usage:

        matchVisits.py <repo> --output <output_repo> --id tract=9615 filter=HSC-I

    """

    _DefaultName = "matchVisits"
    ConfigClass = MatchVisitsConfig

    inputDataset = "analysisCoaddTable_forced"
    outputDataset = "visitMatchTable"

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

    def matchCats(self, df1, df2, raColumn="coord_ra", decColumn="coord_dec"):
        """Match two catalogs, represented as dataframes

        This uses the `match_lists` function, that uses a KDTree for matching.

        Parameters
        ----------
        df1, df2 : pandas.DataFrame
            Catalogs to match

        raColumn, decColumn : str
            Names of ra and dec columns

        Returns
        -------
        good : `numpy.ndarray` (bool)
            Boolean array indicating which indices of df1 have valid matches

        matchId : `numpy.ndarray` (dtype int)
            Index of closest source in df2 to each source in df1.

        distance : `numpy.ndarray` (dtype float)
            Distance of match
        """
        ra1, dec1 = df1[raColumn].values, df1[decColumn].values
        ra2, dec2 = df2[raColumn].values, df2[decColumn].values

        dist, inds = match_lists(ra1, dec1, ra2, dec2, self.config.matchRadius / 3600)

        good = np.isfinite(dist)  # sometimes dist is inf, sometimes nan.
        id2 = df2.index

        return good, id2[inds[good]], dist[good] * 3600.0

    def runDataRef(self, patchRefList):
        """Matches visits to coadd and writes output

        Visits to match are chosen by taking all input coadd patches (collected from
        requested tract), and querying for all visits used to construct that coadd.
        The set of total visits to put in the match table is union of all 

        Parameters
        ----------
        patchRefList : `list`
            List of patch datarefs from which visits will be selected.

        Returns
        -------
        matchDf : `pandas.DataFrame`
            Dataframe of match data.  Column index is multi-level, with the first
            level being visit number, and second level being `['matchId', 'distance']`.

        """
        butler = patchRefList[0].getButler()
        tract = patchRefList[0].dataId["tract"]
        filt = patchRefList[0].dataId["filter"]

        # Collect all visits that overlap any part of the requested tract
        allVisits = set()
        for patchRef in patchRefList:
            try:
                exp = butler.get("deepCoadd_calexp", dataId=patchRef.dataId)
                allVisits = allVisits.union(set(exp.getInfo().getCoaddInputs().visits["id"]))
            except NoResults:
                pass
        self.log.info("matching {} visits to tract {}: {}".format(len(allVisits), tract, allVisits))

        # Match
        columns = ["coord_ra", "coord_dec"]
        coaddDf = (butler.get(self.inputDataset, tract=tract, filter=filt, subdir="").
                   toDataFrame(columns=columns))

        column_index = pd.MultiIndex.from_product([["matchId", "distance"], allVisits])
        matchDf = pd.DataFrame(columns=column_index, index=coaddDf.index)
        for i, visit in enumerate(allVisits):
            try:
                visitDf = (butler.get("analysisVisitTable", tract=tract, filter=filt, visit=visit, subdir="").
                           toDataFrame(columns=columns))
            except NoResults:
                self.log.info(f"({i+1} of {len(allVisits)}) visit {visit}: analysisVisitTable not available")
                continue

            good, ids, distance = self.matchCats(coaddDf, visitDf)

            matchDf.loc[good, ("matchId", visit)] = ids
            matchDf.loc[good, ("distance", visit)] = distance
            self.log.info(
                "({} of {}) visit {}: {} sources matched.".format(i + 1, len(allVisits), visit, good.sum())
            )

        butler.put(ParquetTable(dataFrame=matchDf), self.outputDataset, tract=tract, filter=filt)
        return matchDf

    def writeMetadata(self, dataRef):
        """No metadata to write.
        """
        pass
