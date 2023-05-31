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


import copy
import functools
import unittest

import lsst.utils.tests
import numpy as np
import pandas as pd
from lsst.pipe.base import InMemoryDatasetHandle
from lsst.qa.explorer.functors import StarGalaxyLabeller
from lsst.sphgeom import HtmPixelization


class FunctorTestCase(unittest.TestCase):
    def getMultiIndexDataFrame(self, dataDict):
        """Create a simple test multi-index DataFrame."""

        simpleDF = pd.DataFrame(dataDict)
        dfFilterDSCombos = []
        for ds in self.datasets:
            for band in self.bands:
                df = copy.copy(simpleDF)
                df.reindex(sorted(df.columns), axis=1)
                df["dataset"] = ds
                df["band"] = band
                df.columns = pd.MultiIndex.from_tuples(
                    [(ds, band, c) for c in df.columns],
                    names=("dataset", "band", "column"),
                )
                dfFilterDSCombos.append(df)

        df = functools.reduce(lambda d1, d2: d1.join(d2), dfFilterDSCombos)

        return df

    def getDatasetHandle(self, df):
        lo, hi = HtmPixelization(7).universe().ranges()[0]
        value = np.random.randint(lo, hi)
        return InMemoryDatasetHandle(
            df, storageClass="DataFrame", dataId={"htm7": value}
        )

    def setUp(self):
        np.random.seed(12345)
        self.datasets = ["forced_src", "meas", "ref"]
        self.bands = ["g", "r"]
        self.columns = ["coord_ra", "coord_dec"]
        self.nRecords = 5
        self.dataDict = {
            "coord_ra": [3.77654137, 3.77643059, 3.77621148, 3.77611944, 3.77610396],
            "coord_dec": [0.01127624, 0.01127787, 0.01127543, 0.01127543, 0.01127543],
        }

    def _funcVal(self, functor, df):
        self.assertIsInstance(functor.name, str)
        self.assertIsInstance(functor.shortname, str)

        handle = self.getDatasetHandle(df)

        val = functor(df)
        val2 = functor(handle)
        self.assertTrue((val == val2).all())
        self.assertIsInstance(val, pd.Series)

        val = functor(df, dropna=True)
        val2 = functor(handle, dropna=True)
        self.assertTrue((val == val2).all())
        self.assertEqual(val.isnull().sum(), 0)

        return val

    def testLabeller(self):
        # Covering the code is better than nothing
        self.columns.append("base_ClassificationExtendedness_value")
        self.dataDict["base_ClassificationExtendedness_value"] = np.full(
            self.nRecords, 1
        )
        df = self.getMultiIndexDataFrame(self.dataDict)
        _ = self._funcVal(StarGalaxyLabeller(), df)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
