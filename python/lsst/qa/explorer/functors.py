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

import pandas as pd
from lsst.pipe.tasks.functors import Functor

__all__ = ["Labeller", "StarGalaxyLabeller"]


class Labeller(Functor):
    """Main function of this subclass is to override the dropna=True"""

    _null_label = "null"
    _allow_difference = False
    name = "label"
    _force_str = False

    def __call__(self, parq, dropna=False, **kwargs):
        return super().__call__(parq, dropna=False, **kwargs)


class StarGalaxyLabeller(Labeller):
    _columns = ["base_ClassificationExtendedness_value"]
    _column = "base_ClassificationExtendedness_value"

    def _func(self, df):
        x = df[self._columns][self._column]
        mask = x.isnull()
        test = (x < 0.5).astype(int)
        test = test.mask(mask, 2)

        categories = ["galaxy", "star", self._null_label]
        label = pd.Series(
            pd.Categorical.from_codes(test, categories=categories),
            index=x.index,
            name="label",
        )
        if self._force_str:
            label = label.astype(str)
        return label
