# This file is part of qa_explorer
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""Command-line task and associated config for writing QA tables.

The deepCoadd_qa table is a table with QA columns of interest computed
for all filters for which the deepCoadd_obj tables are written.
"""
import os
from lsst.utils import getPackageDir

from lsst.pipe.tasks.postprocess import TransformObjectCatalogTask, TransformObjectCatalogConfig


class WriteQATableConfig(TransformObjectCatalogConfig):
    def setDefaults(self):
        self.functorFile = os.path.join(getPackageDir("qa_explorer"),
                                        'data', 'QAfunctors.yaml')


class WriteQATableTask(TransformObjectCatalogTask):
    """Compute columns of QA interest from coadd object tables

    Only difference from `TransformObjectCatalogTask` is the default
    YAML functor file.
    """
    _DefaultName = "writeQATable"
    ConfigClass = WriteQATableConfig

    inputDataset = 'deepCoadd_obj'
    outputDataset = 'deepCoadd_qa'
