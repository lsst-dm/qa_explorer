#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

from __future__ import print_function

import os, shutil
import unittest
import tempfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pandas.util.testing import assert_frame_equal

import lsst.utils.tests
import lsst.afw.geom as afwGeom

from lsst.qa.explorer.parquetTable import ParquetTable, MultilevelParquetTable

ROOT = os.path.abspath(os.path.dirname(__file__))

def setup_module(module):
    lsst.utils.tests.init()

class ParquetTableTestCase(unittest.TestCase):
    """Test case for ParquetTable
    """
    test_filename = 'simple_test.parq'
    def setUp(self):
        self.df = pq.read_table(os.path.join(ROOT, testFilename)).to_pandas()
        self.tempDir = tempfile.TemporaryDirectory()
        filename = os.path.join(self.tempDir, self.testFilename)
        self.parq = ParquetTable(filename)

    def tearDown(self):
        del self.df
        del self.parq
        shutil.rmtree(self.tempDir)

    def testRoundTrip(self):    
        assert_frame_equal(self.parq.to_df(), self.df)

    def testColumns(self):
        columns = ['coord_ra', 'coord_dec']
        assert_frame_equal(parq.to_df(columns=columns), 
                           df[columns])
        assert_frame_equal(parq.to_df(columns=columns+['hello']),
                           df[columns])

class MultilevelParquetTableTestCase(ParquetTableTestCase):
    """Test case for MultilevelParquetTable
    """

    def setUp(self):
        super(MultilevelParquetTableTestCase, self).setUp()

        self.datasets = ['meas', 'ref']
        self.filters = ['HSC-G', 'HSC-R']
        self.columns = ['coord_ra', 'coord_dec']

    def testProperties(self):
        assert(all([x==y for x,y in zip(self.parq.columnLevels, self.df.columns.names)]))
        assert(len(self.parq.columns)==len(self.df.columns))

    def testColumns(self):

        # Case A, each level has multiple values
        datasets_A = self.datasets
        filters_A = self.filters
        columns_A = self.columns
        columnDict_A = {'dataset':datasets_A,
                       'filter':filters_A,
                       'column':columns_A}
        colTuples_A = [(self.datasets[0], self.filters[0], self.columns[0]), 
                       (self.datasets[0], self.filters[0], self.columns[1]), 
                       (self.datasets[0], self.filters[1], self.columns[0]), 
                       (self.datasets[0], self.filters[1], self.columns[1]), 
                       (self.datasets[1], self.filters[0], self.columns[0]), 
                       (self.datasets[1], self.filters[0], self.columns[1]), 
                       (self.datasets[1], self.filters[1], self.columns[0]), 
                       (self.datasets[1], self.filters[1], self.columns[1])]
        df_A = df[colTuples_A]
        assert_frame_equal(parq.to_df(columns=columnDict_A), df_A)    

        # Case B: One level has only a single value
        datasets_B = self.datasets[0]
        filters_B = self.filters
        columns_B = self.columns
        columnDict_B = {'dataset':datasets_B,
                       'filter':filters_B,
                       'column':columns_B}
        colTuples_B = [(self.datasets[0], self.filters[0], self.columns[0]), 
                       (self.datasets[0], self.filters[0], self.columns[1]), 
                       (self.datasets[0], self.filters[1], self.columns[0]), 
                       (self.datasets[0], self.filters[1], self.columns[1])]
        df_B = df[colTuples_B]
        df_B.columns = df_B.columns.droplevel('dataset')
        assert_frame_equal(parq.to_df(columns=columnDict_B), df_B) 
        assert_frame_equal(df_B, parq.to_df(columns=colTuples_B))
        
        # Case C: Two levels have a single value; third is not provided
        datasets_C = self.datasets[0]
        filters_C = self.filters[0]
        columnDict_C = {'dataset':datasets_C,
                       'filter':filters_C}
        df_C = df[datasets_C][filters_C]
        assert_frame_equal(parq.to_df(columns=columnDict_C), df_C) 

        # Case D: Only one level (first level) is provided
        dataset_D = self.datasets[0]
        columnDict_D = {'dataset':dataset_D}
        df_D = df[dataset_D]
        assert_frame_equal(parq.to_df(columns=columnDict_D), df_D) 

        # Case E: Only one level (second level) is provided
        filters_E = self.filters[1]
        columnDict_E = {'filter':filters_E}
        # get second level of multi-index column using .xs()
        df_E = df.xs(filters_E, level=1, axis=1) 
        assert_frame_equal(parq.to_df(columns=columnDict_E), df_E) 


