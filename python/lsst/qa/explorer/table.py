
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fastparquet
from astropy.table import Table

class ParquetTable(object):
    """Write/read a pandas dataframe to/from parquet file

    Parameters
    ----------
    df : pandas.DataFrame

    """
    def __init__(self, df, engine='pyarrow'):
        self.df = df
        self.engine = engine

    # def writeParquet(self, filename):
    #     """Write dataframe to parquet (not FITS).
    #     """
    #     if self.engine=='pyarrow':
    #         table = pa.Table.from_pandas(self.df)
    #         pq.write_table(table, filename, compression='none')
    #     elif self.engine=='fastparquet':
    #         fastparquet.write(filename, self.df)

    # @classmethod
    # def readParquet(cls, filename):
    #     """Read parquet file (not FITS) into pandas DataFrame.
    #     """
    #     return pd.read_parquet(filename, engine='pyarrow')