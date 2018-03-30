
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
