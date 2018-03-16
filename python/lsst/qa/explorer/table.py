
import pandas as pd
import fastparquet
from astropy.table import Table

class ParquetTable(object):
    """Shim to use butler to write pandas dataframe to parquet file

    Parameters
    ----------
    df : pandas.DataFrame

    """
    index_col = 'id'
    def __init__(self, df):
        self.df = df

    def writeFits(self, filename):
        """Write dataframe to parquet (not FITS).
        """
        fastparquet.write(filename, self.df)

    @classmethod
    def readFits(cls, filename):
        """Read parquet file (not FITS) into pandas DataFrame.
        """
        return pd.read_parquet(filename)