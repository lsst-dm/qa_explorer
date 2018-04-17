"""
Implementation of thin wrappers to pyarrow.ParquetFile.
"""
import re
import json
from itertools import product

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class ParquetTable(object):
    """Thin wrapper to pyarrow's ParquetFile object

    Call `to_df` method to get a `pandas.DataFrame` object,
    optionally passing specific columns.

    Parameters
    ----------
    filename : str
        Path to Parquet file.

    """
    def __init__(self, filename=None, dataFrame=None):
        if filename is not None:
            self._pf = pq.ParquetFile(filename)
            self._df = None
            self._pandas_md = None
        elif dataFrame is not None:
            self._df = dataFrame
            self._pf = None
        else:
            raise ValueError('Either filename or dataFrame must be passed.')

        self._columns = None
        self._columnIndex = None

    @property
    def df(self):
        return self._df

    @property
    def pf(self):
        return self._pf

    def write(self, filename):
        """Write pandas dataframe to parquet

        Parameters
        ----------
        df : `pandas.DataFrame`
            Dataframe to write to Parquet file.

        filename : str
            Path to which to write.
        """
        if self.df is None:
            raise ValueError('df property must be defined to write.')
        table = pa.Table.from_pandas(self.df)
        pq.write_table(table, filename, compression='none')

    @property
    def pandas_md(self):
        """Pandas metadata as a dictionary.
        """
        if self.pf is None:
            raise ValueError('pf property must be defined to access this attribute.')
        if self._pandas_md is None:
            self._pandas_md = json.loads(self.pf.metadata.metadata[b'pandas'])
        return self._pandas_md

    @property
    def columnIndex(self):
        """Columns as a pandas Index
        """
        if self._columnIndex is None:
            self._columnIndex = self._get_columnIndex()
        return self._columnIndex
    
    def _get_columnIndex(self):
        if self.df is not None:
            return self.df.columns
        else:
            return pd.Index(self.columns)
    
    @property
    def columns(self):
        """List of column names (or column index if df is set)
        """
        if self._columns is None:
            self._columns = self._get_columns()
        return self._columns
    
    def _get_columns(self):
        if self.df is not None:
            return self.df.columns
        else:
            return self.pf.metadata.schema.names
    
    def _sanitizeColumns(self, columns):
        return [c for c in columns if c in self.columnIndex]

    def to_df(self, columns=None):
        """Get table (or specified columns) as a pandas DataFrame

        Parameters
        ----------
        columns : list, optional
            Desired columns.  If `None`, then all columns will be
            returned.  
        """
        if self.pf is None:
            raise ValueError('Only call this function if .pf is defined.')

        if columns is None:
            return self.pf.read().to_pandas()
        
        try:
            df = self.pf.read(columns=columns, use_pandas_metadata=True).to_pandas()
        except AttributeError:
            columns = self._sanitizeColumns(columns)
            df = self.pf.read(columns=columns, use_pandas_metadata=True).to_pandas()

        return df


class MultilevelParquetTable(ParquetTable):
    """Wrapper to access dataframe with multi-level column index from Parquet

    This subclass of `ParquetTable` to handle the multi-level is necessary
    because there is not a convenient way to request specific table subsets
    by level via Parquet through pyarrow, as there is with a `pandas.DataFrame`.

    Additionally, pyarrow stores multilevel index information in a very strange way.
    Pandas stores it as a tuple, so that one can access a single column from a pandas 
    dataframe as `df[('ref', 'HSC-G', 'coord_ra')]`.  However, for some reason
    pyarrow saves these indices as "stringified" tuples, such that in order to read this 
    same column from a table written to Parquet, you would have to do the following:

        pf = pyarrow.ParquetFile(filename)
        df = pf.read(columns=["('ref', 'HSC-G', 'coord_ra')"])

    See also https://github.com/apache/arrow/issues/1771, where I've raised this issue.
    I don't know if this is a bug or intentional, and it may be addressed in the future.

    As multilevel-indexed dataframes can be very useful to store data like multiple filters'
    worth of data in the same table, this case deserves a wrapper to enale easier access;
    that's what this object is for.  For example,

        parq = MultilevelParquetTable(filename)
        columnDict = {'dataset':'meas',
                      'filter':'HSC-G',
                      'column':['coord_ra', 'coord_dec']}
        df = parq.to_df(columns=columnDict)

    will return just the coordinate columns; the equivalent of calling
    `df['meas']['HSC-G'][['coord_ra', 'coord_dec']]` on the total dataframe,
    but without having to load the whole frame into memory---this reads just those 
    columns from disk.  You can also request a sub-table; e.g.,

        parq = MultilevelParquetTable(filename)
        columnDict = {'dataset':'meas',
                      'filter':'HSC-G'}
        df = parq.to_df(columns=columnDict)

    and this will be the equivalent of `df['meas']['HSC-G']` on the total dataframe.
    

    Parameters
    ----------
    filename : str
        Path to Parquet file.

    """

    def __init__(self, *args, **kwargs):
        super(MultilevelParquetTable, self).__init__(*args, **kwargs)

        self._columnLevelNames = None

    @property
    def columnLevelNames(self):
        if self._columnLevelNames is None:
            self._columnLevelNames = {level : list(np.unique(np.array(self.columns)[:,i])) 
                                      for i,level in enumerate(self.columnLevels)}

    @property
    def columnLevels(self):
        """Names of levels in column index
        """
        return self.columnIndex.names
        
    def _get_columnIndex(self):
        if self.df is not None:
            return super(MultilevelParquetTable, self)._get_columnIndex()
        else:
            levelNames = [f['name'] for f in self.pandas_md['column_indexes']]
            return pd.MultiIndex.from_tuples(self.columns, names=levelNames)
        
    def _get_columns(self):
        if self.df is not None:
            return super(MultilevelParquetTable, self)._get_columns()
        else:
            columns = self.pf.metadata.schema.names
            pattern = "'(.*)', '(.*)', '(.*)'"
            matches = [re.search(pattern, c) for c in columns]
            return [m.groups() for m in matches if m is not None]
    
    def to_df(self, columns=None):
        """Get table (or specified columns) as a pandas DataFrame

        To get specific columns in specified sub-levels:

            parq = MultilevelParquetTable(filename)
            columnDict = {'dataset':'meas',
                      'filter':'HSC-G',
                      'column':['coord_ra', 'coord_dec']}
            df = parq.to_df(columns=columnDict)

        Or, to get an entire subtable, leave out one level name:

            parq = MultilevelParquetTable(filename)
            columnDict = {'dataset':'meas',
                          'filter':'HSC-G'}
            df = parq.to_df(columns=columnDict)

        Parameters
        ----------
        columns : list or dict, optional
            Desired columns.  If `None`, then all columns will be
            returned.  If a list, then the names of the columns must
            be *exactly* as stored by pyarrow; that is, stringified tuples.
            If a dictionary, then the entries of the dictionary must
            correspond to the level names of the column multi-index
            (that is, the `columnLevels` attribute).  Not every level
            must be passed; if any level is left out, then all entries
            in that level will be implicitly included.
        """
        if self.pf is None:
            raise ValueError('Only call this function if .pf is defined.')

        if columns is None:
            return self.pf.read().to_pandas()
        
        if isinstance(columns, dict):
            columns = self._colsFromDict(columns)
                        
        pfColumns = self._stringify(columns)
        try:
            df = self.pf.read(columns=pfColumns, use_pandas_metadata=True).to_pandas()
        except (AttributeError, KeyError):
            newColumns = [c for c in columns if c in self.columnIndex]
            if not newColumns:
                raise ValueError('None of the requested columns ({}) are available!'.format(columns))
            pfColumns = self._stringify(newColumns)
            df = self.pf.read(columns=pfColumns, use_pandas_metadata=True).to_pandas()

        # Drop levels of column index that have just one entry
        levelsToDrop = [n for l,n in zip(df.columns.levels, df.columns.names) 
                        if len(l)==1]

        # Prevent error when trying to drop *all* columns
        if len(levelsToDrop) == len(df.columns.names):
            levelsToDrop.remove(df.columns.names[-1])

        df.columns = df.columns.droplevel(levelsToDrop)

        return df
        
    def _colsFromDict(self, colDict):
        new_colDict = {}
        for i,l in enumerate(self.columnLevels):
            if l in colDict:
                if isinstance(colDict[l], str):
                    new_colDict[l] = [colDict[l]]
                else:
                    new_colDict[l] = colDict[l]
            else:
                new_colDict[l] = self.columnIndex.levels[i]
                
        levelCols = [new_colDict[l] for l in self.columnLevels]
        cols = product(*levelCols)
        return list(cols)
        
    def _stringify(self, cols):
        return [str(c) for c in cols]

