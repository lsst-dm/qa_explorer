from __future__ import print_function, division

import pandas as pd
import numpy as np
import re
import fastparquet
import logging
import dask.dataframe as dd
from schwimmbad import choose_pool

from .catalog import Catalog

class Functor(object):
    """Performs computation on an hdf catalog, read from disk

    Catalog is assumed to be the filename of a parquet file.

    Subclasses must define _columns attribute that is read from table
    """
    force_ndarray = False

    def _get_columns(self, catalog, query=None):
        return catalog.get_columns(self.columns)

    @property
    def columns(self):
        try:
            return self._columns
        except AttributeError:
            raise NotImplementedError('Must define columns property or _columns attribute')

    def _func(self, df):
        raise NotImplementedError('Must define calculation on in-memory dataframe')

    def __call__(self, catalog, query=None, dropna=False):
        # First read what we need into memory,
        #  Then perform the calculation.
        if isinstance(catalog, pd.DataFrame):
            df = catalog
        elif isinstance(catalog, dd.DataFrame):
            df = catalog
        else:
            df = self._get_columns(catalog, query=query)

        if query:
            df = df.query(query)

        vals = self._func(df)

        if dropna:
            vals = vals.replace([np.inf, -np.inf], np.nan).dropna(how='any')

        if self.force_ndarray:
            return np.array(vals)
        else:
            return vals

class ParquetReadWorker(object):
    def __init__(self, cols):
        self.cols = cols

    def __call__(self, filename):
        pfile = fastparquet.ParquetFile(filename)
        return pfile.to_pandas(columns=self.cols)

class CompositeFunctor(Functor):
    force_ndarray = False

    def __init__(self, funcDict):
        self.funcDict = funcDict

    @property 
    def columns(self):
        return [x for y in [f.columns for f in self.funcDict.values()] for x in y]

    def _func(self, df):
        # Need to preserve index here...

        return {k : f(df) for k,f in self.funcDict.items()}

    def __getitem__(self, item):
        return self.funcDict[item]

class TestFunctor(Functor):
    name = 'test'

    def __init__(self, n=None, seed=1234):
        self.n = n
        self.seed = seed

    def __call__(self, catalog):
        np.random.seed(self.seed)
        n = len(catalog) if self.n is None else self.n
        u = np.random.random(n)
        x = np.ones(n)
        x[u < 0.5] = -1
        return x

def mag_aware_eval(df, expr):
    # pfile = fastparquet.ParquetFile(filename)
    # cols = [c for c in pfile.columns if re.search(c, expr)]
    # df = pfile.to_pandas(columns=cols)    
    try:
        expr_new = re.sub('mag\((\w+)\)', '-2.5*log(\g<1>)/log(10)', expr)
        val = df.eval(expr_new, truediv=True)
    except:
        expr_new = re.sub('mag\((\w+)\)', '-2.5*log(\g<1>_flux)/log(10)', expr)
        val = df.eval(expr_new, truediv=True)
    return val

class CustomFunctor(Functor):
    _ignore_words = ('mag', 'sin', 'cos', 'exp', 'log')

    def __init__(self, expr):
        self.expr = expr

    @property
    def name(self):
        return self.expr

    @property
    def columns(self):
        flux_cols = re.findall('mag\(\s*(\w+)\s*\)', self.expr)

        cols = [c for c in re.findall('\w+', self.expr) if c not in self._ignore_words]
        not_a_col = []
        for c in flux_cols:
            if not re.search('_flux$', c):
                cols.append('{}_flux'.format(c))
                not_a_col.append(c)
            else:
                cols.append(c)

        return list(set([c for c in cols if c not in not_a_col]))

    def _func(self, df):
        return mag_aware_eval(df, self.expr)

class Column(Functor):
    def __init__(self, col):
        self.col = col

    @property
    def name(self):
        return self.col

    @property
    def columns(self):
        return [self.col]

    def _func(self, df):
        return df[self.col]

class IDColumn(Column):
    col = 'id'

class FootprintNPix(Column):
    col = 'base_Footprint_nPix'

class CoordColumn(Column):
    def _func(self, df):
        return np.rad2deg(df[self.col])

class RAColumn(CoordColumn):
    name = 'RA'
    def __init__(self):
        self.col = 'coord_ra'

class DecColumn(CoordColumn):
    name = 'Dec'
    def __init__(self):
        self.col = 'coord_dec'

def fluxName(col):
    if not col.endswith('_flux'):
        col += '_flux'
    return col

class Mag(Functor):
    def __init__(self, col):
        self.col = fluxName(col)

    @property
    def columns(self):
        return [self.col]

    def _func(self, df):
        return -2.5*np.log10(df[self.col])

    @property
    def name(self):
        return 'mag_{0}'.format(self.col)

class MagDiff(Functor):
    """Functor to calculate magnitude difference"""
    def __init__(self, col1, col2):
        self.col1 = fluxName(col1)
        self.col2 = fluxName(col2)

    @property
    def columns(self):
        return [self.col1, self.col2]

    def _func(self, df):
        return -2.5*np.log10(df[self.col1]/df[self.col2])

    @property
    def name(self):
        return '(mag_{0} - mag_{1})'.format(self.col1, self.col2)

class StarGalaxyLabeller(Functor):
    _columns = ["base_ClassificationExtendedness_value"]
    _column = "base_ClassificationExtendedness_value"

    def _func(self, df):
        return np.where(df[self._column] < 0.5, 'star', 'galaxy')


class DeconvolvedMoments(Functor):
    name = 'Deconvolved Moments'
    _columns = ("ext_shapeHSM_HsmSourceMoments_xx",
                "ext_shapeHSM_HsmSourceMoments_yy",
                "base_SdssShape_xx", "base_SdssShape_yy",
                # "ext_shapeHSM_HsmSourceMoments",
                "ext_shapeHSM_HsmPsfMoments_xx",
                "ext_shapeHSM_HsmPsfMoments_yy")

    def __init__(self, pandas=True, **kwargs):
        self.pandas = pandas

    def _func(self, df):
        """Calculate deconvolved moments"""
        if "ext_shapeHSM_HsmSourceMoments_xx" in df.columns: # _xx added by tdm
            hsm = df["ext_shapeHSM_HsmSourceMoments_xx"] + df["ext_shapeHSM_HsmSourceMoments_yy"]
        else:
            hsm = np.ones(len(df))*np.nan
        sdss = df["base_SdssShape_xx"] + df["base_SdssShape_yy"]
        if "ext_shapeHSM_HsmPsfMoments_xx" in df.columns:
            psf = df["ext_shapeHSM_HsmPsfMoments_xx"] + df["ext_shapeHSM_HsmPsfMoments_yy"]
        else:
            # LSST does not have shape.sdss.psf.  Could instead add base_PsfShape to catalog using
            # exposure.getPsf().computeShape(s.getCentroid()).getIxx()
            # raise TaskError("No psf shape parameter found in catalog")
            raise RuntimeError('No psf shape parameter found in catalog')

        if self.pandas:
            return hsm.where(np.isfinite(hsm), sdss) - psf
        else:
            return np.where(np.isfinite(hsm), hsm, sdss) - psf

class SdssTraceSize(Functor):
    """Functor to calculate SDSS trace radius size for sources"""
    name = "SDSS Trace Size"
    _columns = ("base_SdssShape_xx", "base_SdssShape_yy")
    def _func(self, df):
        srcSize = np.sqrt(0.5*(df["base_SdssShape_xx"] + df["base_SdssShape_yy"]))
        return srcSize


class PsfSdssTraceSizeDiff(Functor):
    """Functor to calculate SDSS trace radius size difference (%) between object and psf model"""
    name = "PSF - SDSS Trace Size"
    _columns = ("base_SdssShape_xx", "base_SdssShape_yy",
                "base_SdssShape_psf_xx", "base_SdssShape_psf_yy")

    def _func(self, df):
        srcSize = np.sqrt(0.5*(df["base_SdssShape_xx"] + df["base_SdssShape_yy"]))
        psfSize = np.sqrt(0.5*(df["base_SdssShape_psf_xx"] + df["base_SdssShape_psf_yy"]))
        sizeDiff = 100*(srcSize - psfSize)/(0.5*(srcSize + psfSize))
        return sizeDiff


class HsmTraceSize(Functor):
    """Functor to calculate HSM trace radius size for sources"""
    name = 'HSM Trace Size'
    _columns = ("ext_shapeHSM_HsmSourceMoments_xx",
                "ext_shapeHSM_HsmSourceMoments_yy")
    def _func(self, df):
        srcSize = np.sqrt(0.5*(df["ext_shapeHSM_HsmSourceMoments_xx"] +
                               df["ext_shapeHSM_HsmSourceMoments_yy"]))
        return srcSize


class PsfHsmTraceSizeDiff(Functor):
    """Functor to calculate HSM trace radius size difference (%) between object and psf model"""
    name = 'PSF - HSM Trace Size'
    _columns = ("ext_shapeHSM_HsmSourceMoments_xx",
                "ext_shapeHSM_HsmSourceMoments_yy",
                "ext_shapeHSM_HsmPsfMoments_xx",
                "ext_shapeHSM_HsmPsfMoments_yy")

    def _func(self, df):
        srcSize = np.sqrt(0.5*(df["ext_shapeHSM_HsmSourceMoments_xx"] +
                               df["ext_shapeHSM_HsmSourceMoments_yy"]))
        psfSize = np.sqrt(0.5*(df["ext_shapeHSM_HsmPsfMoments_xx"] +
                               df["ext_shapeHSM_HsmPsfMoments_yy"]))
        sizeDiff = 100*(srcSize - psfSize)/(0.5*(srcSize + psfSize))
        return sizeDiff
