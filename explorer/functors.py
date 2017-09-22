from __future__ import print_function, division

import pandas as pd
import numpy as np
import re
import fastparquet
import logging
import dask.dataframe as dd
import dask.array as da
import dask
import time

from .catalog import Catalog

class Functor(object):
    """Performs computation on catalog(s), read from disk

    Catalog is a Catalog object

    Subclasses must define _columns attribute that is read from table
    """

    @property
    def columns(self):
        try:
            return self._columns
        except AttributeError:
            raise NotImplementedError('Must define columns property or _columns attribute')

    def _func(self, df, dropna=True):
        raise NotImplementedError('Must define calculation on dataframe')

    def __call__(self, catalog, query=None, dropna=True, dask=False):

        df = catalog.get_columns(self.columns, query=query)
        vals = self._func(df)

        if dropna:
            try:
                if catalog.client:
                    vals = catalog.client.compute(vals[da.isfinite(vals)]).result()
                else:
                    vals = vals[da.isfinite(vals)]
            except TypeError:
                if catalog.client:
                    vals = catalog.client.compute(vals[da.notnull(vals)]).result()
                else:
                    vals = vals[da.notnull(vals)]

        if dask:
            return vals
        else:
            if hasattr(vals, 'compute'):
                return vals.compute()
            elif hasattr(vals, 'result'):
                return vals.result()
            else:
                return vals

    def test(self, catalog, dropna=True, dask=False):
        start = time.time()
        res = self(catalog, dropna=dropna, dask=dask)
        if hasattr(res, 'result'):
            n = len(res.result())
        elif hasattr(res, 'compute'):
            n = len(res.compute())
        else:
            n = len(res)
        end = time.time()
        
        runtime = end - start
        print('Test results for {}:'.format(self.name))
        print('  Took {:.2f}s, length={}.  Type={}'.format(runtime, n, type(res)))    
        return runtime

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

    def __call__(self, catalog, dask=False, **kwargs):
        df = pd.DataFrame({k : f(catalog, dask=dask, **kwargs) 
                            for k,f in self.funcDict.items()})
        if dask:
            return dd.from_pandas(df, chunksize=1000000)
        else:
            return df

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
    _ignore_words = ('mag', 'sin', 'cos', 'exp', 'log', 'sqrt')

    def __init__(self, expr):
        self.expr = expr

    @property
    def name(self):
        return self.expr

    @property
    def columns(self):
        flux_cols = re.findall('mag\(\s*(\w+)\s*\)', self.expr)

        cols = [c for c in re.findall('[a-zA-Z_]+', self.expr) if c not in self._ignore_words]
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
        return df[self.col] * 180 / np.pi

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
        return -2.5*da.log10(df[self.col])

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
        return -2.5*da.log10(df[self.col1]/df[self.col2])

    @property
    def name(self):
        return '(mag_{0} - mag_{1})'.format(self.col1, self.col2)

class Labeller(Functor):
    """Main function of this subclass is to override the dropna=True
    """
    _null_label = 'null'
    name = 'label'
    def __call__(self, catalog, dropna=False, **kwargs):
        return super(Labeller, self).__call__(catalog, dropna=False, **kwargs)

        # Use below if we actually want to not label some things. Could be weird.
        # vals = super(Labeller, self).__call__(catalog, dropna=False, **kwargs)
        # if dropna:
        #     return vals[vals != self._null_label]
        # else:
        #     return vals


class StarGalaxyLabeller(Labeller):
    _columns = ["base_ClassificationExtendedness_value"]
    _column = "base_ClassificationExtendedness_value"

    def _func(self, df):
        x = df[self._columns][self._column]
        mask = x.isnull()
        test = (x < 0.5).astype(int)
        test = test.mask(mask, 2)
        #are these backwards?
        label = pd.Series(pd.Categorical.from_codes(test, categories=['galaxy', 'star', self._null_label]), 
                            index=x.index, name='label')
        return label
        # return np.where(df[self._column] < 0.5, 'star', 'galaxy')


class DeconvolvedMoments(Functor):
    name = 'Deconvolved Moments'
    _columns = ("ext_shapeHSM_HsmSourceMoments_xx",
                "ext_shapeHSM_HsmSourceMoments_yy",
                "base_SdssShape_xx", "base_SdssShape_yy",
                # "ext_shapeHSM_HsmSourceMoments",
                "ext_shapeHSM_HsmPsfMoments_xx",
                "ext_shapeHSM_HsmPsfMoments_yy")

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

        return hsm.where(da.isfinite(hsm), sdss) - psf

class SdssTraceSize(Functor):
    """Functor to calculate SDSS trace radius size for sources"""
    name = "SDSS Trace Size"
    _columns = ("base_SdssShape_xx", "base_SdssShape_yy")

    def _func(self, df):
        srcSize = da.sqrt(0.5*(df["base_SdssShape_xx"] + df["base_SdssShape_yy"]))
        return srcSize


class PsfSdssTraceSizeDiff(Functor):
    """Functor to calculate SDSS trace radius size difference (%) between object and psf model"""
    name = "PSF - SDSS Trace Size"
    _columns = ("base_SdssShape_xx", "base_SdssShape_yy",
                "base_SdssShape_psf_xx", "base_SdssShape_psf_yy")

    def _func(self, df):
        srcSize = da.sqrt(0.5*(df["base_SdssShape_xx"] + df["base_SdssShape_yy"]))
        psfSize = da.sqrt(0.5*(df["base_SdssShape_psf_xx"] + df["base_SdssShape_psf_yy"]))
        sizeDiff = 100*(srcSize - psfSize)/(0.5*(srcSize + psfSize))
        return sizeDiff


class HsmTraceSize(Functor):
    """Functor to calculate HSM trace radius size for sources"""
    name = 'HSM Trace Size'
    _columns = ("ext_shapeHSM_HsmSourceMoments_xx",
                "ext_shapeHSM_HsmSourceMoments_yy")
    def _func(self, df):
        srcSize = da.sqrt(0.5*(df["ext_shapeHSM_HsmSourceMoments_xx"] +
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
        srcSize = da.sqrt(0.5*(df["ext_shapeHSM_HsmSourceMoments_xx"] +
                               df["ext_shapeHSM_HsmSourceMoments_yy"]))
        psfSize = da.sqrt(0.5*(df["ext_shapeHSM_HsmPsfMoments_xx"] +
                               df["ext_shapeHSM_HsmPsfMoments_yy"]))
        sizeDiff = 100*(srcSize - psfSize)/(0.5*(srcSize + psfSize))
        return sizeDiff
