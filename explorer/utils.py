from __future__ import print_function, division

import pandas as pd
import numpy as np
import re

class Functor(object):
    def __call__(self, catalog):
        return np.array(self._func(catalog))

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
    try:
        expr_new = re.sub('mag\((\w+)\)', '-2.5*log(\g<1>)/log(10)', expr)
        val = df.eval(expr_new, truediv=True)
    except:
        expr_new = re.sub('mag\((\w+)\)', '-2.5*log(\g<1>_flux)/log(10)', expr)
        val = df.eval(expr_new, truediv=True)
    return val

class CustomFunctor(Functor):
    def __init__(self, expr):
        self.expr = expr

    @property
    def name(self):
        return self.expr

    def _func(self, catalog):
        return mag_aware_eval(catalog, self.expr)

class Column(Functor):
    def __init__(self, col):
        self.col = col

    @property
    def name(self):
        return self.col

    def _func(self, catalog):
        return catalog[self.col]

class CoordColumn(Column):
    def _func(self, catalog):
        return np.rad2deg(catalog[self.col])

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

    def _func(self, catalog):
        return -2.5*np.log10(catalog[self.col])

    @property
    def name(self):
        return 'mag_{0}'.format(self.col)

class MagDiff(Functor):
    """Functor to calculate magnitude difference"""
    def __init__(self, col1, col2):
        self.col1 = fluxName(col1)
        self.col2 = fluxName(col2)

    def _func(self, catalog):
        return -2.5*np.log10(catalog[self.col1]/catalog[self.col2])

    @property
    def name(self):
        return '(mag_{0} - mag_{1})'.format(self.col1, self.col2)

class StarGalaxyLabeller(object):
    _column = "base_ClassificationExtendedness_value"
    def __call__(self, catalog):
        return np.where(catalog[self._column] < 0.5, 'star', 'galaxy')

class Sizer(object):
    def __init__(self, size):
        self.size = size

    def __call__(self, catalog):
        return np.ones(len(catalog)) * self.size

class DeconvolvedMoments(Functor):
    name = 'Deconvolved Moments'

    def __init__(self, *args, **kwargs):
        pass

    def _func(self, catalog):
        """Calculate deconvolved moments"""
        if "ext_shapeHSM_HsmSourceMoments" in catalog:
            hsm = catalog["ext_shapeHSM_HsmSourceMoments_xx"] + catalog["ext_shapeHSM_HsmSourceMoments_yy"]
        else:
            hsm = np.ones(len(catalog))*np.nan
        sdss = catalog["base_SdssShape_xx"] + catalog["base_SdssShape_yy"]
        if "ext_shapeHSM_HsmPsfMoments_xx" in catalog:
            psf = catalog["ext_shapeHSM_HsmPsfMoments_xx"] + catalog["ext_shapeHSM_HsmPsfMoments_yy"]
        else:
            # LSST does not have shape.sdss.psf.  Could instead add base_PsfShape to catalog using
            # exposure.getPsf().computeShape(s.getCentroid()).getIxx()
            # raise TaskError("No psf shape parameter found in catalog")
            raise RuntimeError('No psf shape parameter found in catalog')
        return np.where(np.isfinite(hsm), hsm, sdss) - psf

