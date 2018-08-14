import logging
import re
from functools import partial

import numpy as np
import pandas as pd
import holoviews as hv

from lsst.qa.explorer.match import match_lists
from lsst.qa.explorer.plots import filter_dset, FilterStream

class QADataset(object):

    _idNames = ('patchId', 'tractId')
    _kdims = ('ra', 'dec', 'psfMag', 'label')


    def __init__(self, df, vdims='all'):

        self._df = df
        self._vdims = vdims

        self._ds = None
        self._flags = None

#         # Extra arguments are to pass to parq.toDataFrame()
#         self._columnDict = kwargs

    @property
    def df(self):
        """Dataframe
        """
        if self._df is None:
            self._makeDataFrame()
        return self._df

    def _makeDataFrame(self):
        raise NotImplementedError('Must implement _makeDataFrame if df is not initialized.')

    @property
    def idNames(self):
        return [n for n in self._idNames if n in self.df.columns]

    @property
    def flags(self):
        if self._flags is None:
            self._flags = [c for c in self.df.columns
                            if self.df[c].dtype == np.dtype('bool')]
        return self._flags

    def _getDims(self):
        """Construct kdims and vdims for hv.Dataset object
        """
        kdims = []
        vdims = []
        for c in self.df.columns:
            if (c in self._kdims or
                c in self._idNames or
                c in self.flags):
                kdims.append(c)
            else:
                if self._vdims == 'all':
                    vdims.append(c)
                elif c in self._vdims:
                    vdims.append(c)

        return kdims, vdims

    @property
    def vdims(self):
        _, vdims = self._getDims()
        return vdims

    @property
    def kdims(self):
        kdims, _ = self._getDims()
        return kdims

    @property
    def ds(self):
        """Holoviews Dataset object
        """
        if self._ds is None:
            self._makeDataset()
        return self._ds

    def _makeDataset(self):
        kdims, vdims = self._getDims()

        df = self.df.replace([np.inf, -np.inf], np.nan).dropna(how='any')
        ds = hv.Dataset(df, kdims=kdims, vdims=vdims)
        self._ds = ds

    def skyPoints(self, vdim, maxMag, label='star', magCol='psfMag',
                     filter_range=None, flags=None, bad_flags=None):
        selectDict = {magCol : (0,maxMag),
                       'label' : label}
        ds = self.ds.select(**selectDict)
        ds = filter_dset(ds, filter_range=filter_range, flags=flags, bad_flags=bad_flags)

        pts = hv.Points(ds, kdims=['ra', 'dec'], vdims=self.vdims + [magCol] + self.idNames)
        return pts.options(color_index=vdim)

    def skyDmap(self, vdim, magRange=(np.arange(16,24.1, 0.2)), magCol='psfMag',
                filter_stream=None,
                range_override=None):
            """Dynamic map of values of a particular dimension

            Parameters
            ----------
            vdim : str
                Name of dimension to explore.

            magRange : array
                Values of faint magnitude limit.  Only points up to this limit will be plotted.
                Beware of scrolling to too faint a limit; it might give you too many points!

            filter_stream : explorer.plots.FilterStream, optional
                Stream of constraints that controls what data to display.  Useful to link
                multiple plots together

            range_override : (min, max), optional
                By default the colormap will be scaled between the 0.005 to 0.995 quantiles
                of the entire data (not just that displayed).  Sometimes this is not a useful range to view,
                so this parameter allows a custom colormap range to be set.
            """
            if filter_stream is not None:
                streams = [filter_stream]
            else:
                streams = [FilterStream()]
            fn = partial(QADataset.skyPoints, self=self, vdim=vdim, magCol=magCol)
            dmap = hv.DynamicMap(fn, kdims=['maxMag', 'label'],
                                 streams=streams)

            y_min = self.df[vdim].quantile(0.005)
            y_max = self.df[vdim].quantile(0.995)

            ra_min, ra_max = self.df.ra.quantile([0, 1])
            dec_min, dec_max = self.df.dec.quantile([0, 1])

            ranges = {vdim : (y_min, y_max),
                      'ra' : (ra_min, ra_max),
                      'dec' : (dec_min, dec_max)}
            if range_override is not None:
                ranges.update(range_override)

            dmap = dmap.redim.values(label=['galaxy', 'star'],
                                     maxMag=magRange).redim.range(**ranges)
            return dmap

class MatchedQADataset(QADataset):
    def __init__(self, data1, data2,
                 match_radius=0.5, match_registry=None,
                 **kwargs):
        self.data1 = data1
        self.data2 = data2
        self.match_radius = match_radius
        self.match_registry = match_registry

        self._matched = False
        self._match_inds1 = None
        self._match_inds2 = None
        self._match_distance = None

        self._df = None
        self._ds = None

    def _match(self):
        if not all([c in self.data1.df.columns for c in ['ra', 'dec',
                                                         'detect_isPrimary']]):
            raise ValueError('Dataframes must have `detect_isPrimary` flag, ' +
                             'as well as ra/dec.')
        isPrimary1 = self.data1.df['detect_isPrimary']
        isPrimary2 = self.data2.df['detect_isPrimary']

        ra1, dec1 = self.data1.df.ra[isPrimary1], self.data1.df.dec[isPrimary1]
        ra2, dec2 = self.data2.df.ra[isPrimary2], self.data2.df.dec[isPrimary2]
        id1 = ra1.index
        id2 = ra2.index

        dist, inds = match_lists(ra1, dec1, ra2, dec2, self.match_radius/3600)

        good = np.isfinite(dist)

        logging.info('{0} matched within {1} arcsec, {2} did not.'.format(good.sum(), self.match_radius, (~good).sum()))

        # Save indices as labels, not positions, as required by dask
        i1 = id1[good]
        i2 = id2[inds[good]]
        d = pd.Series(dist[good] * 3600, index=id1[good], name='match_distance')

        match_df = pd.DataFrame({'id2':i2, 'distance':d}, index=i1)

        self._match_inds1 = i1
        self._match_inds2 = i2
        self._match_distance = d

        self._matched = True

    @property
    def match_distance(self):
        """Distance between objects identified as matches
        """
        if self._match_distance is None:
            self._match()
        return self._match_distance

    @property
    def match_inds1(self):
        if self._match_inds1 is None:
            self._match()
        return self._match_inds1

    @property
    def match_inds2(self):
        if self._match_inds2 is None:
            self._match()
        return self._match_inds2

    @property
    def match_inds(self):
        return self.match_inds1, self.match_inds2

    def _combine_operation(self, v1, v2):
        return v2 - v1

    def _makeDataFrame(self):
        df1 = self.data1.df.copy()
        df2 = self.data2.df.copy()

        # For any *_magDiff columns, add back the psfMag (x) for a more useful difference
        for df in (df1, df2):
            for c in df.columns:
                m = re.search('(.+_mag)Diff$', c)
                if m:
                    newCol = m.group(1)
                    df[newCol] = df[c] + df['psfMag']

        id1, id2 = self.match_inds

        vdims = self.vdims
        v1 = df1.loc[id1, vdims]
        v2 = df2.loc[id2, vdims]
        v2.index = v1.index

        df = df1.copy()
        df[vdims] = self._combine_operation(v1, v2)
        df['match_distance'] = self.match_distance

        self._df = df

    @property
    def flags(self):
        return self.data1.flags

    def _getDims(self):
        kdims, vdims = self.data1._getDims()

        # Replace the *magDiff vdims with *mag
        magDiffDims = [dim for dim in vdims if re.search('(.+_mag)Diff$', dim)]
        magDims = [dim[:-4] for dim in magDiffDims]

        for d1, d2 in zip(magDiffDims, magDims):
            vdims.remove(d1)
            vdims.append(d2)

        vdims.append('match_distance')
        return kdims, vdims
