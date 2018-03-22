import numpy as np
import pandas as pd

import lsst.afw.coord as afwCoord
import lsst.afw.geom as afwGeom
import lsst.afw.image as afwImage
import lsst.afw.display

from functools import partial

from .match import match_lists

class hashable_dict(dict):
  def __key(self):
    return tuple((k,self[k]) for k in sorted(self))
  def __hash__(self):
    return hash(self.__key())
  def __eq__(self, other):
    return self.__key() == other.__key()

def find_closest(dmap, ra, dec):
    df = dmap.values()[0].data
    _, ind = match_lists(np.array([float(ra)]), np.array([float(dec)]), df.ra, df.dec, 1.)
    obj = df.iloc[ind]
    if isinstance(obj, pd.DataFrame):
        obj = obj.iloc[0]

    return obj

class QADisplay(lsst.afw.display.Display):
    """Base class for display object enabling image viewing at given coordinate

    The main purpose of this is to be able to connect a `lsst.afw.display.Display` object
    to a `holoviews.Tap` stream that has a source in a holoviews plot window, such 
    that a new image can be loaded due to a mouse click on the plot. 

    Not for direct use; use instead the `CoaddDisplay` or `VisitDisplay` 
    subclasses.

    Parameters
    ----------
    butler : Butler
        Data repo from which images will be retrieved.

    dmap : holoviews.DynamicMap, optional
        If not provided, it will be set to be the source of the `Tap` stream
        passed to `.connect_tap()`.  Having this attribute is necessary for
        operations like "find the exact coordinates of the closest source
        to the place where I clicked."

    Additional keyword arguments passed to `lsst.afw.display.Display`, 
    such as `dims=(500,500)`

    """

    _datasetName = None

    def __init__(self, butler, dmap=None, **kwargs):
        self.butler = butler

        self.dmap = None

        self._expCache = {}

        super(QADisplay, self).__init__(**kwargs)

    @property
    def datasetName(self):
        """Name of the image dataset to be retrieved from the butler
        """
        if self._datasetName is None:
            raise NotImplementedError('Must define _datasetName property')
        return self._datasetName

    def getExp(self, ra, dec, **kwargs):
        """Get the exposure and pixel position corresponding to sky position

        Parameters
        -----------
        ra, dec : float
            Coordinates in degrees

        Returns
        -------
        exp : afw.Exposure
        xy : afwCoord.PixelCoord
            Pixel coordinates in `exp` corresponding to ra, dec

        Additional keyword arguments passed to `_get_dataId`.
        """
        dataId = self._get_dataId(ra, dec, **kwargs)
        exp = self._expFromId(dataId)

        if self.dmap is not None:
            obj = find_closest(self.dmap, ra, dec)
            ra, dec  = obj.ra, obj.dec

        pos = afwCoord.IcrsCoord(ra*afwGeom.degrees, dec*afwGeom.degrees)
        wcs = self._WcsFromId(dataId)
        xy = wcs.skyToPixel(pos)
        print(ra, dec, xy)

        return exp, xy

    def _WcsFromId(self, dataId):
        """Get requested WCS
        """
        exp = self._expFromId(dataId) # This is by default redundant
        return exp.getWcs()

    def _get_dataId(self, *args, **kwargs):
        """Returns dataId and xy coords
        """
        raise NotImplementedError

    def _expFromId(self, dataId):
        """Get requested image data
        """
        dataId = hashable_dict(dataId)
        if dataId in self._expCache:
            exp = self._expCache[dataId]
        else:
            exp = self.butler.get(self.datasetName, dataId)
            self._expCache[dataId] = exp
        return exp

    def update(self, ra, dec, **kwargs):
        """Refresh the display with a new position

        Parameters
        ----------
        ra, dec : float
            Coordinates in degrees

        Additional keyword arguments passed to `getExp`.
        """
        exp, (x, y) = self.getExp(ra, dec, **kwargs)

        self.mtv(exp)
        self.dot('+', x, y, size=50)
        self.pan(x, y)
        self.zoom(1)
        return self

    def connect_tap(self, tap, **kwargs):
        """Connect a tap stream to display

        Parameters
        ----------
        tap : holoviews.Tap
            Tap stream whose source is the sky map, where
        """
        tap.add_subscriber(partial(self.update, **kwargs))
        self.tap_stream = tap
        self.dmap = tap.source

class CoaddDisplay(QADisplay):
    """Display object enabling coadd image viewing at desired location

    Parameters
    ----------
    butler : Butler
        Data repository from which images will be loaded.

    filt : str
        Filter of images to load.
    """
    _datasetName = 'deepCoadd_calexp'

    def __init__(self, butler, filt, **kwargs):
        self.filt = filt
        super(CoaddDisplay, self).__init__(butler, **kwargs)

    def _get_dataId(self, ra, dec, **kwargs):
        skyMap = self.butler.get('deepCoadd_skyMap')
        pos = afwCoord.IcrsCoord(ra*afwGeom.degrees, dec*afwGeom.degrees)
        tractInfo, patchInfo = skyMap.findClosestTractPatchList([pos])[0]
        
        tractId = tractInfo.getId()
        # If two patches returned, then choose one where point is inside inner bbox
        for p in patchInfo:
            wcs = tractInfo.getWcs()
            xy = wcs.skyToPixel(pos)
            if p.getInnerBBox().contains(afwGeom.Point2I(xy)):
                patchIndex = p.getIndex()
                break
        
        dataId = {'tract':tractId, 'patch':'{},{}'.format(*patchIndex), 'filter':self.filt}
        return dataId


class VisitDisplay(QADisplay):
    """Display object enabling single-visit image viewing at desired location

    Parameters
    ----------
    butler : Butler
        Data repository from which images will be loaded.

    filt : str
        Filter of images to load.

    tract : int
        Tract from which images will load
    """

    _datasetName = 'calexp'

    def __init__(self, butler, filt, tract, **kwargs):
        self.filt = filt
        self.tract = tract
        super(VisitDisplay, self).__init__(butler, **kwargs)

    def _get_dataId(self, ra, dec):
        if self.dmap is None:
            raise ValueError('Must connect a visit dmap!')

        visit = int(self.dmap.keys()[0][0]) #Is there a way to do this via key rather than index?
        obj = find_closest(self.dmap, ra, dec)
        ccd = int(obj.ccdId)

        dataId = {'visit' : visit, 'filter' : self.filt, 'ccd' : ccd, 'tract' : self.tract}
        return dataId

    def _WcsFromId(self, dataId):
        wcsHeader = self.butler.get("wcs_md", dataId, immediate=True)
        try:
            wcs = afwImage.makeWcs(wcsHeader)
        except AttributeError:
            wcs = afwGeom.makeSkyWcs(wcsHeader)
        return wcs

