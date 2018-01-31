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
    return df.iloc[ind]

class QADisplay(lsst.afw.display.Display):
    _datasetName = None

    def __init__(self, butler, dmap=None, **kwargs):
        self.butler = butler
        self.dmap = dmap

        self._expCache = {}

        super(QADisplay, self).__init__(**kwargs)

    @property
    def datasetName(self):
        if self._datasetName is None:
            raise NotImplementedError('Must define _datasetName property')
        return self._datasetName

    def getExp(self, ra, dec, **kwargs):
        dataId = self._get_dataId(ra, dec, **kwargs)
        exp = self._expFromId(dataId)

        if self.dmap is not None:
            obj = find_closest(self.dmap, ra, dec)
            ra, dec  = obj.ra, obj.dec

        pos = afwCoord.IcrsCoord(ra*afwGeom.degrees, dec*afwGeom.degrees)
        xy = exp.getWcs().skyToPixel(pos)
        
        return exp, xy

    def _get_dataId(self, *args, **kwargs):
        """Returns dataId and xy coords
        """
        raise NotImplementedError

    def _expFromId(self, dataId):
        dataId = hashable_dict(dataId)
        if dataId in self._expCache:
            exp = self._expCache[dataId]
        else:
            exp = self.butler.get(self.datasetName, dataId)
            self._expCache[dataId] = exp
        return exp

    def update(self, ra, dec, **kwargs):
        exp, (x, y) = self.getExp(ra, dec, **kwargs)

        self.mtv(exp)
        self.dot('+', x, y, size=50)
        self.pan(x, y)
        self.zoom(1)
        return self

    def connect_tap(self, tap, **kwargs):
        tap.add_subscriber(partial(self.update, **kwargs))
        self.tap_stream = tap


class CoaddDisplay(QADisplay):
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




