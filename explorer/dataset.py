import numpy as np
import pandas as pd
import holoviews as hv

class Dataset(hv.Dataset):
    def __init__(self, catalog, xFunc, yFunc, labeller):
        self.catalog = catalog

        self._xFunc = xFunc
        self._yFunc = yFunc
        self._labeller = labeller

        self._df = None
        self._set_data()

    def reset(self):
        self._df = None

    @property
    def xFunc(self):
        return self._xFunc

    @xFunc.setter
    def xFunc(self, new):
        if new is self._xFunc:
            return
        self._xFunc = new
        self._set_data()

    @property
    def yFunc(self):
        return self._yFunc

    @yFunc.setter
    def yFunc(self, new):
        if new is self._yFunc:
            return
        self._yFunc = new
        self._set_data()

    @property
    def labeller(self):
        return self._labeller

    @labeller.setter
    def labeller(self, new):
        if new is self._labeller:
            return
        self._labeller = new
        self._set_data()

    @property
    def columns(self):
        return self.xFunc.columns + self.yFunc.columns + self.labeller.columns

    def _set_data(self):
        x = self.xFunc(catalog)
