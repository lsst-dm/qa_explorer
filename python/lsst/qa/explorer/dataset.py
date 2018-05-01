import numpy as np
import pandas as pd
import holoviews as hv
from functools import partial
import pickle
import tempfile
import os, shutil
import fastparquet
import dask.dataframe as dd
import logging

from holoviews.operation.datashader import dynspread, datashade

from .functors import Functor, CompositeFunctor, Column, RAColumn, DecColumn, Mag
from .functors import StarGalaxyLabeller
from .catalog import MatchedCatalog, MultiMatchedCatalog, IDMatchedCatalog, MultiBandCatalog
from .plots import filter_dset

class QADataset(object):
    """Container to coordinate and visualize function calculations on catalogs

    The fundamental thing that a `QADataset` does is compute a `pandas.DataFrame`
    containing the results of evaluating the desired functors on the desired
    `Catalog`: the `.df` attribute.  
    In addition to containing a column for the result of each
    `Functor` calculation, `.df` also always contains columns for coordinates
    (`ra`, `dec`), object type label (`label`), x-coordinate of scatter plots 
    (`x`, by default psfMag), an "id" column that will be either 
    `ccdId` or `patchId`, depending on which is relevant,
    and columns for any desired boolean flags.

    In addition to the `.df` attribute, a `QADataset` also puts together a
    `holoviews.Dataset` object that wraps this data, in the `.ds` attribute.
    This is the object that can get passed to various plotting functions 
    from `explorer.plots`.

    A `QADataset` can take different kinds of `Catalog` objects, 
    and the the computed 
    `.df` and `.ds` attributes will look slightly different in each case:
      
    * With a normal single
    `Catalog`, the dataframe will contain the functor computations in columns
    keyed by the keys of the functor dictionary, and `.ds` will just
    be a simple wrapper of this.  
    * With a `MatchedCatalog`,
    the columns will have the same names but will contain the *difference*
    of the computations between the two catalogs (unless a
    functor has `allow_difference=False`).  
    * With a `MultiMatchedCatalog`,
    the columns of `.df` will be multi-level indexed, containing the full
    information of each quantity calculated on each catalog (this is 
    where the memeory footprint can begin to climb).  In this case, `.ds`
    contains the standard deviation of each measurement among all the catalogs;
    also, a `_ds_dict` attribute is created, where a `holoviews.Dataset` 
    object can be accessed for each individual catalog (keyed by visit name or 
    'coadd').  
    * Using a `MultiBandCatalog`, the `.df` attribute contains functor computations
    for each band, in a multi-level column index, and in addition contains 
    color columns for all magnitude functors provided.  In this case, a special
    `.color_ds` attribute

    This object is pickleable, and can be dumped to file using the `.save()`
    method.  Especially when using `MultiMatchedCatalog` catalogs, the 
    dataframe computations can take a long time, so this can be desirable.
    Note that the `client` object cannot get saved though, so client must be
    re-initialized on load, which you can do with the `.load()` classmethod.

    Parameters
    ----------
    catalog : explorer.Catalog 
        Catalog on which to perform calculations.  May be any type of catalog.

    funcs : dict or list
        Dictionary or list of functors.  If list, then the names of the `Functor`
        columns will be `y0, y1, y2, ...`.

    flags : list
        List of flags to load from catalog

    xFunc : explorer.Functor
        Functor to treat as the abscissa for the scatter plots.
        Default is `Mag('base_PsfFlux')`.

    labeller : explorer.functors.Labeller
        Functor to assign labels to sources.  Default is 
        `explorer.functors.StarGalaxyLabeller()`.

    query : str
        [Not implemented fully or tested.  Do not use.]

    client : distributed.Client
        Dask cluster client to be passed to evaluation of functors.

    cachedir : str
        Directory to which to write dataframe if `self.oom` is True.

    oom : bool
        Whether to store computed dataframe out of memory.  Future
        to-be-implemented feature; not really used yet.

    """
    def __init__(self, catalog, funcs, flags=None, 
                 xFunc=Mag('base_PsfFlux', allow_difference=False), 
                 labeller=StarGalaxyLabeller(),
                 query=None, client=None,
                 cachedir=None, oom=False):

        self._set_catalog(catalog)
        self._set_funcs(funcs, xFunc, labeller)
        self._set_flags(flags)

        self.client = client
        self._query = query

        if cachedir is None:
            cachedir = tempfile.gettempdir()
        self._cachedir = cachedir
        self._df_file = None

        self.oom = oom

    def save(self, filename, protocol=4):
        """Write to file

        Parameters
        ----------
        filename : str
            Filename to write pickle file to.  By convention,
            should end with ".pkl"
        """
        pickle.dump(self, open(filename, 'wb'), protocol=protocol)

    @classmethod
    def load(cls, filename, client=None):
        """Restore from a saved file

        Parameters
        ----------
        filename : str
            Filename to load previously saved `QADataset` from.

        client : distributed.Client
            Client object, if desired.  Note that any client previously
            set was not saved, so this must be re-initialized if desired.
        """
        new = pickle.load(open(filename, 'rb'))
        new.client = client
        return new

    def __getstate__(self):
        odict = self.__dict__
        client = self.client
        odict['client'] = None
        return odict

    def __setstate__(self, d):
        self.__dict__ = d

    def __del__(self):
        if self._df_computed and self.oom:
            os.remove(self.df_file)

    def _set_catalog(self, catalog):
        """Change catalog

        Sets catalog to be a new `Catalog`, and resets data structures.

        Parameters
        ----------
        catalog : explorer.Catalog
            New catalog.
        """
        self.catalog = catalog
        self._reset()

    def _set_funcs(self, funcs, xFunc, labeller):
        """Initialize functions

        Parameters
        ----------
        funcs : dict or list
            Dictionary or list of functors.  If list, then will be 
            converted into dictionary keyed by `y0, y1, y2, ...`.


        xFunc : explorer.functors.Functor
            `Functor` to function as the x-coordinate of the scatter
            plots.  

        labeller : explorer.functors.Labeller
            `Functor` to label points.
        """
        if isinstance(funcs, list) or isinstance(funcs, tuple):
            self.funcs = {'y{}'.format(i):f for i,f in enumerate(funcs)}
        elif isinstance(funcs, Functor):
            self.funcs = {'y0':funcs}
        else:
            self.funcs = funcs

        self.xFunc = xFunc
        self.labeller = labeller
        self._reset()

    def _set_flags(self, flags):
        if flags is None:
            self.flags = []
        else:
            self.flags = flags # TODO: check to make sure flags are valid                    
        self._reset()

    def _reset(self):
        """Sets state such that data structurs need to be recomputed

        Necessary after changing catalog, or query, for example.
        """
        self._df_computed = False
        self._ds = None

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, new):
        self._query = new
        self._reset()

    @property
    def allfuncs(self):
        """Dictionary of all functors to be computed from catalog

        In addition to the ones provided upon initialization of the
        `QADataset`, this also contains `ra`, `dec`, `x`, `label`, 
        `ccdId`/`patchId`, and all flags.
        """
        allfuncs = self.funcs.copy()

        # Set coordinates and x value
        allfuncs.update({'ra':RAColumn(), 'dec': DecColumn(), 
                         'x':self.xFunc})
        if self.id_name is not None:
            allfuncs.update({self.id_name : Column(self.id_name, allow_difference=False)})

        # Include flags
        allfuncs.update({f:Column(f, allow_difference=False) for f in self.flags})

        if self.labeller is not None:
            allfuncs.update({'label':self.labeller})

        return allfuncs        

    @property
    def df(self):
        """Dataframe containing results of computation
        """
        if not self._df_computed:
            self._make_df()
        return self._df
        # Save below for when trying to do more out-of-memory
        # df = pd.read_hdf(self.df_file, 'df')
        df = pd.read_parquet(self.df_file) # wait for pandas 0.22
        # df = dd.read_parquet(self.df_file)
        return df

    @property
    def is_matched(self):
        return isinstance(self.catalog, MatchedCatalog)

    @property
    def is_multi_matched(self):
        return isinstance(self.catalog, MultiMatchedCatalog)

    @property 
    def is_idmatched(self):
        return isinstance(self.catalog, IDMatchedCatalog)

    @property
    def is_multiband(self):
        return isinstance(self.catalog, MultiBandCatalog)

    @property
    def id_name(self):
        """patchId or ccdId, as appropriate

        Necessary in order to know which image to load to inspect
        object.
        """
        if self.is_idmatched:
            name = 'patchId'
        elif self.is_multi_matched:
            name = 'ccdId'
        elif self.is_matched and not self.is_multi_matched:
            if 'ccdId' in self.catalog.cat1.columns:
                name = 'ccdId'
            elif 'patchId' in self.catalog.cat1.columns:
                name = 'patchId'
            else:
                logging.warning('No id name available (looked for ccdId, patchId)?')
                name = None
        elif 'ccdId' in self.catalog.columns:
            name = 'ccdId'
        elif 'patchId' in self.catalog.columns:
            name = 'patchId'
        else:
            logging.warning('No id name available (looked for ccdId, patchId)?')
            name = None

        return name

    @property
    def mag_names(self):
        """Names of magnitude functors.

        Used in order to calculate color information if catalog is a `MultiBandCatalog`.
        """
        return [name for name, fn in self.funcs.items() if isinstance(fn, Mag)]

    @property
    def df_file(self):
        """File to store out-of-memory df in 

        [Not really used yet, but placeholder]
        """
        if self._df_file is None:
            self._df_file = os.path.join(self._cachedir, next(tempfile._get_candidate_names()))
        return self._df_file

    def _make_df(self, **kwargs):
        """Compute dataframe.

        This is called if the `.df` attribute is accessed
        but `._df_computed` is False. 
        """
        f = CompositeFunctor(self.allfuncs)
        if self.is_multi_matched:
            kwargs.update(how='all')
        df = f(self.catalog, query=self.query, client=self.client, dropna=False, **kwargs)
        if self.is_matched and not self.is_idmatched:
            df = pd.concat([df, self.catalog.match_distance.dropna(how='all')], axis=1)
        if not self.is_matched: 
            df = df.dropna(how='any')
        df = df.replace([-np.inf, np.inf], np.nan)

        # Add color columns if catalog is a MultiBandCatalog 
        if self.is_multiband:
            cat = self.catalog
            color_dfs = []
            filters = cat.filters
            n_filts = len(filters)
            cols_to_difference = cat.color_groups
            for mag in self.mag_names:
                col_names = [('{}_color'.format(mag), color) for color in cat.colors]
                mags = df[mag]
                color_df = pd.DataFrame({c : mags[c1] - mags[c2] for c, (c1, c2) in zip(col_names, cols_to_difference)})
                color_df.dropna(how='any', inplace=True)
                df = pd.concat([df, color_df], axis=1)

        if self.oom:
            # df.to_hdf(self.df_file, 'df') #must be format='table' if categoricals included
            df.to_parquet(self.df_file) # wait for pandas 0.22
            # fastparquet.write(self.df_file, df) # Doesn't work with multiindexing

        self._df_computed = True

        self._df = df  

    @property
    def ds(self):
        """Holoviews Dataset wrapper of the underlying dataframe
        """
        if self._ds is None:
            self._make_ds()
        return self._ds

    def get_ds(self, key):
        """Get holoviews dataset corresponding to specific catalog

        This is relevant for `MultiMatchedCatalogs`, where multiple 
        `holoviews.Dataset` objects are created and saved in the `_ds_dict`
        attribute.
        """
        if self._ds is None:
            self._make_ds()
        return self._ds_dict[key]

    def get_color_ds(self, key):
        """Get holoviews "color" dataset corresponding to particular magnitude

        This is relevant for `MultiBandCatalog`, where multiple 
        `holoviews.Dataset` objects are created and saved in the `_color_ds_dict`
        attribute, keyed by magnitude name.
        """
        if self._ds is None:
            self._make_ds()
        return self._color_ds_dict[key]

    def _get_kdims(self):
        """Get key dimensions, for generating holoviews Datasets

        Key dimensions are ra, dec, x, label, ccdId/patchId, and all flags
        """
        kdims = ['ra', 'dec', hv.Dimension('x', label=self.xFunc.name), 'label']
        if self.id_name is not None:
            kdims.append(self.id_name)
        kdims += self.flags
        return kdims        

    def _make_ds(self, **kwargs):
        """Create holoviews.Dataset objects needed to generate plots.
        """
        kdims = self._get_kdims()
        vdims = []
        for k,v in self.allfuncs.items():
            if k in ('ra', 'dec', 'x', 'label', self.id_name) or k in self.flags:
                continue
            label = v.name
            if v.allow_difference and not self.is_multiband:
                if self.is_multi_matched:
                    label = 'std({})'.format(label)
                elif self.is_matched:
                    label = 'diff({})'.format(label)
            vdims.append(hv.Dimension(k, label=label))

        if self.is_matched and not self.is_idmatched:
            vdims += [hv.Dimension('match_distance', label='Match Distance [arcsec]')]

        if self.is_multiband:
            self._color_ds_dict = {}
            for mag in self.mag_names:
                self._color_ds_dict[mag] = self.color_ds(mag)
            df = self.df.dropna(how='any')

        elif self.is_multi_matched:

            # reduce df appropriately here
            coadd_cols = ['ra', 'dec', 'x', 'label'] + self.flags
            visit_cols = list(set(self.df.columns.levels[0]) - set(coadd_cols))

            df_swap = self.df.swaplevel(axis=1)
            coadd_df = df_swap.loc[:, 'coadd'][coadd_cols]
            visit_df = self.df[visit_cols].drop('coadd', axis=1, level=1)
            dfs = [coadd_df, visit_df.std(axis=1, level=0).dropna(how='any')]

            # This dropna thing is a problem when there are NaNs in flags.
            #  Solution: use subset=[...] to define the subset of columns to look for NaNs
            subset_to_check = [c for c in df_swap['coadd'].columns if c not in [self.id_name] + self.flags]
            df_dict = {k:df_swap[k].dropna(how='any', subset=subset_to_check).reset_index() 
                            for k in ['coadd'] + self.catalog.visit_names}
            self._ds_dict = {k:hv.Dataset(df_dict[k], kdims=kdims, vdims=vdims) for k in df_dict}

            # Keep only rows that aren't nan in visit values
            df = pd.concat(dfs, axis=1, join='inner')
        else:
            df = self.df.dropna(how='any')

        ds = hv.Dataset(df.reset_index(), kdims=kdims, vdims=vdims)
        self._ds = ds        

    def color_ds(self, mag):
        """Calculate holoviews.Dataset object containing colors for a given magnitude type

        * Functor values and 'x' values come from catalog's reference filter.
        * Flags are computed as the "or" of all bands.
        * Labels are re-determined as follows: 
            * If object is a star in all bands, it is called a "star"
            * If it is a star in zero bands, it is called a "noStar"
            * If it is called a star in some bands but not all, it is called a "maybe"
        """
        if not self.is_multiband:
            return NotImplementedError('Can only get color_ds if catalog is a MultiBandCatalog')
        if not isinstance(self.allfuncs[mag], Mag):
            raise ValueError('Requested column must be a magnitude: {} requested'.format(mag))

        color_df = self.df[['ra', 'dec']]
        color_df.columns = color_df.columns.get_level_values(0)

        filt = self.catalog.reference_filt
        swap_df = self.df.swaplevel(axis=1)

        # Get values for functors and 'x' from reference filter
        func_keys = list(self.funcs.keys()) + ['x'] + [self.id_name]
        color_df = pd.concat([color_df, swap_df[filt][func_keys]], axis=1)

        # Compute flags as the "or" of all 
        flag_cols = [pd.Series(self.df[flag].max(axis=1).astype(bool), name=flag) for flag in self.flags]
        color_df = pd.concat([color_df] + flag_cols, axis=1)

        # Calculate group label
        n = self.catalog.n_filters
        n_star = (self.df['label']=='star').sum(axis=1)
        label = pd.Series(pd.cut(n_star, [-1, 0, n-1 , n], labels=['noStar', 'maybe', 'star']),
                                    index=n_star.index, name='label')
        color_df['label'] = label

        color_df = pd.concat([color_df, self.df['{}_color'.format(mag)]], axis=1)

        # color_df = pd.concat([self.df[['ra', 'dec']], 
        #                       swap_df[filt], 
        #                       self.df['{}_color'.format(mag)]], axis=1)
        # color_df = color_df.rename(columns={('ra', 'ra'):'ra', ('dec', 'dec'): 'dec'})    

        return hv.Dataset(color_df, kdims=self._get_kdims())


    def visit_points(self, vdim, visit, x_max, label,
                     filter_range=None, flags=None, bad_flags=None):
        """Companion function to visit_explore that returns Points object

        Parameters
        ----------
        vdim : str
            Name of dimension whose value gets colormapped.

        visit, x_max, label : int, float, str
            Parameters that become kdims in the holoviews.DynamicMap of 
            `visit_explore`.

        filter_range, flags, bad_flags : dict, list, list
            Parameters controlled by the `filter_stream` parameter
            of `visit_explore`.
        """

        if self.is_multi_matched:
            try:
                dset = self.get_ds(visit)
            except KeyError: 
                dset = self.get_ds(str(visit))
        else:
            if visit != 'coadd':
                raise ValueError('visit name must be "coadd"!')
            dset = self.ds

        dset = dset.select(x=(None, x_max), label=label)
        # filter_range = {} if filter_range is None else filter_range
        # flags = [] if flags is None else flags
        # bad_flags = [] if bad_flags is None else bad_flags
        dset = filter_dset(dset, filter_range=filter_range, flags=flags, bad_flags=bad_flags)
        # dset = dset.redim(**{vdim:'y'})
        vdims = [vdim, 'id', 'x']
        if self.id_name is not None:
            vdims.append(self.id_name)
        pts = hv.Points(dset, kdims=['ra', 'dec'], vdims=vdims)
        return pts.opts(plot={'color_index':vdim})

    def visit_explore(self, vdim, x_range=np.arange(15,24.1,0.5), filter_stream=None,
                      range_override=None):
        """Dynamic map of values of a particular dimension, scrollable through visits

        Parameters
        ----------
        vdim : str
            Name of dimension to explore.

        x_range : array
            Values of faint magnitude limit.  Only points up to this limit will be plotted.
            Beware of scrolling to too faint a limit; it might give you too many points!

        filter_stream : explorer.plots.FilterStream, optional
            Stream of constraints that controls what data to display.  Useful to link
            multiple plots together

        range_override : (min, max), optional
            By default the colormap will be scalled between the 0.005 to 0.995 quantiles
            of the *entire* set of visits.  Sometimes this is not a useful range to view, 
            so this parameter allows a custom colormap range to be set.
        """
        if filter_stream is not None:
            streams = [filter_stream]
        else:
            streams = []
        fn = partial(QADataset.visit_points, self=self, vdim=vdim)
        dmap = hv.DynamicMap(fn, kdims=['visit', 'x_max', 'label'],
                             streams=streams)

        y_min = self.df[vdim].drop('coadd', axis=1).quantile(0.005).min()
        y_max = self.df[vdim].drop('coadd', axis=1).quantile(0.995).max()

        ra_min, ra_max = self.catalog.coadd_cat.ra.quantile([0, 1])
        dec_min, dec_max = self.catalog.coadd_cat.dec.quantile([0, 1])

        ranges = {vdim : (y_min, y_max),
                  'ra' : (ra_min, ra_max),
                  'dec' : (dec_min, dec_max)}
        if range_override is not None:
            ranges.update(range_override)

        # Force visit names to be integers, if possible
        try:
            visit_names = [int(v) for v in self.catalog.visit_names]
            visit_names.sort()
        except:
            visit_names = self.catalog.visit_names

        dmap = dmap.redim.values(visit=visit_names, 
                                 # vdim=list(self.funcs.keys()) + ['match_distance'], 
                                 # vdim=[vdim], 
                                 label=['galaxy', 'star'],
                                 x_max=x_range).redim.range(**ranges)
        return dmap

    def coadd_points(self, vdim, x_max, label, **kwargs):
        """Same as visit_points, but for coadd image.
        """
        return self.visit_points(vdim, 'coadd', x_max, label, **kwargs)

    def coadd_explore(self, vdim, x_range=np.arange(15,24.1,0.5), filter_stream=None,
                        range_override=None):
        """Dynamic map of coadd values

        Parameters
        ----------
        vdim : str
            Name of dimension to explore.

        x_range : array
            Values of faint magnitude limit.  Only points up to this limit will be plotted.
            Beware of scrolling to too faint a limit; it might give you too many points!

        filter_stream : explorer.plots.FilterStream, optional
            Stream of constraints that controls what data to display.  Useful to link
            multiple plots together

        range_override : (min, max), optional
            By default the colormap will be scalled between the 0.005 to 0.995 quantiles
            of the *entire* set of visits.  Sometimes this is not a useful range to view, 
            so this parameter allows a custom colormap range to be set.
        """
        if filter_stream is not None:
            streams = [filter_stream]
        else:
            streams = []
        fn = partial(QADataset.coadd_points, self=self, vdim=vdim)
        dmap = hv.DynamicMap(fn, kdims=['x_max', 'label'],
                             streams=streams)

        if self.is_multi_matched:
            y_min = self.df[(vdim, 'coadd')].quantile(0.005)
            y_max = self.df[(vdim, 'coadd')].quantile(0.995)
            ra_min, ra_max = self.catalog.coadd_cat.ra.quantile([0, 1])
            dec_min, dec_max = self.catalog.coadd_cat.dec.quantile([0, 1])
        else:
            y_min = self.df[vdim].quantile(0.005)
            y_max = self.df[vdim].quantile(0.995)
            ra_min, ra_max = self.catalog.ra.quantile([0, 1])
            dec_min, dec_max = self.catalog.dec.quantile([0, 1])


        ranges = {vdim : (y_min, y_max),
                  'ra' : (ra_min, ra_max),
                  'dec' : (dec_min, dec_max)}
        if range_override is not None:
            ranges.update(range_override)

        # Force visit names to be integers, if possible

        dmap = dmap.redim.values(label=['galaxy', 'star'],
                                 x_max=x_range).redim.range(**ranges)
        return dmap


    def color_points(self, mag=None, xmax=21, label='star', 
                     filter_range=None, flags=None, bad_flags=None,
                    x_range=None, y_range=None):
        """Datashaded layout of color-color plots 

        Parameters
        ----------
        mag : str
            Name of magnitude to get color info from (e.g., name of mag functor).

        xmax : float
            faint magnitude limit.

        label : str
            Desired label of points

        filter_range, flags, bad_flags : dict, list, list
            Parameters controlled by the `filter_stream` parameter
            of `color_explore`.

        x_range, y_range : 
            Arguments required for datashaded map to be dynamic when passed to a DynamicMap        
        """    
        if mag is None:
            mag = self.mag_names[0]
        colors = self.catalog.colors
        pts_list = []
        for c1,c2 in zip(colors[:-1], colors[1:]):
            dset = self.get_color_ds(mag).select(x=(0,xmax), label=label)
            if filter_range is not None:
                dset = filter_dset(dset, filter_range=filter_range, flags=flags, bad_flags=bad_flags)
                
            pts_list.append(dset.to(hv.Points, kdims=[c1, c2], groupby=[]).redim.range(**{c1:(-0.2,1.5),
                                                                                          c2:(-0.2,1.5)}))
        return hv.Layout([dynspread(datashade(pts, dynamic=False, x_range=x_range, y_range=y_range)) for pts in pts_list]).cols(2)


    def color_explore(self, xmax_range=np.arange(18,26.1,0.5), filter_stream=None):
        """Dynamic map of color-color plots

        Parameters
        ----------
        xmax_range : array
            Array of max magnitude values for slider widget

        filter_stream : explorer.plots.FilterStream
            Stream of constraints that controls what data to display.  Useful to link
            multiple plots together

        """

        streams = [hv.streams.RangeXY()]
        if filter_stream is not None:
            streams += [filter_stream]
        dmap = hv.DynamicMap(partial(QADataset.color_points, self=self), kdims=['mag', 'xmax', 'label'], 
                             streams=streams)
        dmap = dmap.redim.values(mag=self.mag_names, xmax=xmax_range, label=['star', 'maybe', 'noStar'])
        return dmap

    def color_points_fit(self, mag=None, colors='GRI', xmax=21, label='star', 
                     filter_range=None, flags=None, bad_flags=None,
                    x_range=None, y_range=None, bounds=None, order=3):
        """Simple points + polynomial fit of selected range

        This is more a simple proof-of-concept than anything particularly
        useful at this point.  

        Parameters
        ----------
        mag : str
            Name of magnitude from which colors are desired.

        colors : str
            Color-color group desired (e.g., 'GRI', 'RIZ', 'IZY').

        xmax : float
            Maximum magnitude to allow

        label : str
            Desired label of points.

        filter_range, flags, bad_flags : dict, list, list
            Parameters controlled by the `filter_stream` parameter
            of `color_fit_explore`.

        x_range, y_range : 
            Arguments required for datashaded map to be dynamic when passed to a DynamicMap        
            (though right now this particular element does not get datashaded)

        bounds : (l,b,r,t)
            Bounds of selection box.

        order : int
            Order of polynomial fit
        """

        if mag is None:
            mag = self.mag_names[0]

        c1 = '{}_{}'.format(*colors[0:2])
        c2 = '{}_{}'.format(*colors[1:3])

        dset = self.get_color_ds(mag).select(x=(0,xmax), label=label)
        if filter_range is not None:
            dset = filter_dset(dset, filter_range=filter_range, flags=flags, bad_flags=bad_flags)

        pts = dset.to(hv.Points, kdims=[c1, c2], groupby=[]).redim.range(**{c1:(-0.2,1.5),
                                                                            c2:(-0.2,1.5)})
        # Fit selected region to polynomial and plot
        if bounds is None:
            fit = hv.Curve([])
        else:
            l,b,r,t = bounds
            subdf = pts.data.query('({0} < {4} < {2}) and ({1} < {5} < {3})'.format(l,b,r,t,c1,c2))
            coeffs = np.polyfit(subdf[c1], subdf[c2], order)
            x_grid = np.linspace(subdf[c1].min(), subdf[c1].max(), 100)
            y_grid = np.polyval(coeffs, x_grid)
    #         print(x_grid, y_grid)
            fit = hv.Curve(np.array([x_grid, y_grid]).T).opts(style={'color':'black', 'width':3})
            print(fit)
        return pts * fit
    #     return dynspread(datashade(pts, dynamic=False, x_range=x_range, y_range=y_range)) * fit

    def color_fit_explore(self, xmax_range=np.arange(18,26.1,0.5), filter_stream=None):
        """Dynamic map exploring polynomial fit to selected range of color-color plot

        Parameters
        ----------
        xmax_range : array
            Array of max magnitude values for slider widget

        filter_stream : explorer.plots.FilterStream
            Stream of constraints that controls what data to display.  Useful to link
            multiple plots together

        """
        streams = [hv.streams.RangeXY(), hv.streams.BoundsXY()]
        if filter_stream is not None:
            streams += [filter_stream]
        dmap = hv.DynamicMap(partial(QADataset.color_points_fit, self=self), kdims=['colors','mag', 'xmax', 'label'], 
                             streams=streams)
        dmap = dmap.redim.values(mag=self.mag_names, xmax=xmax_range, 
                                 label=['star', 'maybe', 'noStar'],
                                colors=self.catalog.color_colors)
        return dmap

