"""
Utility functions
"""

import sqlite3
import os, glob, re

try:
    from lsst.pipe.analysis.utils import Filenamer
except ImportError:
    logging.warning('Pipe analysis not available.')


def get_visits_sql(field, tract, filt, sqlitedir='/scratch/hchiang2/parejko/'):
    """Once was useful; now outdated.
    """
    with sqlite3.connect(os.path.join(sqlitedir, 'db{}.sqlite3'.format(field))) as conn:
        cursor = conn.cursor()
        cmd = "select distinct visit from calexp where tract=:tract and filter=:filt"
        cursor.execute(cmd, dict(tract=tract, filt=filt))
        result = cursor.fetchall()
    return [x[0] for x in result]
    # return '^'.join(str(x[0]) for x in result)

def result(df):
    """Returns in-memory dataframe or series, getting result if necessary from dask
    """
    if hasattr(df, 'compute'):
        return df.compute()
    elif hasattr(df, 'result'):
        return df.result()
    else:
        return df

def get_tracts(butler):
    """Returns tracts for which plots are available.

    Decently hack-y pseudo-butler activity here, thanks to `lsst.pipe.analysis`
    Returns list of tracts that have *either* PNGs or parquet files in them.

    Parameters
    ----------
    butler : lsst.daf.persistance.Butler
        Data repository
    """     
    dataId = {'tract':0, 'filter':'HSC-I'}
    filenamer = Filenamer(butler, 'plotCoadd', dataId)

    fake_filename = butler.get(filenamer.dataset + '_filename', dataId, description='foo', style='bar')[0]
    m = re.search('(.+/plots)/.+', fake_filename)
    plot_rootdir = m.group(1)
    filters = os.listdir(plot_rootdir)
    tracts = []
    for f in filters:
        dirs = os.listdir(os.path.join(plot_rootdir, f))
        for d in dirs:
            files = glob.glob('{}/{}/{}/*.png'.format(plot_rootdir,f,d)) 
            files += glob.glob('{}/{}/{}/*.parq'.format(plot_rootdir,f,d)) 
            if len(files) > 0:
                tracts.append(d)
    tracts = list(set([int(t.replace('tract-', '')) for t in tracts]))
    tracts.sort()
    return tracts

def get_visits(butler, tract, filt):
    """Returns visits for which plots exist, for given tract and filt
    """
    dataId = {'tract':tract, 'filter':filt}
    filenamer = Filenamer(butler, 'plotCoadd', dataId)

    fake_filename = butler.get(filenamer.dataset + '_filename', dataId, description='foo', style='bar')[0]
    tract_dir = os.path.dirname(fake_filename)
    visit_dirs = glob.glob(os.path.join(tract_dir, 'visit*'))
    visits = [int(re.search('visit-(\d+)', d).group(1)) for d in visit_dirs if len(os.listdir(d)) > 0]
    visits.sort()
    return visits