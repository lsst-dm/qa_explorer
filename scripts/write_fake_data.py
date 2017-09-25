import pandas as pd
import numpy as np
import fastparquet
import itertools
import re
import schwimmbad

def perturb_catalog(filename, ra_offset=2, dec_offset=2, tag='fake', id_offset=0):

    cat = pd.read_hdf(filename)
    new = cat.copy()
    for i, c in enumerate(cat.columns):
        if c=='coord_ra':
            new.loc[:, c] = cat[c] + np.deg2rad(ra_offset)
        elif c=='coord_dec':
            new.loc[:, c] = cat[c] + np.deg2rad(dec_offset)
        elif re.search('_flux$', c):
            new.loc[:, c] *= (1 + np.random.randn(len(new))*0.01)
        elif c=='id':
            new.loc[:, c] = cat[c] + id_offset 

    ra_sign = 'm' if ra_offset < 0 else 'p'
    dec_sign = 'm' if dec_offset < 0 else 'p'

    new_filename = filename[:-3] + '_{}_{}{}_{}{}.parq'.format(tag, ra_sign, abs(ra_offset),
                                                                 dec_sign, abs(dec_offset))

    new = new.set_index('id', drop=True)
    fastparquet.write(new_filename, new)


class write_worker(object):
    def __init__(self, filename, tag='fake'):
        self.filename = filename
        self.tag = tag

    def __call__(self, args):
        i, (dra, ddec) = args
        id_offset = i*10000000000000000
        print('dra={}, ddec={}, id_offset={}'.format(dra, ddec, id_offset))
        return perturb_catalog(self.filename, ra_offset=dra, dec_offset=ddec, id_offset=id_offset, tag=self.tag)

def write_fake_grid(pool, filename, ra_offsets=range(-10,11), dec_offsets=range(-10,11), tag='fake'):
    worker = write_worker(filename, tag=tag)

    results = pool.map(worker, enumerate(itertools.product(ra_offsets, dec_offsets)))

if __name__=='__main__':

    import schwimmbad

    from argparse import ArgumentParser
    parser = ArgumentParser(description="Schwimmbad example.")

    parser.add_argument('filename')
    parser.add_argument('--test', action='store_true')

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--ncores", dest="n_cores", default=1,
                       type=int, help="Number of processes (uses multiprocessing).")
    group.add_argument("--mpi", dest="mpi", default=False,
                       action="store_true", help="Run with MPI.")
    args = parser.parse_args()

    pool = schwimmbad.choose_pool(mpi=args.mpi, processes=args.n_cores)

    if args.test:
        write_fake_grid(pool, args.filename, ra_offsets=range(-1,2), dec_offsets=range(-1,2), tag='test')
    else:
        write_fake_grid(pool, args.filename)

    pool.close()
