import pandas as pd
import numpy as np
import fastparquet
import itertools
import re

def perturb_catalog(filename, ra_offset=2, dec_offset=2):
    cat = pd.read_hdf(filename)
    new = cat.copy()
    for i, c in enumerate(cat.columns):
        if c=='coord_ra':
            new.loc[:, c] = cat[c] + np.deg2rad(ra_offset)
        elif c=='coord_dec':
            new.loc[:, c] = cat[c] + np.deg2rad(dec_offset)
        elif re.search('_flux$', c):
            new.loc[:, c] *= (1 + np.random.randn(len(new))*0.01)

    ra_sign = 'm' if ra_offset < 0 else 'p'
    dec_sign = 'm' if dec_offset < 0 else 'p'

    new_filename = filename[:-3] + '_fake_{}{}_{}{}.parq'.format(ra_sign, ra_offset,
                                                                 dec_sign, dec_offset)

    fastparquet.write('data/forced_big_fake_m2_p0.parq', new)


def write_fake_grid(filename, ra_offsets=range(-10,11), dec_offsets=range(-10,11)):
    for dra, ddec in itertools.product(ra_offsets, dec_offsets):
        print('dra={}, ddec={}'.format(dra, ddec))
        perturb_catalog(filename, dra, ddec)

if __name__=='__main__':
    filename = 'data/forced_big.h5'
    write_fake_grid(filename, ra_offsets=range(-1,2), dec_offsets=range(-1,2))
