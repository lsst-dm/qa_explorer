import argparse

parser = argparse.ArgumentParser()

parser.add_argument('repo', help='data repository')
parser.add_argument('--tract', type=int)
parser.add_argument('--filt', type=str)
parser.add_argument('--output', '-o', default='QA.ipynb')

args = parser.parse_args()

from explorer.notebook import Coadd_QANotebook

nb = Coadd_QANotebook(args.repo, args.tract, args.filt)
nb.write(args.output)
