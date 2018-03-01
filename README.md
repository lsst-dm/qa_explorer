# qa_explorer

Enabling the creation of interactive QA plots and notebooks for LSST data. 

## Quick start

Until this package gets stack-ified, you should be able to clone the repo and run `python setup.py install --user` (after having set up the LSST stack) in order to use it.  All of its dependencies should be available in the shared stack.  As a quick test, you can use the notebook generator script, and try to run the notebooks that get produced, e.g.:
```
$ generateQANotebook.py /datasets/hsc/repo/rerun/RC/w_2018_06/DM-13435/ --tract 9813 --filt HSC-Z
$ ls QA-notebooks
  coadd_9813_HSC-Z.ipynb  color_9813_HSC-Z.ipynb  visitMatch_9813_HSC-Z.ipynb
```
These notebooks are templates containing examples of some of the `qa_explorer` functionality, that can easily be customized for a particular investigation.  
