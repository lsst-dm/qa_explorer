# qa_explorer

Enabling the creation of interactive QA plots and notebooks for LSST data. 

## Quick start

Set up the shared stack on `lsst-dev`:
```
$ source /ssd/lsstsw/stack3/loadLSST.bash
```
Do a local user-install of both this repository (it is not yet "stack-ified") and current master of [parambokeh](https://ioam.github.io/parambokeh/) (this second step will soon become unnecessary when the next release happens):
```
$ pip install git+ssh://git@github.com/lsst-dm/qa_explorer.git --user
$ pip install git+ssh://git@github.com/ioam/parambokeh.git --user
```
Set up [pipe_analysis](https://github.com/lsst-dm/pipe_analysis) and [display_ginga](https://github.com/lsst/display_ginga) (clone if you haven't already), and enable the jupyter extensions for display_ginga:
```
$ setup -r /path/to/pipe_analysis
$ setup -r /path/to/display_ginga
$ jupyter nbextension enable --py widgetsnbextension
$ jupyter nbextension enable --py ipyevents
```
As a quick test to make sure everything is working, you can use the notebook generator script, and try to run the notebooks that get produced, e.g.:
```
$ generateQANotebook.py /datasets/hsc/repo/rerun/RC/w_2018_06/DM-13435/ --tract 9813 --filt HSC-Z
$ ls QA-notebooks
  coadd_9813_HSC-Z.ipynb  color_9813_HSC-Z.ipynb  visitMatch_9813_HSC-Z.ipynb
```
These notebooks are templates containing examples of some of the `qa_explorer` functionality, that can easily be customized for a particular investigation.  
