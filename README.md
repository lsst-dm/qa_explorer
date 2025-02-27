# qa_explorer

Enabling the creation of interactive QA plots and notebooks for LSST data. 

## Quick start

Set up the shared stack on `lsst-dev`:
```
$ source /ssd/lsstsw/stack3/loadLSST.bash
```
Do a local user-install of the current master of [parambokeh](https://ioam.github.io/parambokeh/) as well as pyarrow (these will hopefully soon become unnecessary):
```
$ pip install git+ssh://git@github.com/ioam/parambokeh.git --user
$ pip install pyarrow --user
```
Clone and setup this package, as well as [pipe_analysis](https://github.com/lsst-dm/pipe_analysis) and [display_ginga](https://github.com/lsst/display_ginga), and enable the jupyter extensions display_ginga requires:
```
$ setup -r /path/to/qa_explorer
$ setup -r /path/to/pipe_analysis
$ setup -r /path/to/display_ginga
$ jupyter nbextension enable --py widgetsnbextension
$ jupyter nbextension enable --py ipyevents
```
As a test to make sure everything is working, you can use the notebook generator script, and try to run the notebooks that get produced:
```
$ generateQANotebook.py /datasets/hsc/repo/rerun/RC/w_2018_06/DM-13435/ --tract 9813 --filt HSC-Z
$ ls QA-notebooks
  coadd_9813_HSC-Z.ipynb  color_9813.ipynb  visitMatch_9813_HSC-Z.ipynb
```
These notebooks are templates containing examples of some of the `qa_explorer` functionality, that can easily be customized for a particular investigation.  (Note that the `visitMatch*` notebook may take ~10 minutes to compute, so try the coadd or color ones first.)
To run the notebooks on `lsst-dev` but access them in a local browser, open a tunnel to your favorite port on `lsst-dev`, and start the jupyter notebook with that port, e.g., 
```
$ ssh -NfL localhost:xxxx:localhost:xxxx lsst-dev01.ncsa.illinois.edu  # on your local machine
```
and
```
$ jupyter notebook --NotebookApp.iopub_data_rate_limit=10000000000 --no-browser --port xxxx  # on lsst-dev
```
Then browse to `localhost:xxxx` on your local machine.

## Overview

For a more verbose overview and discussion of the building blocks of this package, please check out the [overview notebook](notebooks/overview.ipynb).
