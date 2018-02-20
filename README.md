# GCR Catalogs

This repo hosts the mock galaxy catalogs used by [DESCQA2](https://github.com/LSSTDESC/descqa).

On a NERSC machine, all these catalogs can be directly accessed through the "Generic Catalog Reader" (GCR) inferface.
More information about GCR can be found [here](https://github.com/yymao/generic-catalog-reader).

Currently these sets of catalogs are available (**Note that these catalogs are not perfect and will continue to be updated**):

1. protoDC2: 
   by Eve Kovacs, Danila Korytov, Andrew Benson, Katrin Heitmann et al. 
   - `protoDC2` (full catalog)
   
2. Buzzard series: 
   by Joe DeRose, Risa Wechsler, Eli Rykoff et al.
   - `buzzard` (full catalog, DES Y3 area)
   - `buzzard_test` (same as `buzzard` but a small subset for testing purpose)
   - `buzzard_high-res` (higher resolution, smaller sky area)
   - `buzzard_v1.6_1`, `buzzard_v1.6_2`, `buzzard_v1.6_3`, `buzzard_v1.6_5`, `buzzard_v1.6_21` (different realizations of `buzzard`)
      
3. DC1 catalog: 
   - `dc1`

Each of the catalogs is specified by a YAML config file, which can be found [here](https://github.com/LSSTDESC/gcr-catalogs/tree/master/GCRCatalogs/catalog_configs). The galaxy quantities in these catalogs conform to [this schema](https://docs.google.com/document/d/1rUsImkBkjjw82Xa_-3a8VMV6K9aYJ8mXioaRhz0JoqI/edit).


## Use GCRCatalogs under the DESCQA Python environment on NERSC

_Note_: These instructions about Python environment may change in the future. If you encounter issues, please check if there's any updates on these instructions.

`GCRCatalogs` is already installed in the DESCQA Python envoirnment at NERSC. To use it:

### with Jypeter notebooks:

First, [start a NERSC notebook server](https://jupyter-dev.nersc.gov) and open a notebook with Python kernel. Make sure you add the DESCQA Python enviornment to `sys.path`:

For Python 3 (recommended):
```python
import sys
sys.path.insert(0, '/global/common/software/lsst/common/miniconda/py3-4.2.12/lib/python3.6/site-packages')
```

For Python 2:
```python
import sys
sys.path.insert(0, '/global/common/cori/contrib/lsst/apps/anaconda/py2-envs/DESCQA/lib/python2.7/site-packages')
```

### in a terminal:

Activate DESCQA Python environment by running the following on NERSC (needs to be in `bash` or `zsh`):

For Python 3 (recommended):

    source /global/common/software/lsst/cori-haswell-gcc/stack/setup_w_2017_46_py3_gcc6.sh
    source activate DESCQA

For Python 2:

    source /global/common/cori/contrib/lsst/apps/anaconda/4.4.0-py2/bin/activate
    source activate DESCQA


### with a python script: 

To be able to import `GCRCatalogs`, the first line of the script should be:

For Python 3 (recommended):

    #!/global/common/software/lsst/common/miniconda/py3-4.2.12/bin/python

For Python 2:

    #!/global/common/cori/contrib/lsst/apps/anaconda/py2-envs/DESCQA/bin/python 


## Install GCRCatalogs on your own

You can install the latest version by running (but note that you need to change the python paths accordingly) 

    pip install https://github.com/LSSTDESC/gcr-catalogs/archive/master.zip

But note that the actual catalogs can only be accessed on a NERSC machine. 


## Usage and examples

- See [this notebook](https://github.com/LSSTDESC/gcr-catalogs/blob/master/examples/GCRCatalogs%20Demo.ipynb) for a tutorial on how to use GCR Catalogs.

- See [this notebook](https://github.com/LSSTDESC/gcr-catalogs/blob/master/examples/CLF%20Test.ipynb) for an actual application (the Conditional  Luminosity Function test) using GCR Catalogs. (Thanks to Joe DeRose for providing the CLF test example!)

- See [GCR documentation](https://yymao.github.io/generic-catalog-reader/) for the complete GCR API.

- See [this script](https://github.com/LSSTDESC/gcr-catalogs/blob/master/examples/phosim_descqa.py) for an example of interfacing PhoSim through CatSim using GCR Catalogs.


## Contribute to GCRCatalogs:

1. On GitHub [fork](https://guides.github.com/activities/forking/) the GCRCatalogs GitHub repo.

2. On NERSC, clone your fork (you can skip this if you've done it)

       cd /your/own/directory
       git clone git@github.com:YourGitHubUsername/gcr-catalogs.git
       git remote add upstream https://github.com/LSSTDESC/gcr-catalogs.git


3. Sync with the upstream master branch (**always do this!**)

       cd /your/own/directory/gcr-catalogs
       git checkout master
       git pull upstream master
       git push origin master


4. Create a new branch for this edit:

       git checkout -b newBranchName master


5. Make changes

6. Test by adding your clone to the path when running Python: 
   ```python
   import sys
   sys.path.insert(0, '/your/own/directory/gcr-catalogs')
   ```

7. Commit and push to your forked repo

       git add <files changed>
       git commit -m <short but meaningful message>
       git push origin newBranchName


8. Go to your forked repo's GitHub page and "create a pull request". 
