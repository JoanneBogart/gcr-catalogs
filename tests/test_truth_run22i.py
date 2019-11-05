"""
Tests for  DC2 Truth catalog, Run 2.2i
"""
import os
import pytest
import numpy as np
##from numpy.testing import assert_array_equal
import GCRCatalogs

# pylint: disable=redefined-outer-name
@pytest.fixture(scope='module')
def load_truth_catalog():
    """Convenience function to provide catalog"""
    this_dir = os.path.dirname(__file__)

    reader='dc2_truth_run2.2i_hp9430_million_static_galaxies.yaml'
    
    return GCRCatalogs.load_catalog(reader)

#  Make tests doing some simple queries.  It's hard to imagine anything
#  realistic which doesn't employ some sort of cut.

def test_truth():
    gc = load_truth_catalog()
    print('Catalog loaded')
    galaxies = gc['id']
    print('Found {} galaxies'.format(len(galaxies)))

    columns =gc.list_all_native_quantities()

    for c in columns: print(c)
    
    ra = 61.99398718973142
    #de = -29.1
    de = -32.83695608408115
    #radius = 20 # in arcseconds


if __name__== "__main__":
    test_truth()

