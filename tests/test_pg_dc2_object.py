"""
Tests for Postgres DC2 Object Reader
"""
import os
import pytest
import numpy as np
##from numpy.testing import assert_array_equal
import GCRCatalogs

# pylint: disable=redefined-outer-name
@pytest.fixture(scope='module')
def load_dc2_pg_catalog():
    """Convenience function to provide catalog"""
    this_dir = os.path.dirname(__file__)
    #reader = 'dc2_object_run1.1p_tract4850'
    reader='dc2_object_run2.1_dr1b_pg2'
    
    #config = {'base_dir': os.path.join(this_dir, 'dc2_object_data'),
    #          'filename_pattern': 'test_object_tract_4850.hdf5'}
    #return GCRCatalogs.load_catalog(reader, config)
    return GCRCatalogs.load_catalog(reader)

#  Make tests doing some simple queries.  It's hard to imagine anything
#  realistic which doesn't employ some sort of cut.
#   ideally cuts on tract, patch or location should be intercepted
#   to make use of UDFs

def test_pg():
    gc = load_dc2_pg_catalog()
    print('Catalog loaded')
    # Just fetching one column with no cuts works
    #  objects = gc['objectid']
    #  print(len(objects))

    #   Comment out temporarily while trying out cone
    # clean_objects = gc.get_quantities(['objectid','ra', 'dec'],
    #                                   native_filters=['clean', 'tract=4850',
    #                                                   'patch=4,2'])
    # for k in clean_objects:
    #     print('Got data for ',k)

    # print(len(clean_objects))
    # print(len(clean_objects['ra']))

    ra = 61.2
    de = -29.1
    radius = 20 # in arcseconds

    cone_condition='in_cone(ra={},dec={},radius={})'.format(ra,de,radius)

    in_cone = gc.get_quantities(['objectid', 'ra', 'dec'],
                                native_filters=[cone_condition, 'clean'])
    print('#objects in cone: ',len(in_cone['ra']))

# def test_get_missing_column(load_dc2_catalog):
#     """Verify that a missing column gets correct defaults.

#     Uses just a local minimal HDF5 file and schema.yaml
#     """
#     gc = load_dc2_catalog

#     empty_float_column_should_be_nan = gc['g_base_PsfFlux_flux']
#     empty_int_column_should_be_neg1 = gc['g_parent']
#     empty_bool_column_should_be_False = gc['g_base_SdssShape_flag']

#     assert_array_equal(empty_float_column_should_be_nan,
#                        np.repeat(np.nan, len(gc)))
#     assert_array_equal(empty_int_column_should_be_neg1,
#                        np.repeat(-1, len(gc)))
#     assert_array_equal(empty_bool_column_should_be_False,
#                        np.repeat(False, len(gc)))


# def test_get_tract_patch(load_dc2_catalog):
#     """Verify that we get tract, patch columns correctly.

#     These are not originally stored as columns in the HDF5 files.
#     They are added by the reader.
#     So we want to test here that that actually works.
#     """
#     gc = load_dc2_catalog()
#     tract = 4850
#     patch = '3,1'

#     tract_col = gc['tract']
#     patch_col = gc['patch']

#     assert_array_equal(tract_col, np.repeat(tract, len(gc)))
#     assert_array_equal(patch_col, np.repeat(patch, len(gc)))


