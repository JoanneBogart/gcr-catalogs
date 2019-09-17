import os
import warnings
import psycopg2
import numpy as np
import re

from GCR import BaseGenericCatalog
#from .utils import md5, is_string_like

__all__ = ['DC2ObjectCatalog', 'DC2ForcedSourceCatalog']

# Utilities for any Postgres catalog
def _make_connection(connection_parameters):
    """
    Make a connection to a Postgres database

    Parameters
    ----------
    connection_parameters: dict of connection parameters recognized
           by Postgres.  Expect password to be supplied another way,
           e.g. by .pgpass file
    """
    conn = psycopg2.connect(**connection_parameters)
    return conn

def _translate_dtype(pg_type):
    to_translate = {'bigint' : np.int64, 'double precision' : np.float64,
                    'boolean' : np.bool}
    if str(pg_type) in to_translate:
        return to_translate[pg_type]
    else:
        return pg_type
    
_numeric = ' *(?P<num_const>[-+]?\d+(.\d+)?) *'
_tractnum = ' *(?P<tract_num>\d+) *'
_patch = ' *(?P<patch_s>\d,\d) *'
_rdop = ' *(?P<operator><=|>=|<|>) *'
_radec = '(?P<axis>ra|dec)'
_tractop = ' *(?P<operator><=|>=|<|>|=|!=) *'
_patchop = ' *(?P<operator>=|!=) *'
_tract_func = 'tract_from_object_id(objectid)'
_patch_func = 'patch_s_from_object_id(objectid)'

_axis_left_pat = re.compile(_radec + _rdop + _numeric)
_axis_right_pat = re.compile(_numeric + _rdop + _radec)

_tract_left_pat = re.compile(' *tract *' + _tractop + _tractnum)
_tract_right_pat =  re.compile(_tractnum + _tractop + ' *tract *')
_patch_left_pat = re.compile(' *patch *' + _patchop + _patch)
_patch_right_pat =  re.compile(_patch + _patchop + ' *patch *')
##_tract_pat = re.compile(_tract_left + '|' +  _tract_right)
def _optimize_filters(filters):
    """
    Look for conditions which can be optimized by use of UDFs. If found,
    return rewritten version.

    Parameters
    ----------
    Tuple of strings, each expressing a condition which will go in the 
    'where' clause

    Return
    ------
    Reworked list of condition strings.  May be shorter than original
    and ordering may be different.
    
    """
    ra_cond = []
    dec_cond = []
    tp_cond = [] # tract & patch filters should go before random; they're faster
    done = []
    others = []
    for f in filters:
        m = _tract_left_pat.match(f)
        if not m: m = _tract_right_pat.match(f)
        if m:
            cond = f.replace('tract', _tract_func)
            tp_cond.append(cond)
            continue
        m = _patch_left_pat.match(f)
        if not m: m = _patch_right_pat.match(f)
        if m:
            print("original filter string: ", f)
            print("patch_func: ", _patch_func)
            cond1 = f.replace('patch', _patch_func)
            the_patch = m.group('patch_s')
            cond = cond1.replace(the_patch,"'" + the_patch + "'")
            tp_cond.append(cond)
            continue
            
        others.append(f)
    done = tp_cond + others
    return done
    
class DC2ObjectCatalog(BaseGenericCatalog):
    """
    Reader for Postgres tables corresponding to object catalog in DC2

    Parameters
    ----------
    connection_parameters: dict
        connection parameters for Postgres database
    schema_name: Postgres schema containing relevant tables
    dpdd_table: Name of table or view containing dpdd
    dpdd_is_view:   Boolean
    other_tables: List of temaining tables (or all tables) making up 
    "native quantities"
    """
    native_filter_string_only = True

    def _subclass_init(self, **kwargs):
        self._conn = _make_connection(kwargs['connection'])
        self._dbname = kwargs['connection']['dbname']
        self._schema = kwargs['schema_name']
        self._dpdd = kwargs['dpdd_table']
        self._dpdd_is_view = kwargs['dpdd_is_view'] # maybe unnecessary
        self._native_tables = kwargs['other_tables']
        #if not self._dpdd_is_view:
        self._native_tables.append(self._dpdd)
        self._native_dtypes = {}
        self._native_dpdd_dtypes = {}
        self._table_name = self._dpdd

        self.base_filters = ()
        dpdd_query = 'select column_name,data_type from information_schema.columns where '
        dpdd_query += " table_catalog='{}' and table_schema='{}' and ".format(self._dbname, self._schema)
        dpdd_query  += " table_name='dpdd'"
        self._quantity_modifiers = {}
        self._native_dpdd_dtypes = {}
        with self._conn.cursor() as cursor:
            cursor.execute(dpdd_query)
            records = cursor.fetchall()
            for r in records:
                # coord is of user-defined type.   It's not really a
                # dpdd quantity, but is included in the view as a
                # convenience
                if r[0] == 'coord':  continue 
                self._native_dpdd_dtypes[r[0]] = _translate_dtype(r[1])
                #print('Item ', r[0],' has dtype ',r[1])

        # likely self._quantity_modifiers should be a dict where keys
        # are column names in the dpdd and values are all
        #   dpdd.<column_name>

        # _generate_native_quantity_list should return column names
        # from all tables in self._native

        # To get all column names and datatypes in dpdd (whether view
        # or table), make query
        #
        # select column_name, data_type from information_schema.colums where
        #    table_catalog=<connection['dbname']> and
        #    table_schema=<schema_name> and
        #    table_name='dpdd'
        #  With results form dict where keys are names, values are data types.
        native_query_s = 'select column_name,data_type from '
        native_query_s += 'information_schema.columns where '
        native_query_s += "table_catalog='{}' and table_schema='{}'".format(self._dbname, self._schema)
        native_query_s += " and table_name='{}'"
        with self._conn.cursor() as cursor:
            for t in self._native_tables:
                cursor.execute(native_query_s.format(t))
                records = cursor.fetchall()
                for r in records:
                    #self._native_dtypes[t + '.' + r[0]] = r[1]
                    self._native_dtypes[r[0]] = _translate_dtype(r[1])
            

        
    def _generate_native_quantity_list(self):
        return list(self._native_dpdd_dtypes)

    @staticmethod
    def _obtain_native_data_dict(native_quantities_needed, native_quantity_getter):
        """
        Overloading this so that we can query the database backend
        for multiple columns at once
        """
        return native_quantity_getter(native_quantities_needed)
    
    def _iter_native_dataset(self, native_filters=None):
        """
        Required to be implemented by subclass.
        Must be a generator.
        Must yield a callable, *native_quantity_getter*.
        This function must iterate over subsets of rows, not columns!

        *native_filters* will either be `None` or a `GCRQuery` object
        (unless `self.native_filter_string_only` is set to True, in which
        case *native_filters* will either be `None` or a tuple of strings).
        If *native_filters* is a GCRQuery object, you can use
        `native_filters.check_scalar(scalar_dict)` to check if `scalar_dict`
        satisfies `native_filters`.

        Below are specifications of *native_quantity_getter*
        -----------------------------------------
        Must take a single argument of a native quantity name
        (unless `_obtain_native_data_dict` is edited accordingly) DONE HERE
        Should assume the argument is valid.
        Must return a numpy 1d array.
        """

        ###cursor = self._conn.cursor()
        conn = self._conn

        if native_filters is not None:
            all_filters = self.base_filters + tuple(native_filters)
        else:
            all_filters = self.base_filters

        if all_filters:
            opt_filters = _optimize_filters(all_filters)
            query_where_clause = 'WHERE ({})'.format(') AND ('.join(opt_filters))
        else:
            query_where_clause = ''

        print('WHERE clause: \n', query_where_clause)
        
        def dc2_postgres_object_native_quantity_getter(quantities):
            # note the API of this getter is not normal, and hence
            # we have overwritten _obtain_native_data_dict
            dtype = np.dtype([(q, self._native_dpdd_dtypes[q]) for q in quantities])
            query = 'SELECT {} FROM {}.{} {};'.format(
                ', '.join(quantities),
                self._schema,
                self._table_name,
                query_where_clause
            )

            print('Full query: ', query)

            #exit(0)
            # may need to switch to fetchmany for larger dataset
            with conn.cursor() as cursor:
                cursor.execute(query)
                r = cursor.fetchall()
                
            return np.array(r, dtype)

        yield dc2_postgres_object_native_quantity_getter

