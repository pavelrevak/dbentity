import unittest

from dbentity.db_entity import DbEntity
from dbentity.db_query import Select, Delete, Count
from dbentity.db_control import Gt, Limit, OrderByDesc
from dbentity.attribute import (
    IndexAttribute,
    StringAttribute,
    IntegerAttribute,
)


class User(DbEntity):
    TABLE = 'users'
    ITEMS = (
        IndexAttribute(),
        StringAttribute('name'),
        IntegerAttribute('age'),
    )


class TestPgQueryBytes(unittest.TestCase):
    def test_no_placeholders(self):
        q = Select(User)
        # No %s in query → identical bytes (just .encode())
        self.assertEqual(q.pg_query_bytes, q.query_str.encode())
        self.assertNotIn(b'%s', q.pg_query_bytes)

    def test_single_placeholder(self):
        q = Select(User, name='John')
        self.assertIn(b'$1', q.pg_query_bytes)
        self.assertNotIn(b'%s', q.pg_query_bytes)
        self.assertNotIn(b'$2', q.pg_query_bytes)

    def test_multiple_placeholders_numbered_sequentially(self):
        q = Select(User, Gt(age=18), Limit(10), name='John')
        pg = q.pg_query_bytes
        self.assertNotIn(b'%s', pg)
        # 3 args: name, age, limit → $1, $2, $3 in order
        self.assertIn(b'$1', pg)
        self.assertIn(b'$2', pg)
        self.assertIn(b'$3', pg)
        self.assertNotIn(b'$4', pg)
        # Sequential: $1 must appear before $2 before $3
        self.assertLess(pg.index(b'$1'), pg.index(b'$2'))
        self.assertLess(pg.index(b'$2'), pg.index(b'$3'))

    def test_in_list_expands_to_separate_placeholders(self):
        q = Select(User, age=[20, 25, 30])
        pg = q.pg_query_bytes
        self.assertIn(b'$1', pg)
        self.assertIn(b'$2', pg)
        self.assertIn(b'$3', pg)
        self.assertNotIn(b'%s', pg)
        self.assertEqual(set(q.args), {20, 25, 30})

    def test_returns_bytes(self):
        q = Select(User, name='John')
        self.assertIsInstance(q.pg_query_bytes, bytes)

    def test_cached(self):
        q = Select(User, name='John')
        self.assertIs(q.pg_query_bytes, q.pg_query_bytes)

    def test_args_unaffected(self):
        q = Select(User, Gt(age=18), name='John')
        args_before = list(q.args)
        _ = q.pg_query_bytes
        self.assertEqual(q.args, args_before)

    def test_query_str_unaffected(self):
        q = Select(User, name='John')
        sql_before = q.query_str
        _ = q.pg_query_bytes
        self.assertEqual(q.query_str, sql_before)
        # query_str still contains %s (DB-API style), not $N
        self.assertIn('%s', q.query_str)

    def test_delete_pg_query_bytes(self):
        q = Delete(User, name='John')
        self.assertNotIn(b'%s', q.pg_query_bytes)
        self.assertIn(b'$1', q.pg_query_bytes)

    def test_count_pg_query_bytes(self):
        q = Count(User, age=30)
        self.assertNotIn(b'%s', q.pg_query_bytes)
        self.assertIn(b'$1', q.pg_query_bytes)

    def test_count_no_where(self):
        q = Count(User)
        self.assertEqual(q.pg_query_bytes, q.query_str.encode())

    def test_complex_query_all_placeholders_converted(self):
        q = Select(
            User, Gt(age=18), OrderByDesc('age'), Limit(5),
            name='John')
        pg = q.pg_query_bytes
        # Count placeholders matches args count
        self.assertEqual(pg.count(b'$'), len(q.args))
        self.assertEqual(len(q.args), 3)  # name, age, limit
