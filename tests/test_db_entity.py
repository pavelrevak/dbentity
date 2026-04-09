import unittest

from dbentity.db_entity import DbEntity, DbEntityError
from dbentity.db_query import QueryError
from dbentity.attribute import (
    IndexAttribute,
    StringAttribute,
    IntegerAttribute,
    ConnectionAttribute,
)
from dbentity.db_control import Gt, Limit, OrderByDesc


class User(DbEntity):
    TABLE = 'users'
    ITEMS = (
        IndexAttribute(),
        StringAttribute('name'),
        IntegerAttribute('age'),
    )


class Post(DbEntity):
    TABLE = 'posts'
    ITEMS = (
        IndexAttribute(),
        StringAttribute('title'),
        ConnectionAttribute('author', sub_entity=User),
    )


class TestDbEntityBasics(unittest.TestCase):
    def test_table_attribute(self):
        self.assertEqual(User.TABLE, 'users')
        self.assertEqual(Post.TABLE, 'posts')

    def test_creation(self):
        user = User(data={'uid': 1, 'name': 'John', 'age': 30})
        self.assertEqual(user.uid, 1)
        self.assertEqual(user.name, 'John')
        self.assertEqual(user.age, 30)

    def test_locked_by_default(self):
        user = User(data={'name': 'John'})
        with self.assertRaises(AttributeError):
            user.custom = 'value'


class TestDbEntitySelectParts(unittest.TestCase):
    def test_select_parts(self):
        parts = User.select_parts()
        self.assertIn('users.id', parts)
        self.assertIn('users.name', parts)
        self.assertIn('users.age', parts)

    def test_select_parts_with_alias(self):
        parts = User.select_parts(alias='u')
        self.assertIn('u.id', parts)
        self.assertIn('u.name', parts)
        self.assertIn('u.age', parts)

    def test_select_parts_excludes_connections(self):
        parts = Post.select_parts()
        # ConnectionAttribute should not be in select_parts
        for part in parts:
            self.assertNotIn('author', part)


class TestDbEntityTableColumns(unittest.TestCase):
    def test_table_columns(self):
        columns = User.table_columns()
        self.assertIn('uid', columns)
        self.assertIn('name', columns)
        self.assertIn('age', columns)

    def test_table_columns_with_parent(self):
        columns = User.table_columns(parent_column='user')
        self.assertIn('user.uid', columns)
        self.assertIn('user.name', columns)
        self.assertIn('user.age', columns)


class TestDbEntityInsert(unittest.TestCase):
    def test_insert_query(self):
        insert_str, insert_args = User._insert(name='John', age=30)
        self.assertIn('INSERT INTO users', insert_str)
        self.assertIn('name', insert_str)
        self.assertIn('age', insert_str)
        self.assertIn('VALUES', insert_str)
        self.assertIn('RETURNING', insert_str)
        self.assertEqual(insert_args, ['John', 30])

    def test_insert_unknown_attribute(self):
        with self.assertRaises(DbEntityError):
            User._insert(unknown='value')


class TestDbEntityUpsert(unittest.TestCase):
    def test_upsert_default_updates_all_non_conflict_cols(self):
        sql, args = User._upsert(
            'name', name='John', age=30)
        self.assertIn('INSERT INTO users', sql)
        self.assertIn('(name, age)', sql)
        self.assertIn('VALUES (%s, %s)', sql)
        self.assertIn('ON CONFLICT (name)', sql)
        self.assertIn('DO UPDATE SET', sql)
        self.assertIn('age = EXCLUDED.age', sql)
        # Conflict target itself must NOT be in the SET clause.
        self.assertNotIn('name = EXCLUDED.name', sql)
        self.assertIn('RETURNING', sql)
        self.assertEqual(args, ['John', 30])

    def test_upsert_multi_column_conflict(self):
        sql, args = User._upsert(
            ('name', 'age'), name='John', age=30)
        self.assertIn('ON CONFLICT (name, age)', sql)
        # Both targets excluded from SET → nothing to update → DO NOTHING.
        self.assertIn('DO NOTHING', sql)
        self.assertEqual(args, ['John', 30])

    def test_upsert_explicit_update_list(self):
        sql, args = User._upsert(
            'name', update=['age'], name='John', age=30)
        self.assertIn('ON CONFLICT (name)', sql)
        self.assertIn('DO UPDATE SET age = EXCLUDED.age', sql)
        self.assertEqual(args, ['John', 30])

    def test_upsert_do_nothing_when_update_empty(self):
        sql, _args = User._upsert(
            'name', update=[], name='John', age=30)
        self.assertIn('DO NOTHING', sql)
        self.assertNotIn('DO UPDATE', sql)

    def test_upsert_unknown_conflict_raises(self):
        with self.assertRaises(DbEntityError):
            User._upsert('nonexistent', name='John')

    def test_upsert_unknown_update_raises(self):
        with self.assertRaises(DbEntityError):
            User._upsert(
                'name', update=['nonexistent'], name='John', age=30)

    def test_upsert_unknown_value_kwarg_raises(self):
        with self.assertRaises(DbEntityError):
            User._upsert('name', name='John', bogus=1)

    def test_db_upsert_returns_entity(self):
        class FakeRow:
            def __init__(self, vals):
                self.vals = vals

            def fetchone(self):
                return self.vals

        class FakeDb:
            def execute(self, sql, args):
                self.last_sql = sql
                self.last_args = args
                # uid, name, age — order matches table_columns()
                return FakeRow((42, 'John', 30))

        db = FakeDb()
        user = User.db_upsert(db, 'name', name='John', age=30)
        self.assertEqual(user.uid, 42)
        self.assertEqual(user.name, 'John')
        self.assertEqual(user.age, 30)
        self.assertIn('ON CONFLICT (name)', db.last_sql)

    def test_db_upsert_returns_none_on_do_nothing_conflict(self):
        class FakeDb:
            def execute(self, sql, args):
                class R:
                    def fetchone(self_inner):
                        return None
                return R()

        result = User.db_upsert(
            FakeDb(), 'name', update=[], name='John', age=30)
        self.assertIsNone(result)

    def test_upsert_from_data_filters_keys(self):
        captured = {}

        class FakeDb:
            def execute(self, sql, args):
                captured['sql'] = sql
                captured['args'] = args

                class R:
                    def fetchone(self_inner):
                        return (1, 'John', 30)
                return R()

        # 'uid' (INDEX) and 'unknown' must be silently ignored.
        User.upsert_from_data(
            FakeDb(),
            {'uid': 999, 'name': 'John', 'age': 30, 'unknown': 'x'},
            conflict='name')
        self.assertIn('ON CONFLICT (name)', captured['sql'])
        self.assertEqual(captured['args'], ['John', 30])

    def test_upsert_from_data_extra_kwargs_override(self):
        captured = {}

        class FakeDb:
            def execute(self, sql, args):
                captured['args'] = args

                class R:
                    def fetchone(self_inner):
                        return (1, 'Jane', 25)
                return R()

        User.upsert_from_data(
            FakeDb(),
            {'name': 'John', 'age': 30},
            conflict='name',
            age=25)
        # kwargs override params dict
        self.assertEqual(captured['args'], ['John', 25])


class TestDbEntityNestedData(unittest.TestCase):
    def test_set_data_with_nested_entity(self):
        data = {
            'uid': 1,
            'title': 'Hello',
            'author.uid': 2,
            'author.name': 'John',
            'author.age': 30,
        }
        post = Post(data=data)
        self.assertEqual(post.uid, 1)
        self.assertEqual(post.title, 'Hello')
        self.assertIsNotNone(post.author)
        self.assertEqual(post.author.uid, 2)
        self.assertEqual(post.author.name, 'John')

    def test_set_data_with_null_nested_entity(self):
        data = {
            'uid': 1,
            'title': 'Hello',
            'author.uid': None,
        }
        post = Post(data=data)
        self.assertEqual(post.uid, 1)
        self.assertIsNone(post.author)


class TestDbEntitySQL(unittest.TestCase):
    def test_db_insert_sql(self):
        user = User(data={'name': 'John', 'age': 30})
        # We can't actually execute, but we can verify the method exists
        self.assertTrue(hasattr(user, 'db_insert'))

    def test_db_update_sql(self):
        user = User(data={'uid': 1, 'name': 'John', 'age': 30})
        self.assertTrue(hasattr(user, 'db_update'))

    def test_db_delete_sql(self):
        user = User(data={'uid': 1})
        self.assertTrue(hasattr(user, 'db_delete'))

    def test_db_save_dispatches_correctly(self):
        # With uid -> should call update
        user_with_uid = User(data={'uid': 1, 'name': 'John'})
        self.assertTrue(hasattr(user_with_uid, 'db_save'))

        # Without uid -> should call insert
        user_without_uid = User(data={'name': 'John'})
        self.assertTrue(hasattr(user_without_uid, 'db_save'))


class TestDbEntityClassMethods(unittest.TestCase):
    def test_db_list_method_exists(self):
        self.assertTrue(hasattr(User, 'db_list'))

    def test_db_get_method_exists(self):
        self.assertTrue(hasattr(User, 'db_get'))

    def test_create_method_exists(self):
        self.assertTrue(hasattr(User, 'create'))

    def test_delete_by_method_exists(self):
        self.assertTrue(hasattr(User, 'delete_by'))

    def test_db_count_method_exists(self):
        self.assertTrue(hasattr(User, 'db_count'))

    def test_db_exists_method_exists(self):
        self.assertTrue(hasattr(User, 'db_exists'))

    def test_db_distinct_method_exists(self):
        self.assertTrue(hasattr(User, 'db_distinct'))


class MockCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class MockDb:
    def __init__(self, rows):
        self._rows = rows
        self.last_query = None
        self.last_args = None

    def execute(self, query, args=None):
        self.last_query = query
        self.last_args = args
        return MockCursor(self._rows)


class TestDbDistinct(unittest.TestCase):
    def test_distinct_single_column(self):
        db = MockDb([('SK',), ('CZ',), ('PL',)])
        result = User.db_distinct(db, 'name')
        self.assertEqual(result, ['SK', 'CZ', 'PL'])
        self.assertIn('SELECT DISTINCT', db.last_query)
        self.assertIn('users.name', db.last_query)
        self.assertIn('ORDER BY', db.last_query)

    def test_distinct_multiple_columns(self):
        db = MockDb([('SK', 30), ('CZ', 25), ('PL', 40)])
        result = User.db_distinct(db, ('name', 'age'))
        self.assertEqual(result, [('SK', 30), ('CZ', 25), ('PL', 40)])
        self.assertIn('SELECT DISTINCT', db.last_query)
        self.assertIn('users.name', db.last_query)
        self.assertIn('users.age', db.last_query)

    def test_distinct_with_where(self):
        db = MockDb([('John',)])
        result = User.db_distinct(db, 'name', age=30)
        self.assertEqual(result, ['John'])
        self.assertIn('WHERE', db.last_query)
        self.assertIn('users.age = %s', db.last_query)
        self.assertEqual(db.last_args, [30])

    def test_distinct_with_order_by(self):
        db = MockDb([('PL',), ('CZ',), ('SK',)])
        result = User.db_distinct(db, 'name', OrderByDesc('name'))
        self.assertEqual(result, ['PL', 'CZ', 'SK'])
        self.assertIn('ORDER BY users.name DESC', db.last_query)

    def test_distinct_with_limit(self):
        db = MockDb([('SK',), ('CZ',)])
        result = User.db_distinct(db, 'name', Limit(2))
        self.assertEqual(result, ['SK', 'CZ'])
        self.assertIn('LIMIT %s', db.last_query)
        self.assertIn(2, db.last_args)

    def test_distinct_with_all_controls(self):
        db = MockDb([(3,), (2,), (1,)])
        result = User.db_distinct(
            db, 'age',
            Gt(age=18),
            OrderByDesc('age'),
            Limit(3),
        )
        self.assertEqual(result, [3, 2, 1])
        self.assertIn('SELECT DISTINCT', db.last_query)
        self.assertIn('WHERE', db.last_query)
        self.assertIn('ORDER BY users.age DESC', db.last_query)
        self.assertIn('LIMIT %s', db.last_query)

    def test_distinct_default_order_without_order_by(self):
        db = MockDb([])
        User.db_distinct(db, 'name')
        self.assertIn('ORDER BY users.name', db.last_query)
        self.assertNotIn('DESC', db.last_query)

    def test_distinct_unknown_column(self):
        db = MockDb([])
        with self.assertRaises(QueryError):
            User.db_distinct(db, 'unknown_column')


class TestDbCountBy(unittest.TestCase):
    def test_count_by_single_column(self):
        db = MockDb([('SK', 150), ('CZ', 80), ('PL', 45)])
        result = User.db_count_by(db, 'name')
        self.assertEqual(result, [('SK', 150), ('CZ', 80), ('PL', 45)])
        self.assertIn('SELECT', db.last_query)
        self.assertIn('users.name', db.last_query)
        self.assertIn('COUNT(*)', db.last_query)
        self.assertIn('GROUP BY', db.last_query)
        self.assertNotIn('ORDER BY', db.last_query)

    def test_count_by_multiple_columns(self):
        db = MockDb([('SK', 30, 50), ('SK', 25, 40), ('CZ', 30, 30)])
        result = User.db_count_by(db, ('name', 'age'))
        self.assertEqual(
            result,
            [(('SK', 30), 50), (('SK', 25), 40), (('CZ', 30), 30)])
        self.assertIn('users.name', db.last_query)
        self.assertIn('users.age', db.last_query)
        self.assertIn('GROUP BY', db.last_query)

    def test_count_by_with_where(self):
        db = MockDb([('SK', 100)])
        result = User.db_count_by(db, 'name', age=30)
        self.assertEqual(result, [('SK', 100)])
        self.assertIn('WHERE', db.last_query)
        self.assertIn('users.age = %s', db.last_query)
        self.assertEqual(db.last_args, [30])

    def test_count_by_with_limit(self):
        from dbentity.db_control import Limit
        db = MockDb([('SK', 150), ('CZ', 80)])
        result = User.db_count_by(db, 'name', Limit(2))
        self.assertEqual(result, [('SK', 150), ('CZ', 80)])
        self.assertIn('LIMIT %s', db.last_query)
        self.assertIn(2, db.last_args)

    def test_count_by_order_asc(self):
        from dbentity.db_control import OrderByAsc
        db = MockDb([('PL', 45), ('CZ', 80), ('SK', 150)])
        result = User.db_count_by(db, 'name', OrderByAsc('_cnt'))
        self.assertEqual(result, [('PL', 45), ('CZ', 80), ('SK', 150)])
        self.assertIn('ORDER BY _cnt ASC', db.last_query)

    def test_count_by_unknown_column(self):
        db = MockDb([])
        with self.assertRaises(QueryError):
            User.db_count_by(db, 'unknown_column')


if __name__ == '__main__':
    unittest.main()
