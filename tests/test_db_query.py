import unittest

from dbentity.db_entity import DbEntity
from dbentity.db_query import Select, Delete, Count, QueryError
from dbentity.db_control import (
    Where,
    And,
    Or,
    Lt,
    Gt,
    OrderByAsc,
    OrderByDesc,
    Limit,
    Offset,
    GroupBy,
    LeftJoin,
)
from dbentity.attribute import (
    IndexAttribute,
    StringAttribute,
    IntegerAttribute,
    ConnectionAttribute,
    SumIntegerAttribute,
)


class User(DbEntity):
    TABLE = 'users'
    ITEMS = (
        IndexAttribute(),
        StringAttribute('name'),
        IntegerAttribute('age'),
        IntegerAttribute('score'),
    )


class Post(DbEntity):
    TABLE = 'posts'
    ITEMS = (
        IndexAttribute(),
        StringAttribute('title'),
        StringAttribute('content'),
        ConnectionAttribute('author', sub_entity=User),
    )


class NoTableEntity(DbEntity):
    ITEMS = (IndexAttribute(),)


class TestSelectBasic(unittest.TestCase):
    def test_simple_select(self):
        query = Select(User)
        sql = query.query_str
        self.assertIn('SELECT', sql)
        self.assertIn('FROM users', sql)
        self.assertTrue(sql.endswith(';'))

    def test_select_with_where(self):
        query = Select(User, name='John')
        sql = query.query_str
        self.assertIn('WHERE', sql)
        self.assertIn('users.name = %s', sql)
        self.assertEqual(query.args, ['John'])

    def test_select_with_multiple_where(self):
        query = Select(User, name='John', age=30)
        sql = query.query_str
        self.assertIn('WHERE', sql)
        self.assertIn('AND', sql)
        self.assertEqual(len(query.args), 2)

    def test_select_no_table(self):
        with self.assertRaises(QueryError):
            Select(NoTableEntity)


class TestSelectControls(unittest.TestCase):
    def test_select_with_order_by(self):
        query = Select(User, OrderByAsc('name'))
        sql = query.query_str
        self.assertIn('ORDER BY', sql)
        self.assertIn('users.name ASC', sql)

    def test_select_with_multiple_order_by(self):
        query = Select(User, OrderByDesc('age'), OrderByAsc('name'))
        sql = query.query_str
        self.assertIn('ORDER BY', sql)
        self.assertIn('users.age DESC', sql)
        self.assertIn('users.name ASC', sql)

    def test_select_with_limit(self):
        query = Select(User, Limit(10))
        sql = query.query_str
        self.assertIn('LIMIT %s', sql)
        self.assertEqual(query.args, [10])

    def test_select_with_offset(self):
        query = Select(User, Offset(20))
        sql = query.query_str
        self.assertIn('OFFSET %s', sql)
        self.assertEqual(query.args, [20])

    def test_select_with_limit_and_offset(self):
        query = Select(User, Limit(10), Offset(20))
        sql = query.query_str
        self.assertIn('LIMIT %s', sql)
        self.assertIn('OFFSET %s', sql)
        self.assertEqual(query.args, [10, 20])

    def test_select_with_group_by(self):
        query = Select(User, GroupBy('age'))
        sql = query.query_str
        self.assertIn('GROUP BY', sql)
        self.assertIn('users.age', sql)


class TestSelectWhere(unittest.TestCase):
    def test_select_with_and(self):
        query = Select(User, And(name='John', age=30))
        sql = query.query_str
        self.assertIn('WHERE', sql)
        self.assertEqual(len(query.args), 2)

    def test_select_with_or(self):
        # Or with same field needs nested Where
        w1 = Where(name='John')
        w2 = Where(name='Jane')
        query = Select(User, Or(w1, w2))
        sql = query.query_str
        self.assertIn('WHERE', sql)
        self.assertIn('OR', sql)

    def test_select_with_lt_gt(self):
        query = Select(User, Gt(age=18), Lt(age=65))
        sql = query.query_str
        self.assertIn('users.age > %s', sql)
        self.assertIn('users.age < %s', sql)
        self.assertEqual(query.args, [18, 65])


class TestSelectJoin(unittest.TestCase):
    def test_select_with_left_join(self):
        query = Select(Post, LeftJoin('author'))
        sql = query.query_str
        self.assertIn('LEFT JOIN', sql)
        self.assertIn('users', sql)
        self.assertIn('ON', sql)

    def test_select_join_with_where(self):
        query = Select(Post, LeftJoin('author', name='John'))
        sql = query.query_str
        self.assertIn('LEFT JOIN', sql)
        self.assertIn('WHERE', sql)
        self.assertIn('name = %s', sql)
        self.assertEqual(query.args, ['John'])

    def test_select_self_join(self):
        class Employee(DbEntity):
            TABLE = 'employees'
            ITEMS = (
                IndexAttribute(),
                StringAttribute('name'),
                ConnectionAttribute('manager'),
            )
        Employee.ITEMS[2]._sub_entity = Employee

        query = Select(Employee, LeftJoin('manager'))
        sql = query.query_str
        self.assertIn('LEFT JOIN employees AS __manager', sql)
        self.assertIn('ON employees.manager_id = __manager.id', sql)
        self.assertIn('__manager.id', sql)
        self.assertIn('__manager.name', sql)


class TestSelectColumns(unittest.TestCase):
    def test_columns_match_items(self):
        query = Select(User)
        # columns should match non-connection items
        self.assertIn('uid', query._columns)
        self.assertIn('name', query._columns)
        self.assertIn('age', query._columns)

    def test_create_dataobject(self):
        query = Select(User)
        row = (1, 'John', 30, 100)  # uid, name, age, score
        obj = query.create_dataobject(row)
        self.assertIsInstance(obj, User)
        self.assertEqual(obj.uid, 1)
        self.assertEqual(obj.name, 'John')
        self.assertEqual(obj.age, 30)


class TestDelete(unittest.TestCase):
    def test_simple_delete(self):
        query = Delete(User)
        sql = query.query_str
        self.assertIn('DELETE FROM users', sql)
        self.assertNotIn('WHERE', sql)

    def test_delete_with_where(self):
        query = Delete(User, name='John')
        sql = query.query_str
        self.assertIn('DELETE FROM users', sql)
        self.assertIn('WHERE', sql)
        self.assertIn('users.name = %s', sql)
        self.assertEqual(query.args, ['John'])

    def test_delete_with_multiple_conditions(self):
        query = Delete(User, name='John', age=30)
        sql = query.query_str
        self.assertIn('WHERE', sql)
        self.assertIn('AND', sql)
        self.assertEqual(len(query.args), 2)


class TestQueryIntegration(unittest.TestCase):
    def test_complex_query(self):
        query = Select(
            User,
            Gt(age=18),
            Lt(age=65),
            OrderByDesc('score'),
            OrderByAsc('name'),
            Limit(10),
            Offset(0),
            name='John',
        )
        sql = query.query_str
        self.assertIn('SELECT', sql)
        self.assertIn('FROM users', sql)
        self.assertIn('WHERE', sql)
        self.assertIn('users.age > %s', sql)
        self.assertIn('users.age < %s', sql)
        self.assertIn('users.name = %s', sql)
        self.assertIn('ORDER BY', sql)
        self.assertIn('LIMIT %s', sql)
        self.assertIn('OFFSET %s', sql)
        self.assertIn(10, query.args)
        self.assertIn(0, query.args)


class TestCount(unittest.TestCase):
    def test_simple_count(self):
        query = Count(User)
        sql = query.query_str
        self.assertIn('SELECT COUNT(*)', sql)
        self.assertIn('FROM users', sql)
        self.assertTrue(sql.endswith(';'))

    def test_count_with_where(self):
        query = Count(User, name='John')
        sql = query.query_str
        self.assertIn('SELECT COUNT(*)', sql)
        self.assertIn('WHERE', sql)
        self.assertIn('users.name = %s', sql)
        self.assertEqual(query.args, ['John'])

    def test_count_with_multiple_conditions(self):
        query = Count(User, name='John', age=30)
        sql = query.query_str
        self.assertIn('WHERE', sql)
        self.assertIn('AND', sql)
        self.assertEqual(len(query.args), 2)


if __name__ == '__main__':
    unittest.main()
