import unittest

from dbentity.db_entity import DbEntity
from dbentity.db_control import (
    Where,
    And,
    Or,
    Not,
    Nand,
    Nor,
    Lt,
    Gt,
    Le,
    Ge,
    BitwiseAnd,
    Like,
    ILike,
    IsNull,
    IsNotNull,
    Between,
    OrderBy,
    OrderByAsc,
    OrderByDesc,
    GroupBy,
    Limit,
    Offset,
    BaseJoin,
    LeftJoin,
    RightJoin,
    InnerJoin,
    FullJoin,
    EntityControlError,
)
from dbentity.attribute import (
    IndexAttribute,
    StringAttribute,
    IntegerAttribute,
    ConnectionAttribute,
)


class TestUser(DbEntity):
    TABLE = 'users'
    ITEMS = (
        IndexAttribute(),
        StringAttribute('name'),
        IntegerAttribute('age'),
        IntegerAttribute('status'),
    )


class TestPost(DbEntity):
    TABLE = 'posts'
    ITEMS = (
        IndexAttribute(),
        StringAttribute('title'),
        ConnectionAttribute('author', sub_entity=TestUser),
    )


class TestWhereBasic(unittest.TestCase):
    def test_simple_where(self):
        where = Where(name='John')
        where.process(TestUser, 'users')
        self.assertEqual(where.where_part, 'users.name = %s')
        self.assertEqual(where.args, ['John'])

    def test_where_multiple_kwargs(self):
        where = Where(name='John', age=30)
        where.process(TestUser, 'users')
        self.assertIn('users.name = %s', where.where_part)
        self.assertIn('users.age = %s', where.where_part)
        self.assertIn('John', where.args)
        self.assertIn(30, where.args)

    def test_where_none_value(self):
        where = Where(name=None)
        where.process(TestUser, 'users')
        self.assertEqual(where.where_part, 'users.name IS NULL')
        self.assertEqual(where.args, [])

    def test_where_list_value(self):
        where = Where(age=[25, 30, 35])
        where.process(TestUser, 'users')
        self.assertIn('IN', where.where_part)
        self.assertEqual(len(where.args), 3)

    def test_where_list_with_none(self):
        where = Where(age=[25, None, 35])
        where.process(TestUser, 'users')
        self.assertIn('IN', where.where_part)
        self.assertIn('IS NULL', where.where_part)

    def test_where_unknown_attribute(self):
        where = Where(unknown='value')
        with self.assertRaises(EntityControlError):
            where.process(TestUser, 'users')


class TestAndOr(unittest.TestCase):
    def test_and_combines_with_and(self):
        where = And(name='John', age=30)
        where.process(TestUser, 'users')
        self.assertIn(' AND ', where.where_part)

    def test_or_combines_with_or(self):
        where = Or(name='John', name2='Jane')
        # Using nested where for OR
        w1 = Where(name='John')
        w2 = Where(name='Jane')
        or_where = Or(w1, w2)
        or_where.process(TestUser, 'users')
        self.assertIn(' OR ', or_where.where_part)

    def test_nested_and_or(self):
        w1 = Where(age=25)
        w2 = Where(age=30)
        inner = Or(w1, w2)
        outer = And(inner, name='John')
        outer.process(TestUser, 'users')
        self.assertIn('John', outer.args)
        self.assertIn(25, outer.args)
        self.assertIn(30, outer.args)


class TestNot(unittest.TestCase):
    def test_not_negates(self):
        where = Not(name='John')
        where.process(TestUser, 'users')
        self.assertIn('NOT', where.where_part)

    def test_nand(self):
        where = Nand(name='John', age=30)
        where.process(TestUser, 'users')
        # Nand extends Not, so all parts should be negated
        parts = where.where_part.split(' AND ')
        for part in parts:
            self.assertIn('NOT', part)

    def test_nor(self):
        where = Nor(name='John', age=30)
        where.process(TestUser, 'users')
        # Nor should negate and combine with OR
        self.assertIn('NOT', where.where_part)
        self.assertIn(' OR ', where.where_part)


class TestComparison(unittest.TestCase):
    def test_lt(self):
        where = Lt(age=30)
        where.process(TestUser, 'users')
        self.assertEqual(where.where_part, 'users.age < %s')
        self.assertEqual(where.args, [30])

    def test_lt_with_zero(self):
        where = Lt(age=0)
        where.process(TestUser, 'users')
        self.assertEqual(where.where_part, 'users.age < %s')
        self.assertEqual(where.args, [0])

    def test_gt(self):
        where = Gt(age=18)
        where.process(TestUser, 'users')
        self.assertEqual(where.where_part, 'users.age > %s')
        self.assertEqual(where.args, [18])

    def test_le(self):
        where = Le(age=65)
        where.process(TestUser, 'users')
        self.assertEqual(where.where_part, 'users.age <= %s')
        self.assertEqual(where.args, [65])

    def test_ge(self):
        where = Ge(age=18)
        where.process(TestUser, 'users')
        self.assertEqual(where.where_part, 'users.age >= %s')
        self.assertEqual(where.args, [18])

    def test_bitwise_and(self):
        where = BitwiseAnd(status=4)
        where.process(TestUser, 'users')
        self.assertIn('&', where.where_part)
        self.assertIn('> 0', where.where_part)


class TestLikeOperators(unittest.TestCase):
    def test_like(self):
        where = Like(name='John%')
        where.process(TestUser, 'users')
        self.assertEqual(where.where_part, 'users.name LIKE %s')
        self.assertEqual(where.args, ['John%'])

    def test_ilike(self):
        where = ILike(name='%john%')
        where.process(TestUser, 'users')
        self.assertEqual(where.where_part, 'users.name ILIKE %s')
        self.assertEqual(where.args, ['%john%'])

    def test_like_unknown_column(self):
        where = Like(unknown='test')
        with self.assertRaises(EntityControlError):
            where.process(TestUser, 'users')


class TestNullOperators(unittest.TestCase):
    def test_is_null(self):
        where = IsNull('name')
        where.process(TestUser, 'users')
        self.assertEqual(where.where_part, 'users.name IS NULL')
        self.assertEqual(where.args, [])

    def test_is_null_multiple(self):
        where = IsNull('name', 'age')
        where.process(TestUser, 'users')
        self.assertIn('users.name IS NULL', where.where_part)
        self.assertIn('users.age IS NULL', where.where_part)

    def test_is_not_null(self):
        where = IsNotNull('name')
        where.process(TestUser, 'users')
        self.assertEqual(where.where_part, 'users.name IS NOT NULL')

    def test_is_null_unknown_column(self):
        where = IsNull('unknown')
        with self.assertRaises(EntityControlError):
            where.process(TestUser, 'users')


class TestBetween(unittest.TestCase):
    def test_between(self):
        where = Between('age', 18, 65)
        where.process(TestUser, 'users')
        self.assertEqual(where.where_part, 'users.age BETWEEN %s AND %s')
        self.assertEqual(where.args, [18, 65])

    def test_between_unknown_column(self):
        where = Between('unknown', 1, 10)
        with self.assertRaises(EntityControlError):
            where.process(TestUser, 'users')


class TestOrderBy(unittest.TestCase):
    def test_order_by(self):
        order = OrderBy('name')
        part = order.get_order_part(TestUser)
        self.assertEqual(part, 'users.name')

    def test_order_by_asc(self):
        order = OrderByAsc('name')
        part = order.get_order_part(TestUser)
        self.assertEqual(part, 'users.name ASC')

    def test_order_by_desc(self):
        order = OrderByDesc('age')
        part = order.get_order_part(TestUser)
        self.assertEqual(part, 'users.age DESC')

    def test_order_by_unknown_column(self):
        order = OrderBy('unknown')
        with self.assertRaises(EntityControlError):
            order.get_order_part(TestUser)


class TestGroupBy(unittest.TestCase):
    def test_group_by(self):
        group = GroupBy('status')
        part = group.get_group_by_part(TestUser)
        self.assertEqual(part, 'users.status')

    def test_group_by_unknown_column(self):
        group = GroupBy('unknown')
        with self.assertRaises(EntityControlError):
            group.get_group_by_part(TestUser)


class TestLimitOffset(unittest.TestCase):
    def test_limit(self):
        limit = Limit(10)
        self.assertEqual(limit.get_limit(), 'LIMIT 10')

    def test_offset(self):
        offset = Offset(20)
        self.assertEqual(offset.get_offset(), 'OFFSET 20')


class TestJoin(unittest.TestCase):
    def test_base_join_type(self):
        self.assertEqual(LeftJoin.JOIN_TYPE, 'LEFT')
        self.assertEqual(RightJoin.JOIN_TYPE, 'RIGHT')
        self.assertEqual(InnerJoin.JOIN_TYPE, 'INNER')
        self.assertEqual(FullJoin.JOIN_TYPE, 'FULL')

    def test_find_conn_attr(self):
        join = LeftJoin('author')
        attr = join.find_conn_attr(TestPost)
        self.assertEqual(attr.name, 'author')
        self.assertTrue(attr.CONNECTION)

    def test_find_conn_attr_not_found(self):
        join = LeftJoin('nonexistent')
        with self.assertRaises(EntityControlError):
            join.find_conn_attr(TestPost)

    def test_find_conn_attr_not_connection(self):
        join = LeftJoin('title')
        with self.assertRaises(EntityControlError):
            join.find_conn_attr(TestPost)


if __name__ == '__main__':
    unittest.main()
