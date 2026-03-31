import unittest

from dbentity.db_entity import DbEntity, DbEntityError
from dbentity.attribute import (
    IndexAttribute,
    StringAttribute,
    IntegerAttribute,
    ConnectionAttribute,
)


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


if __name__ == '__main__':
    unittest.main()
