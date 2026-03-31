import unittest

from dbentity.attribute import (
    Attribute,
    IndexAttribute,
    CreateIndexAttribute,
    StringAttribute,
    IntegerAttribute,
    FixedPointAttribute,
    BooleanAttribute,
    ConnectionAttribute,
    NumberOutOfRangeException,
    WrongNumberFormatException,
)


class TestAttribute(unittest.TestCase):
    def test_basic_attribute(self):
        attr = Attribute('name')
        self.assertEqual(attr.name, 'name')
        self.assertEqual(attr.db_key, 'name')
        self.assertIsNone(attr.form_key)
        self.assertIsNone(attr.default)

    def test_attribute_with_db_key(self):
        attr = Attribute('name', db_key='user_name')
        self.assertEqual(attr.name, 'name')
        self.assertEqual(attr.db_key, 'user_name')

    def test_attribute_with_form_key(self):
        attr = Attribute('name', form_key='form_name')
        self.assertEqual(attr.form_key, 'form_name')

    def test_is_name(self):
        attr = Attribute('name')
        self.assertTrue(attr.is_name('name'))
        self.assertFalse(attr.is_name('other'))


class TestIndexAttribute(unittest.TestCase):
    def test_default_index_attribute(self):
        attr = IndexAttribute()
        self.assertEqual(attr.name, 'uid')
        self.assertEqual(attr.db_key, 'id')
        self.assertTrue(attr.INDEX)
        self.assertFalse(attr.CREATE)
        self.assertFalse(attr.SAVE)

    def test_custom_index_attribute(self):
        attr = IndexAttribute(name='custom_id', db_key='custom_pk')
        self.assertEqual(attr.name, 'custom_id')
        self.assertEqual(attr.db_key, 'custom_pk')

    def test_create_index_attribute(self):
        attr = CreateIndexAttribute()
        self.assertTrue(attr.INDEX)
        self.assertTrue(attr.CREATE)
        self.assertFalse(attr.SAVE)


class TestIntegerAttribute(unittest.TestCase):
    def test_from_form_valid(self):
        attr = IntegerAttribute('age')
        self.assertEqual(attr.from_form('25'), 25)
        self.assertEqual(attr.from_form('-10'), -10)
        self.assertEqual(attr.from_form('+5'), 5)

    def test_from_form_empty(self):
        attr = IntegerAttribute('age')
        self.assertIsNone(attr.from_form(''))
        self.assertIsNone(attr.from_form(None))

    def test_from_form_invalid(self):
        attr = IntegerAttribute('age')
        with self.assertRaises(WrongNumberFormatException):
            attr.from_form('abc')
        with self.assertRaises(WrongNumberFormatException):
            attr.from_form('12.5')

    def test_from_form_with_min(self):
        attr = IntegerAttribute('age', minimal=0)
        self.assertEqual(attr.from_form('0'), 0)
        with self.assertRaises(NumberOutOfRangeException):
            attr.from_form('-1')

    def test_from_form_with_max(self):
        attr = IntegerAttribute('age', maximal=100)
        self.assertEqual(attr.from_form('100'), 100)
        with self.assertRaises(NumberOutOfRangeException):
            attr.from_form('101')

    def test_from_form_with_min_max(self):
        attr = IntegerAttribute('age', minimal=0, maximal=150)
        self.assertEqual(attr.from_form('75'), 75)
        with self.assertRaises(NumberOutOfRangeException):
            attr.from_form('-1')
        with self.assertRaises(NumberOutOfRangeException):
            attr.from_form('151')


class TestFixedPointAttribute(unittest.TestCase):
    def test_from_form_fp2(self):
        attr = FixedPointAttribute('price', fp=2)
        self.assertEqual(attr.from_form('10.50'), 1050)
        self.assertEqual(attr.from_form('10,50'), 1050)

    def test_to_value_fp2(self):
        attr = FixedPointAttribute('price', fp=2)
        self.assertEqual(attr.to_value(1050), 10.50)
        self.assertIsNone(attr.to_value(None))

    def test_from_value_fp2(self):
        attr = FixedPointAttribute('price', fp=2)
        self.assertEqual(attr.from_value(10.50), 1050)
        self.assertIsNone(attr.from_value(None))

    def test_to_template_fp2(self):
        attr = FixedPointAttribute('price', fp=2)
        self.assertEqual(attr.to_template(1050), 10.50)
        self.assertEqual(attr.to_template(None), '')

    def test_from_form_invalid(self):
        attr = FixedPointAttribute('price', fp=2)
        with self.assertRaises(WrongNumberFormatException):
            attr.from_form('abc')

    def test_from_form_with_limits(self):
        attr = FixedPointAttribute('price', fp=2, minimal=0, maximal=100)
        self.assertEqual(attr.from_form('50.00'), 5000)
        with self.assertRaises(NumberOutOfRangeException):
            attr.from_form('-1')
        with self.assertRaises(NumberOutOfRangeException):
            attr.from_form('100.01')


class TestBooleanAttribute(unittest.TestCase):
    def test_from_form(self):
        attr = BooleanAttribute('active')
        self.assertTrue(attr.from_form(True))
        self.assertTrue(attr.from_form(1))
        self.assertTrue(attr.from_form('yes'))
        self.assertFalse(attr.from_form(None))
        self.assertFalse(attr.from_form(''))
        self.assertFalse(attr.from_form(0))

    def test_from_form_with_default(self):
        attr = BooleanAttribute('active', default=True)
        self.assertTrue(attr.from_form(None))


class TestConnectionAttribute(unittest.TestCase):
    def test_default_db_key(self):
        attr = ConnectionAttribute('user')
        self.assertEqual(attr.db_key, 'user_id')
        self.assertEqual(attr.conn_key, 'id')

    def test_custom_db_key(self):
        attr = ConnectionAttribute('user', db_key='owner_id')
        self.assertEqual(attr.db_key, 'owner_id')

    def test_custom_conn_key(self):
        attr = ConnectionAttribute('user', conn_key='uid')
        self.assertEqual(attr.conn_key, 'uid')

    def test_connection_flags(self):
        attr = ConnectionAttribute('user')
        self.assertTrue(attr.CONNECTION)
        self.assertFalse(attr.SAVE)


if __name__ == '__main__':
    unittest.main()
