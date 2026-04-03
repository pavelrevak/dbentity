import unittest

import datetime
import time

from dbentity.attribute import (
    Attribute,
    IndexAttribute,
    CreateIndexAttribute,
    StringAttribute,
    IntegerAttribute,
    FixedPointAttribute,
    BooleanAttribute,
    ConnectionAttribute,
    DatetimeAttribute,
    LastTimeAttribute,
    MinLastTimeAttribute,
    MaxLastTimeAttribute,
    BytesAttribute,
    PasswordAttribute,
    SubElementsAttribute,
    SumIntegerAttribute,
    SumFixedPointAttribute,
    IntegerArrayAttribute,
    NumberOutOfRangeException,
    WrongNumberFormatException,
    last_time_to_string,
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


class TestDatetimeAttribute(unittest.TestCase):
    def test_to_json(self):
        attr = DatetimeAttribute('created')
        dt = datetime.datetime(2024, 6, 15, 10, 30, 45)
        result = attr.to_json(dt)
        self.assertEqual(result, '2024-06-15T10:30:45')

    def test_to_json_none(self):
        attr = DatetimeAttribute('created')
        self.assertIsNone(attr.to_json(None))

    def test_to_template(self):
        attr = DatetimeAttribute('created')
        dt = datetime.datetime(2024, 6, 15, 10, 30, 45)
        result = attr.to_template(dt)
        self.assertEqual(result, '2024-06-15T10:30:45')

    def test_to_template_none(self):
        attr = DatetimeAttribute('created')
        self.assertIsNone(attr.to_template(None))


class TestDatetimeFullAttribute(unittest.TestCase):
    def test_to_json(self):
        from dbentity.attribute import DatetimeFullAttribute
        attr = DatetimeFullAttribute('created')
        dt = datetime.datetime(2024, 6, 15, 10, 30, 45)
        result = attr.to_json(dt)
        self.assertEqual(result['datetime'], '2024-06-15 10:30:45')
        self.assertEqual(result['datetime_short'], '20240615103045')
        self.assertIn('timestamp', result)

    def test_to_template(self):
        from dbentity.attribute import DatetimeFullAttribute
        attr = DatetimeFullAttribute('created')
        dt = datetime.datetime(2024, 6, 15, 10, 30, 45)
        result = attr.to_template(dt)
        self.assertEqual(result['datetime'], '2024-06-15 10:30:45')
        self.assertEqual(result['datetime_short'], '20240615103045')


class TestLastTimeAttribute(unittest.TestCase):
    def test_to_json(self):
        attr = LastTimeAttribute('last_seen')
        past = time.time() - 60
        result = attr.to_json(past)
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result, 60, delta=1)

    def test_to_json_none(self):
        attr = LastTimeAttribute('last_seen')
        self.assertIsNone(attr.to_json(None))

    def test_to_value(self):
        attr = LastTimeAttribute('last_seen')
        past = time.time() - 120
        result = attr.to_value(past)
        self.assertAlmostEqual(result, 120, delta=1)

    def test_to_template(self):
        attr = LastTimeAttribute('last_seen')
        past = time.time() - 3600
        result = attr.to_template(past)
        self.assertIn('timestamp', result)
        self.assertIn('since_sec', result)
        self.assertIn('since_str', result)
        self.assertAlmostEqual(result['since_sec'], 3600, delta=1)

    def test_to_template_none(self):
        attr = LastTimeAttribute('last_seen')
        self.assertIsNone(attr.to_template(None))


class TestLastTimeToString(unittest.TestCase):
    def test_days(self):
        result = last_time_to_string(90061)
        self.assertIn('d', result)
        self.assertIn('h', result)

    def test_hours(self):
        result = last_time_to_string(3661)
        self.assertIn('h', result)
        self.assertIn('m', result)
        self.assertNotIn('d', result)

    def test_minutes(self):
        result = last_time_to_string(125)
        self.assertIn('m', result)
        self.assertIn('s', result)
        self.assertNotIn('h', result)

    def test_seconds_over_10(self):
        result = last_time_to_string(15.5)
        self.assertIn('s', result)
        self.assertNotIn('m', result)

    def test_seconds_over_1(self):
        result = last_time_to_string(5.123)
        self.assertIn('s', result)

    def test_seconds_over_01(self):
        result = last_time_to_string(0.456)
        self.assertIn('s', result)

    def test_milliseconds(self):
        result = last_time_to_string(0.05)
        self.assertIn('ms', result)


class TestMinMaxLastTimeAttribute(unittest.TestCase):
    def test_min_function(self):
        attr = MinLastTimeAttribute('min_time')
        self.assertEqual(attr.FUNCTION, 'MIN')

    def test_max_function(self):
        attr = MaxLastTimeAttribute('max_time')
        self.assertEqual(attr.FUNCTION, 'MAX')


class TestBytesAttribute(unittest.TestCase):
    def test_to_json(self):
        attr = BytesAttribute('data')
        result = attr.to_json(b'\x00\x01\x02')
        self.assertIn('\\x00', result)

    def test_to_json_none(self):
        attr = BytesAttribute('data')
        self.assertIsNone(attr.to_json(None))

    def test_to_template(self):
        attr = BytesAttribute('data')
        result = attr.to_template(b'\xff\xfe')
        self.assertIn('\\xff', result)

    def test_to_template_none(self):
        attr = BytesAttribute('data')
        self.assertIsNone(attr.to_template(None))


class TestPasswordAttribute(unittest.TestCase):
    def test_to_template_always_empty(self):
        attr = PasswordAttribute('password')
        self.assertEqual(attr.to_template('secret123'), '')
        self.assertEqual(attr.to_template(None), '')


class TestSubElementsAttribute(unittest.TestCase):
    def test_flags(self):
        attr = SubElementsAttribute('items')
        self.assertFalse(attr.SAVE)
        self.assertTrue(attr.CONNECTIONS)
        self.assertFalse(attr.save)

    def test_db_key_false(self):
        attr = SubElementsAttribute('items')
        self.assertFalse(attr._db_key)


class TestSumIntegerAttribute(unittest.TestCase):
    def test_function(self):
        attr = SumIntegerAttribute('total')
        self.assertEqual(attr.FUNCTION, 'SUM')

    def test_with_limits(self):
        attr = SumIntegerAttribute('total', minimal=0, maximal=100)
        self.assertEqual(attr._min, 0)
        self.assertEqual(attr._max, 100)


class TestSumFixedPointAttribute(unittest.TestCase):
    def test_function(self):
        attr = SumFixedPointAttribute('total', fp=2)
        self.assertEqual(attr.FUNCTION, 'SUM')

    def test_to_value(self):
        attr = SumFixedPointAttribute('total', fp=2)
        self.assertEqual(attr.to_value(1050), 10.50)
        self.assertIsNone(attr.to_value(None))

    def test_to_template(self):
        attr = SumFixedPointAttribute('total', fp=2)
        self.assertEqual(attr.to_template(2500), 25.00)
        self.assertEqual(attr.to_template(None), '')


class TestIntegerArrayAttribute(unittest.TestCase):
    def test_from_form_list(self):
        attr = IntegerArrayAttribute('ids')
        result = attr.from_form(['1', '2', '3'])
        self.assertEqual(result, [1, 2, 3])

    def test_from_form_single(self):
        attr = IntegerArrayAttribute('ids')
        result = attr.from_form('5')
        self.assertEqual(result, [5])

    def test_from_form_empty(self):
        attr = IntegerArrayAttribute('ids')
        self.assertIsNone(attr.from_form([]))
        self.assertIsNone(attr.from_form(None))

    def test_from_form_deduplicates(self):
        attr = IntegerArrayAttribute('ids')
        result = attr.from_form(['1', '2', '1', '3', '2'])
        self.assertEqual(result, [1, 2, 3])

    def test_from_form_invalid(self):
        attr = IntegerArrayAttribute('ids')
        with self.assertRaises(WrongNumberFormatException):
            attr.from_form(['abc'])

    def test_from_form_with_limits(self):
        attr = IntegerArrayAttribute('ids', minimal=1, maximal=10)
        self.assertEqual(attr.from_form(['5', '7']), [5, 7])
        with self.assertRaises(NumberOutOfRangeException):
            attr.from_form(['0'])
        with self.assertRaises(NumberOutOfRangeException):
            attr.from_form(['11'])


if __name__ == '__main__':
    unittest.main()
