import unittest

from dbentity.entity import Entity, EntityError
from dbentity.attribute import (
    IndexAttribute,
    StringAttribute,
    IntegerAttribute,
    BooleanAttribute,
)


class TestEntity(Entity):
    ITEMS = (
        IndexAttribute(),
        StringAttribute('name'),
        IntegerAttribute('age'),
        BooleanAttribute('active'),
    )


class EmptyEntity(Entity):
    pass


class TestEntityBasics(unittest.TestCase):
    def test_entity_requires_items(self):
        with self.assertRaises(EntityError):
            EmptyEntity()

    def test_entity_creation(self):
        entity = TestEntity()
        # uid is None until data is loaded (IndexAttribute has CREATE=False)
        self.assertIsNone(entity.uid)
        self.assertIsNone(entity.name)
        self.assertIsNone(entity.age)

    def test_entity_with_data(self):
        entity = TestEntity(data={'name': 'John', 'age': 30})
        self.assertEqual(entity.name, 'John')
        self.assertEqual(entity.age, 30)

    def test_entity_with_tuple_data(self):
        entity = TestEntity(data=[('name', 'Jane'), ('age', 25)])
        self.assertEqual(entity.name, 'Jane')
        self.assertEqual(entity.age, 25)


class TestEntityAttributes(unittest.TestCase):
    def test_get_item(self):
        item = TestEntity.get_item('name')
        self.assertIsNotNone(item)
        self.assertEqual(item.name, 'name')

    def test_get_item_not_found(self):
        item = TestEntity.get_item('nonexistent')
        self.assertIsNone(item)

    def test_getattr(self):
        entity = TestEntity(data={'name': 'Test'})
        self.assertEqual(entity.name, 'Test')

    def test_getattr_not_found(self):
        entity = TestEntity()
        with self.assertRaises(AttributeError):
            _ = entity.nonexistent

    def test_setattr(self):
        entity = TestEntity(data={'name': 'Old'}, lock=False)
        entity.name = 'New'
        self.assertEqual(entity.name, 'New')

    def test_setattr_tracks_updated(self):
        entity = TestEntity(data={'name': 'Old'}, lock=False)
        self.assertFalse(entity.updated)
        entity.name = 'New'
        self.assertTrue(entity.updated)

    def test_setattr_locked(self):
        entity = TestEntity(data={'name': 'Test'})
        with self.assertRaises(AttributeError):
            entity.custom_attr = 'value'


class TestEntityEquality(unittest.TestCase):
    def test_equality_same_uid(self):
        entity1 = TestEntity(data={'uid': 1, 'name': 'A'})
        entity2 = TestEntity(data={'uid': 1, 'name': 'B'})
        self.assertEqual(entity1, entity2)

    def test_equality_different_uid(self):
        entity1 = TestEntity(data={'uid': 1})
        entity2 = TestEntity(data={'uid': 2})
        self.assertNotEqual(entity1, entity2)

    def test_equality_with_none(self):
        entity = TestEntity(data={'uid': 1})
        self.assertNotEqual(entity, None)

    def test_hash(self):
        entity1 = TestEntity(data={'uid': 1})
        entity2 = TestEntity(data={'uid': 1})
        self.assertEqual(hash(entity1), hash(entity2))

        entities = {entity1}
        self.assertIn(entity2, entities)


class TestEntitySerialization(unittest.TestCase):
    def test_get_template_data(self):
        entity = TestEntity(data={'name': 'Test', 'age': 25, 'active': True})
        data = entity.get_template_data()
        self.assertEqual(data['name'], 'Test')
        self.assertEqual(data['age'], 25)
        self.assertTrue(data['active'])

    def test_get_json_data(self):
        entity = TestEntity(data={'name': 'Test', 'age': 25})
        data = entity.get_json_data()
        self.assertEqual(data['name'], 'Test')
        self.assertEqual(data['age'], 25)

    def test_repr(self):
        entity = TestEntity(data={'name': 'Test'})
        repr_str = repr(entity)
        self.assertIn('TestEntity', repr_str)
        self.assertIn('name', repr_str)


class TestEntityFormData(unittest.TestCase):
    def test_set_from_form_data(self):
        class FormEntity(Entity):
            ITEMS = (
                IndexAttribute(),
                StringAttribute('name', form_key='user_name'),
                IntegerAttribute('age', form_key='user_age'),
            )

        entity = FormEntity(data={'name': 'Old', 'age': 20}, lock=False)
        entity.set_from_form_data({'user_name': 'New', 'user_age': '30'})
        self.assertEqual(entity.name, 'New')
        self.assertEqual(entity.age, 30)
        self.assertTrue(entity.updated)


if __name__ == '__main__':
    unittest.main()
