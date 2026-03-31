"""Base entity module for data objects."""

import uuid as _uuid


class EntityError(Exception):
    """General entity error."""


class Entity:
    """Base class for data objects.

    Define attributes via ITEMS tuple. Handles attribute access,
    locking, and serialization (JSON, templates, forms).
    """

    ITEMS = tuple()

    @classmethod
    def get_item(cls, name):
        """Get attribute definition by name."""
        for item in cls.ITEMS:
            if item.is_name(name):
                return item
        return None

    def __init__(self, data=None, lock=True):
        """Initialize entity.

        Args:
            data: Initial data as dict or iterable of (name, value) pairs.
            lock: If True, prevent adding new attributes after init.
        """
        if not self.ITEMS:
            raise EntityError("Entity has not defined ITEMS")
        self._loaded = False
        self._data = {}
        if data:
            self._set_data(data)
        self._updated = set()
        self._uid = None
        self._locked = lock
        if self.get_item('uid') is None:
            self._uid = _uuid.uuid4()

    @property
    def updated(self):
        """Return True if any attribute was modified."""
        return len(self._updated) > 0

    def _set_data(self, data):
        if isinstance(data, dict):
            data = data.items()
        for item_name, val in data:
            if self.get_item(item_name):
                self._data[item_name] = val
        self._loaded = True

    def _lock(self):
        self._locked = True

    def _load(self):
        pass

    def __getattr__(self, key):
        item = self.get_item(key.rstrip('_'))
        if item:
            return self.get(item)
        if key == 'uid':
            return self._uid
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{key}'")

    def __setattr__(self, key, value):
        item = self.get_item(key)
        if item:
            value = item.from_value(value)
            if self._data.get(item.name) != value:
                self._data[item.name] = value
                self._updated.add(key)
        elif hasattr(self, key) or not (
                hasattr(self, '_locked') and self._locked):
            # setting or creating new attributes
            super().__setattr__(key, value)
        else:
            # attempt to set attribute after lock
            raise AttributeError(
                f"'{self.__class__.__name__}' cannot set attribute '{key}'")

    def __eq__(self, other):
        if other is None:
            return False
        return self.uid == other.uid

    def __hash__(self):
        return hash((self.__class__.__name__, self.uid))

    def __repr__(self):
        return f'{self.__class__.__name__}({repr(self._data)})'

    def get(self, item):
        """Get attribute value by Attribute instance."""
        return item.to_value(self._data.get(item.name))

    def get_template_data(self):
        """Return dict with all attributes formatted for templates."""
        data = {}
        for item in self.ITEMS:
            value = self._data.get(item.name)
            if item.CONNECTION:
                if value:
                    value = value.get_template_data()
            elif item.CONNECTIONS:
                if value:
                    value = [val.get_template_data() for val in value]
            else:
                value = item.to_template(value)
            data[item.name] = value
        return data

    def get_json_data(self, recursive=True):
        """Return dict with all attributes formatted for JSON.

        Args:
            recursive: If True, include nested entities.
        """
        data = {}
        for item in self.ITEMS:
            value = self._data.get(item.name)
            if item.CONNECTION:
                if recursive and value is not None:
                    data[item.name] = value.get_json_data()
            elif item.CONNECTIONS:
                if value:
                    value = [val.get_json_data() for val in value]
            else:
                data[item.name] = item.to_json(value)
        return data

    def set_from_form_data(self, params):
        """Update entity from form data dict using form_key mappings."""
        for item in self.ITEMS:
            form_key = item.form_key
            if form_key:
                value = params.get(form_key)
                value = item.from_form(value)
                if self._data.get(item.name) != value:
                    self._data[item.name] = value
                    self._updated.add(item.name)
