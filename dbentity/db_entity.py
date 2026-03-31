import dbentity.db_query as _db_query
import dbentity.entity as _entity


class DbEntityError(_entity.EntityError):
    """General Data object error"""


class DbEntity(_entity.Entity):
    TABLE = ''

    def __init__(self, data=None):
        super().__init__(lock=False)
        if data:
            self._set_data(data)
        self._lock()

    def _set_data(self, data):
        if isinstance(data, dict):
            data = data.items()
        entities = {}
        for item_name, val in data:
            if '.' in item_name:
                entity_name, item_name = item_name.split('.', 1)
                if entity_name in entities:
                    entities[entity_name][item_name] = val
                else:
                    entities[entity_name] = {item_name: val}
            else:
                if self.get_item(item_name):
                    self._data[item_name] = val
        for entity_name, sub_data in entities.items():
            if sub_data is not None and sub_data.get('uid') is not None:
                item = self.get_item(entity_name)
                if item.CONNECTION:
                    sub_entity = item.sub_entity or self.__class__
                    self._data[entity_name] = sub_entity(sub_data)
        self._loaded = True

    def db_save(self, db):
        if self._data.get('uid'):
            self.db_update(db)
        else:
            self.db_insert(db)

    def db_insert(self, db):
        insert_columns = []
        insert_values = []
        insert_args = []
        insert_str = f"INSERT INTO {self.TABLE}"
        for item in self.ITEMS:
            if item.SAVE and item.db_key and item.name in self._data:
                insert_columns.append(f'{item.db_key}')
                insert_values.append('%s')
                insert_args.append(self._data.get(item.name))
        insert_str += f" ({', '.join(insert_columns)})"
        insert_str += f" VALUES ({', '.join(insert_values)})"
        insert_str += ";"
        db.execute(insert_str, insert_args)
        self._updated.clear()

    def db_update(self, db):
        """Update stored data into database"""
        if not self._updated:
            return
        query_str = f"UPDATE {self.TABLE} SET "
        set_parts = []
        set_values = []
        for item in self.ITEMS:
            if item.SAVE and item.db_key and item.name in self._updated:
                set_parts.append(f'{item.db_key}=%s')
                set_values.append(self._data.get(item.name))
        query_str += ', '.join(set_parts)
        query_str += ' WHERE id=%s;'
        set_values.append(self.uid)
        db.execute(query_str, set_values)
        self._updated.clear()

    def db_delete(self, db):
        """delete this item from DB"""
        query_str = f"DELETE FROM {self.TABLE}"
        query_str += ' WHERE id=%s;'
        db.execute(query_str, (self.uid, ))

    @classmethod
    def delete_by(cls, db, *args, **kwargs):
        """delete from database"""
        query = _db_query.Delete(cls, *args, **kwargs)
        query_str = query.query_str
        db.execute(query_str, query.args)

    @classmethod
    def select_parts(cls, alias=None):
        if alias is None:
            alias = cls.TABLE
        select_parts = []
        for item in cls.ITEMS:
            if item.CONNECTION:
                continue
            if item.db_key:
                key = f'{alias}.{item.db_key}'
                if item.FUNCTION:
                    key = f'{item.FUNCTION}({key})'
                select_parts.append(key)
        return select_parts

    @classmethod
    def table_columns(cls, parent_column=None):
        columns = []
        for item in cls.ITEMS:
            if item.CONNECTION:
                continue
            if item.db_key:
                if parent_column:
                    columns.append(f'{parent_column}.{item.name}')
                else:
                    columns.append(item.name)
        return columns

    @classmethod
    def db_list(cls, db, *args, **kwargs):
        query = _db_query.Select(cls, *args, **kwargs)
        rows = db.execute(query.query_str, query.args).fetchall()
        output = [query.create_dataobject(row) for row in rows]
        return output

    @classmethod
    def db_get(cls, db, *args, **kwargs):
        query = _db_query.Select(cls, *args, **kwargs)
        row = db.execute(query.query_str, query.args).fetchone()
        output = None
        if row:
            output = query.create_dataobject(row)
        return output

    @classmethod
    def _insert(cls, **kwargs):
        insert_columns = []
        insert_values = []
        insert_args = []
        insert_str = f"INSERT INTO {cls.TABLE}"
        for key, val in kwargs.items():
            item = cls.get_item(key)
            if not item:
                raise DbEntityError(f"Unknown argument '{key}'")
            if (item.SAVE or item.CREATE) and item.db_key:
                insert_columns.append(f'{item.db_key}')
                insert_values.append('%s')
                insert_args.append(item.from_value(val))
        insert_str += f" ({', '.join(insert_columns)})"
        insert_str += f" VALUES ({', '.join(insert_values)})"
        insert_str += f" RETURNING {', '.join(cls.select_parts())}"
        insert_str += ";"
        return insert_str, insert_args

    @classmethod
    def create(cls, db, **kwargs):
        insert_str, insert_args = cls._insert(**kwargs)
        row = db.execute(insert_str, insert_args).fetchone()
        if row:
            row = zip(cls.table_columns(), row)
            return cls(row)
        return None

    @classmethod
    def create_from_form_data(cls, db, params, **kwargs):
        data = {}
        for item in cls.ITEMS:
            form_key = item.form_key
            if form_key:
                value = params.get(form_key)
                value = item.from_form(value)
                data[item.name] = value
        data.update(kwargs)
        return cls.create(db, **data)
