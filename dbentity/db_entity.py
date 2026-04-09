"""Database entity module with CRUD operations."""

import dbentity.db_query as _db_query
import dbentity.entity as _entity


class DbEntityError(_entity.EntityError):
    """Database entity error."""


class DbEntity(_entity.Entity):
    """Entity with database operations.

    Requires TABLE class attribute. Provides db_list(), db_get(),
    create(), db_save(), db_update(), db_delete() methods.
    """

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
        """Save entity to database. Calls db_insert() or db_update()."""
        if self._data.get('uid'):
            self.db_update(db)
        else:
            self.db_insert(db)

    def db_insert(self, db):
        """Insert entity into database."""
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
        """Update modified attributes in database."""
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
        """Delete this entity from database."""
        query_str = f"DELETE FROM {self.TABLE}"
        query_str += ' WHERE id=%s;'
        db.execute(query_str, (self.uid, ))

    @classmethod
    def delete_by(cls, db, *args, **kwargs):
        """Delete matching rows from database."""
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
        """Return list of entities matching criteria."""
        query = _db_query.Select(cls, *args, **kwargs)
        rows = db.execute(query.query_str, query.args).fetchall()
        output = [query.create_dataobject(row) for row in rows]
        return output

    @classmethod
    def db_get(cls, db, *args, **kwargs):
        """Return first entity matching criteria or None."""
        query = _db_query.Select(cls, *args, **kwargs)
        row = db.execute(query.query_str, query.args).fetchone()
        output = None
        if row:
            output = query.create_dataobject(row)
        return output

    @classmethod
    def db_count(cls, db, *args, **kwargs):
        """Return count of matching rows."""
        query = _db_query.Count(cls, *args, **kwargs)
        row = db.execute(query.query_str, query.args).fetchone()
        return row[0] if row else 0

    @classmethod
    def db_exists(cls, db, *args, **kwargs):
        """Return True if any matching row exists."""
        return cls.db_count(db, *args, **kwargs) > 0

    @classmethod
    def db_query(cls, *args, **kwargs):
        """Build SELECT query without executing it.

        Returns Select query object with query_str, args, create_objects().
        """
        return _db_query.Select(cls, *args, **kwargs)

    @classmethod
    def db_distinct(cls, db, columns, *args, **kwargs):
        """Return distinct values for column(s).

        Args:
            columns: column name or tuple/list of column names
            *args: controls (OrderBy, Limit, Offset, Where, etc.)
            **kwargs: WHERE conditions

        Returns:
            List of values (single column) or list of tuples (multiple columns)
        """
        if isinstance(columns, str):
            columns = (columns,)
        query = _db_query.Distinct(cls, columns, *args, **kwargs)
        rows = db.execute(query.query_str, query.args).fetchall()
        if len(columns) == 1:
            return [row[0] for row in rows]
        return rows

    @classmethod
    def db_count_by(cls, db, columns, *args, **kwargs):
        """Return count grouped by column(s).

        Args:
            columns: column name or tuple/list of column names
            *args: controls (Limit, Offset, Where, OrderByAsc/Desc('_cnt'))
            **kwargs: WHERE conditions

        Returns:
            List of tuples: [(value, count), ...] for single column
            or [(values_tuple, count), ...] for multiple columns.
            Use OrderByDesc('_cnt') or OrderByAsc('_cnt') to sort by count.
        """
        if isinstance(columns, str):
            columns = (columns,)
        query = _db_query.CountBy(cls, columns, *args, **kwargs)
        rows = db.execute(query.query_str, query.args).fetchall()
        if len(columns) == 1:
            return [(row[0], row[-1]) for row in rows]
        return [(row[:-1], row[-1]) for row in rows]

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
        """Create new entity in database and return it."""
        insert_str, insert_args = cls._insert(**kwargs)
        row = db.execute(insert_str, insert_args).fetchone()
        if row:
            row = zip(cls.table_columns(), row)
            return cls(row)
        return None

    @classmethod
    def _upsert(cls, conflict, update=None, **kwargs):
        """Build INSERT ... ON CONFLICT ... RETURNING SQL.

        Args:
            conflict: attribute name (str) or tuple/list of attribute
                names whose db_keys form the ON CONFLICT target. The
                target columns must be backed by a UNIQUE constraint
                or unique index in the schema.
            update: optional iterable of attribute names to update on
                conflict. ``None`` (default) updates every column being
                inserted except the conflict target itself. Pass an
                empty list/tuple to get DO NOTHING semantics (in which
                case PG returns no row on conflict).
            **kwargs: column values, same shape as ``create()``.
        """
        if isinstance(conflict, str):
            conflict_names = (conflict,)
        else:
            conflict_names = tuple(conflict)
        conflict_cols = []
        for name in conflict_names:
            item = cls.get_item(name)
            if not item or not item.db_key:
                raise DbEntityError(
                    f"Unknown conflict column '{name}'")
            conflict_cols.append(item.db_key)

        insert_columns = []
        insert_values = []
        insert_args = []
        for key, val in kwargs.items():
            item = cls.get_item(key)
            if not item:
                raise DbEntityError(f"Unknown argument '{key}'")
            if (item.SAVE or item.CREATE) and item.db_key:
                insert_columns.append(item.db_key)
                insert_values.append('%s')
                insert_args.append(item.from_value(val))

        if update is None:
            update_cols = [
                c for c in insert_columns if c not in conflict_cols]
        else:
            update_cols = []
            for name in update:
                item = cls.get_item(name)
                if not item or not item.db_key:
                    raise DbEntityError(
                        f"Unknown update column '{name}'")
                update_cols.append(item.db_key)

        sql = f"INSERT INTO {cls.TABLE}"
        sql += f" ({', '.join(insert_columns)})"
        sql += f" VALUES ({', '.join(insert_values)})"
        sql += f" ON CONFLICT ({', '.join(conflict_cols)})"
        if update_cols:
            sets = ', '.join(
                f"{c} = EXCLUDED.{c}" for c in update_cols)
            sql += f" DO UPDATE SET {sets}"
        else:
            sql += " DO NOTHING"
        sql += f" RETURNING {', '.join(cls.select_parts())};"
        return sql, insert_args

    @classmethod
    def db_upsert(cls, db, conflict, update=None, **kwargs):
        """INSERT or UPDATE on conflict, return the resulting entity.

        Returns the entity row whether it was newly inserted or updated
        (PG ``RETURNING`` fires for both branches of ON CONFLICT DO
        UPDATE). Returns ``None`` only if DO NOTHING was requested
        (``update=[]``) and a conflict occurred — in that case PG
        returns no row.

        Note: PG calls ``nextval()`` on the id sequence for every
        upsert call, even when the UPDATE branch is taken. With
        ``SERIAL`` PKs at high upsert rates this can burn id space;
        prefer ``BIGSERIAL`` or natural keys for hot upsert paths.
        """
        sql, args = cls._upsert(conflict, update=update, **kwargs)
        row = db.execute(sql, args).fetchone()
        if row:
            return cls(zip(cls.table_columns(), row))
        return None

    @classmethod
    def upsert_from_data(cls, db, params, conflict, update=None, **kwargs):
        """Upsert entity from data dict (e.g. JSON).

        Symmetric with ``create_from_data``: only SAVE-able, non-INDEX
        attributes are accepted from ``params`` and converted via
        ``from_value``. ``conflict`` and ``update`` have the same
        semantics as ``db_upsert``.
        """
        data = {}
        for item in cls.ITEMS:
            if not item.SAVE or item.INDEX:
                continue
            if item.name not in params:
                continue
            data[item.name] = item.from_value(params[item.name])
        data.update(kwargs)
        return cls.db_upsert(db, conflict, update=update, **data)

    @classmethod
    def create_from_data(cls, db, params, **kwargs):
        """Create entity from data dict (e.g. JSON).

        Only SAVE-able, non-INDEX attributes are accepted.
        Values are converted via from_value.
        """
        data = {}
        for item in cls.ITEMS:
            if not item.SAVE or item.INDEX:
                continue
            if item.name not in params:
                continue
            data[item.name] = item.from_value(params[item.name])
        data.update(kwargs)
        return cls.create(db, **data)

    @classmethod
    def create_from_form_data(cls, db, params, **kwargs):
        """Create entity from form data using form_key mappings."""
        data = {}
        for item in cls.ITEMS:
            form_key = item.form_key
            if form_key:
                value = params.get(form_key)
                value = item.from_form(value)
                data[item.name] = value
        data.update(kwargs)
        return cls.create(db, **data)
