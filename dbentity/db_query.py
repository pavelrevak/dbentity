"""SQL query builders for SELECT, DELETE, COUNT operations."""

import dbentity.db_control as _db_control


class QueryError(Exception):
    """Query building error."""


class BaseQuery:
    """Base class for query builders."""
    def __init__(self, entity, *args, **kwargs):
        self._entity = entity
        self._where = _db_control.And(*args, **kwargs)
        self._prepare(*args, **kwargs)

    @property
    def entity(self):
        return self._entity

    @property
    def where(self):
        return self._where

    def _prepare(self, *args, **kwargs):
        if not self._entity.TABLE:
            raise QueryError("'TABLE' is not defined")
        if not self._entity.ITEMS:
            raise QueryError("'ITEMS' is not defined")
        self._where.process(self._entity, self._entity.TABLE)

    @property
    def args(self):
        return self._where.args


class Select(BaseQuery):
    """SELECT query builder with support for JOIN, ORDER BY, LIMIT, etc."""
    def __init__(self, entity, *args, **kwargs):
        self._columns = []
        self._select_parts = []
        self._join_parts = []
        self._order_parts = []
        self._group_by_parts = []
        self._limit_part = None
        self._limit_arg = None
        self._offset_part = None
        self._offset_arg = None
        super().__init__(entity, *args, **kwargs)

    def extend_columns(self, parts):
        self._columns.extend(parts)

    def add_column(self, part):
        self._columns.append(part)

    def extend_select_parts(self, parts):
        self._select_parts.extend(parts)

    def add_select_part(self, part):
        self._select_parts.append(part)

    def extend_join_parts(self, parts):
        self._join_parts.extend(parts)

    def add_join_part(self, part):
        self._join_parts.append(part)

    def extend_order_parts(self, parts):
        self._order_parts.extend(parts)

    def add_order_part(self, part):
        self._order_parts.append(part)

    def extend_group_by_parts(self, parts):
        self._group_by_parts.extend(parts)

    def add_group_by_part(self, part):
        self._group_by_parts.append(part)

    def _prepare(self, *args, **kwargs):
        super()._prepare(*args, **kwargs)
        self.extend_select_parts(self._entity.select_parts())
        self.extend_columns(self._entity.table_columns())
        for control in args:
            if isinstance(control, _db_control.BaseJoin):
                control.process(self)
            elif isinstance(control, _db_control.OrderBy):
                self.add_order_part(control.get_order_part(self._entity))
            elif isinstance(control, _db_control.Limit):
                self._limit_part, self._limit_arg = control.get_limit()
            elif isinstance(control, _db_control.Offset):
                self._offset_part, self._offset_arg = control.get_offset()
            elif isinstance(control, _db_control.GroupBy):
                self.add_group_by_part(control.get_group_by_part(self._entity))
            elif isinstance(control, _db_control.Where):
                pass
            else:
                raise QueryError(f"Unknown argument '{control}'")

    @property
    def query_str(self):
        select_parts = self._select_parts
        if self._group_by_parts:
            select_parts = self._group_by_parts
        query = [f"SELECT {', '.join(select_parts)}"]
        query.append(f"FROM {self._entity.TABLE}")
        if self._join_parts:
            query.append(' '.join(self._join_parts))
        if self._where.count_parts:
            query.append(f"WHERE {self._where.where_part}")
        if self._group_by_parts:
            query.append(f"GROUP BY {', '.join(self._group_by_parts)}")
        if self._order_parts:
            query.append(f"ORDER BY {', '.join(self._order_parts)}")
        if self._limit_part:
            query.append(self._limit_part)
        if self._offset_part:
            query.append(self._offset_part)
        query_str = ' '.join(query)
        query_str += ";"
        return query_str

    @property
    def args(self):
        args = self._where.args
        if self._limit_arg is not None:
            args = [*args, self._limit_arg]
        if self._offset_arg is not None:
            args = [*args, self._offset_arg]
        return args

    def create_dataobject(self, row):
        if self._group_by_parts:
            data = zip(self._group_by_parts, row)
        else:
            data = zip(self._columns, row)
        return self._entity(data=data)


class Delete(BaseQuery):
    """DELETE query builder."""

    @property
    def query_str(self):
        query = [f"DELETE FROM {self._entity.TABLE}"]
        if self._where.count_parts:
            query.append(f"WHERE {self._where.where_part}")
        query_str = ' '.join(query)
        query_str += ";"
        return query_str


class Count(BaseQuery):
    """SELECT COUNT(*) query builder."""

    @property
    def query_str(self):
        query = [f"SELECT COUNT(*) FROM {self._entity.TABLE}"]
        if self._where.count_parts:
            query.append(f"WHERE {self._where.where_part}")
        query_str = ' '.join(query)
        query_str += ";"
        return query_str
