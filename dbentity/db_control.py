import dbentity.attribute as _attribute


class EntityControlError(Exception):
    """General Data object error"""


class Control():
    pass


class Where(Control):
    """Where
    """
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._where_parts = []
        self._where_args = []

    @property
    def count_parts(self):
        return len(self._where_parts)

    def add(self, *args, **kwargs):
        if args:
            self._args += args
        if kwargs:
            self._kwargs.update(kwargs)

    def add_where_part(self, part):
        if part:
            self._where_parts.append(part)

    def extend_where_args(self, args):
        self._where_args.extend(args)

    def add_where_arg(self, arg):
        self._where_args.append(arg)

    def add_where(self, where, parentheses=False):
        if where.count_parts:
            parts = where.where_part
            if parentheses and where.count_parts > 1:
                parts = f'({parts})'
            self.add_where_part(parts)
            self.extend_where_args(where.args)

    def process(self, entity, alias):
        for key, val in self._kwargs.items():
            item = entity.get_item(key)
            if item:
                if isinstance(val, (tuple, list, set)):
                    values = set(val)
                    if None in values:
                        values.remove(None)
                        where_list = ", ".join(["%s"] * len(values))
                        self.add_where_part(
                            f'({alias}.{item.db_key} IN ({where_list})'
                            f' OR {alias}.{item.db_key} IS NULL)')
                    else:
                        where_list = ", ".join(["%s"] * len(values))
                        self.add_where_part(
                            f'{alias}.{item.db_key} IN ({where_list})')
                    self.extend_where_args(values)
                else:
                    if val is None:
                        self.add_where_part(f'{alias}.{item.db_key} IS NULL')
                    else:
                        self.add_where_part(f'{alias}.{item.db_key} = %s')
                        self.add_where_arg(val)
            else:
                raise EntityControlError(f"Unknown argument '{key}'")
        for control in self._args:
            if isinstance(control, Where):
                control.process(entity, alias)
                self.add_where(control, parentheses=True)

    @property
    def args(self):
        return self._where_args

    @property
    def where_part(self):
        return ' AND '.join(self._where_parts)


class And(Where):
    """Where AND
    """
    # @property
    # def where_part(self):
    #     return ' AND '.join(self._where_parts)


class Or(Where):
    """Where OR
    """
    @property
    def where_part(self):
        return ' OR '.join(self._where_parts)


class Not(Where):
    """Where NOT
    """
    def add_where_part(self, part):
        if part:
            self._where_parts.append('NOT ' + part)


class Nand(Not):
    """Where NAND

    NAND is not needed, is default in all statements
    """
    # @property
    # def where_part(self):
    #     return ' AND '.join(self._where_parts)


class Nor(Not):
    """Where NOR
    """
    @property
    def where_part(self):
        return ' OR '.join(self._where_parts)


class Lt(Where):
    """Less than
    """
    def process(self, entity, alias):
        for key, val in self._kwargs.items():
            item = entity.get_item(key)
            if item:
                if val is not None:
                    self.add_where_part(f'{alias}.{item.db_key} < %s')
                    self.add_where_arg(val)
            else:
                raise EntityControlError(f"Unknown argument '{key}'")


class Gt(Where):
    """Greater than
    """
    def process(self, entity, alias):
        for key, val in self._kwargs.items():
            item = entity.get_item(key)
            if item:
                if val is not None:
                    self.add_where_part(f'{alias}.{item.db_key} > %s')
                    self.add_where_arg(val)
            else:
                raise EntityControlError(f"Unknown argument '{key}'")


class Le(Where):
    """Less than
    """
    def process(self, entity, alias):
        for key, val in self._kwargs.items():
            item = entity.get_item(key)
            if item:
                if val is not None:
                    self.add_where_part(f'{alias}.{item.db_key} <= %s')
                    self.add_where_arg(val)
            else:
                raise EntityControlError(f"Unknown argument '{key}'")


class Ge(Where):
    """Greater than
    """
    def process(self, entity, alias):
        for key, val in self._kwargs.items():
            item = entity.get_item(key)
            if item:
                if val is not None:
                    self.add_where_part(f'{alias}.{item.db_key} >= %s')
                    self.add_where_arg(val)
            else:
                raise EntityControlError(f"Unknown argument '{key}'")


class BitwiseAnd(Where):
    """BitWise AND
    """
    def process(self, entity, alias):
        for key, val in self._kwargs.items():
            item = entity.get_item(key)
            if item:
                if val:
                    self.add_where_part(f'{alias}.{item.db_key} & %s > 0')
                    self.add_where_arg(val)
            else:
                raise EntityControlError(f"Unknown argument '{key}'")


class Like(Where):
    """LIKE pattern matching (case sensitive)
    """
    def process(self, entity, alias):
        for key, val in self._kwargs.items():
            item = entity.get_item(key)
            if item:
                if val is not None:
                    self.add_where_part(f'{alias}.{item.db_key} LIKE %s')
                    self.add_where_arg(val)
            else:
                raise EntityControlError(f"Unknown argument '{key}'")


class ILike(Where):
    """ILIKE pattern matching (case insensitive, PostgreSQL)
    """
    def process(self, entity, alias):
        for key, val in self._kwargs.items():
            item = entity.get_item(key)
            if item:
                if val is not None:
                    self.add_where_part(f'{alias}.{item.db_key} ILIKE %s')
                    self.add_where_arg(val)
            else:
                raise EntityControlError(f"Unknown argument '{key}'")


class IsNull(Where):
    """IS NULL check
    """
    def __init__(self, *columns):
        super().__init__()
        self._columns = columns

    def process(self, entity, alias):
        for col in self._columns:
            item = entity.get_item(col)
            if item:
                self.add_where_part(f'{alias}.{item.db_key} IS NULL')
            else:
                raise EntityControlError(f"Unknown argument '{col}'")


class IsNotNull(Where):
    """IS NOT NULL check
    """
    def __init__(self, *columns):
        super().__init__()
        self._columns = columns

    def process(self, entity, alias):
        for col in self._columns:
            item = entity.get_item(col)
            if item:
                self.add_where_part(f'{alias}.{item.db_key} IS NOT NULL')
            else:
                raise EntityControlError(f"Unknown argument '{col}'")


class Between(Where):
    """BETWEEN range check
    """
    def __init__(self, column, min_val, max_val):
        super().__init__()
        self._column = column
        self._min = min_val
        self._max = max_val

    def process(self, entity, alias):
        item = entity.get_item(self._column)
        if item:
            self.add_where_part(f'{alias}.{item.db_key} BETWEEN %s AND %s')
            self.add_where_arg(self._min)
            self.add_where_arg(self._max)
        else:
            raise EntityControlError(f"Unknown argument '{self._column}'")


class GroupBy(Control):
    def __init__(self, column):
        self._column = column

    def get_group_by_part(self, entity, alias=None):
        item = entity.get_item(self._column)
        if not item:
            raise EntityControlError(f"Column '{self._column}' not found")
        if alias is None:
            alias = entity.TABLE
        part_str = f'{alias}.{item.db_key}'
        return part_str


class OrderBy(Control):
    def __init__(self, column, direction=None):
        self._column = column
        self._direction = direction

    def get_order_part(self, entity, alias=None):
        item = entity.get_item(self._column)
        if not item:
            raise EntityControlError(f"Column '{self._column}' not found")
        if alias is None:
            alias = entity.TABLE
        part_str = f'{alias}.{item.db_key}'
        if self._direction:
            part_str += f' {self._direction}'
        return part_str


class OrderByAsc(OrderBy):
    def __init__(self, column):
        super().__init__(column, 'ASC')


class OrderByDesc(OrderBy):
    def __init__(self, column):
        super().__init__(column, 'DESC')


class Limit(Control):
    def __init__(self, limit):
        self._limit = limit

    def get_limit(self):
        return f'LIMIT {self._limit:d}'


class Offset(Control):
    def __init__(self, limit):
        self._limit = limit

    def get_offset(self):
        return f'OFFSET {self._limit:d}'


class BaseJoin(Control):
    JOIN_TYPE = ''

    def __init__(self, column, *args, **kwargs):
        self._column = column
        self._args = args
        self._kwargs = kwargs
        self._where = And(*args, **kwargs)
        self._order_parts = []

    def find_conn_attr(self, entity):
        conn_attr = entity.get_item(self._column)
        if conn_attr is None:
            raise EntityControlError(f"Attribute '{self._column}' not found")
        if not isinstance(conn_attr, _attribute.ConnectionAttribute):
            raise EntityControlError(
                f"Attribute '{self._column}' is not ConnectionAttribute")
        return conn_attr

    def join_expr(self, query, sub_entity, conn_attr, join_on, alias):
        table = sub_entity.TABLE
        join_str = f'{self.JOIN_TYPE} JOIN {table} AS {alias}'
        join_str += f' ON {join_on}.{conn_attr.db_key} = {alias}.{conn_attr.conn_key}'
        query.add_join_part(join_str)

    def select_expr(self, _base_table):
        query_part = ''
        return query_part

    def add_join(self, query, entity, alias=None):
        if alias:
            join_on = alias
            alias = f'{alias}__{self._column}'
        else:
            join_on = query.entity.TABLE
            alias = f'__{self._column}'
        column_prefix = alias.lstrip('__').replace('__', '.')
        if len(alias) > 63:
            raise EntityControlError(
                f"Alias has more than 63 characters: '{alias}'")
        conn_attr = self.find_conn_attr(entity)
        sub_entity = conn_attr.sub_entity or entity
        self.join_expr(query, sub_entity, conn_attr, join_on, alias)
        query.extend_select_parts(sub_entity.select_parts(alias))
        query.extend_columns(sub_entity.table_columns(column_prefix))
        self._where.process(sub_entity, alias)
        query.where.add_where(self._where)
        for control in self._args:
            if isinstance(control, (LeftJoin, RightJoin)):
                control.add_join(query, sub_entity, alias)
            elif isinstance(control, OrderBy):
                query.add_order_part(control.get_order_part(
                    sub_entity, alias))
            elif isinstance(control, GroupBy):
                query.add_group_by_part(control.get_group_by_part(
                    sub_entity, alias))
            elif isinstance(control, Where):
                pass
            else:
                raise EntityControlError(f"Unknown argument '{control}'")

    def process(self, query):
        self.add_join(query, query.entity)


class LeftJoin(BaseJoin):
    JOIN_TYPE = 'LEFT'


class RightJoin(BaseJoin):
    JOIN_TYPE = 'RIGHT'


class InnerJoin(BaseJoin):
    JOIN_TYPE = 'INNER'


class FullJoin(BaseJoin):
    JOIN_TYPE = 'FULL'
