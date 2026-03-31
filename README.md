# dbentity

Lightweight Python ORM library for PostgreSQL.

## Installation

```bash
pip install dbentity
```

## Features

- Declarative entity definitions with typed attributes
- Automatic SQL query generation
- Support for JOIN operations
- Query builder with boolean logic (AND, OR, NOT)
- Database migration support
- No external dependencies

## Quick Start

```python
from dbentity.db_entity import DbEntity
from dbentity.attribute import IndexAttribute, StringAttribute, IntegerAttribute
from dbentity.db_control import OrderByDesc, Limit

class User(DbEntity):
    TABLE = 'users'
    ITEMS = (
        IndexAttribute(),
        StringAttribute('name', form_key='user_name'),
        IntegerAttribute('age', minimal=0, maximal=150),
    )

# Query with controls
users = User.db_list(db, OrderByDesc('age'), Limit(10), name='John')
user = User.db_get(db, uid=123)

# Create new entity
user = User.create(db, name='Jane', age=25)

# Update
user.age = 26
user.db_save(db)

# Delete
user.db_delete(db)
```

## Attribute Types

- `IndexAttribute` / `CreateIndexAttribute` - Primary key
- `StringAttribute` - Text fields
- `IntegerAttribute` - Integer with optional min/max
- `FixedPointAttribute` - Decimal numbers
- `BooleanAttribute` - Boolean values
- `BytesAttribute` - Binary data
- `PasswordAttribute` - Password fields (hidden in templates)
- `DatetimeAttribute` - Datetime values
- `ConnectionAttribute` - Foreign key relationships
- `SubElementsAttribute` - One-to-many relationships

## Query Controls

```python
from dbentity.db_control import (
    Where, And, Or, Not,
    Lt, Gt, Le, Ge,
    OrderBy, OrderByAsc, OrderByDesc,
    Limit, Offset,
    LeftJoin, RightJoin,
    GroupBy,
)

# Complex queries
users = User.db_list(
    db,
    Or(Where(name='John'), Where(name='Jane')),
    Gt(age=18),
    Lt(age=65),
    OrderByDesc('age'),
    Limit(10),
)

# Joins
posts = Post.db_list(
    db,
    LeftJoin('author', name='John'),
    OrderByDesc('created_at'),
)
```

## Database Migrations

```python
from dbentity.db_upgrade import db_upgrade

SQL_UPGRADE_FILES = [
    (1, 'upgrade_001.sql'),
    (2, 'upgrade_002.sql'),
]

db_upgrade(db, log, 'sql/', 'init.sql', SQL_UPGRADE_FILES)
```

## License

MIT
