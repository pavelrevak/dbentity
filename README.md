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

---

## Quick Start

```python
from dbentity.db_entity import DbEntity
from dbentity.attribute import IndexAttribute, StringAttribute, IntegerAttribute
from dbentity.db_control import OrderByDesc, Limit

class User(DbEntity):
    TABLE = 'users'
    ITEMS = (
        IndexAttribute(),
        StringAttribute('name'),
        IntegerAttribute('age'),
    )

# Query
users = User.db_list(db, OrderByDesc('age'), Limit(10))
user = User.db_get(db, uid=123)

# Create
user = User.create(db, name='Jane', age=25)

# Update
user.age = 26
user.db_save(db)

# Delete
user.db_delete(db)
```

---

## Modules

### dbentity.entity

Base entity class for data objects.

| Class | Description |
|-------|-------------|
| `Entity` | Base class. Define attributes via `ITEMS` tuple. |
| `EntityError` | Base exception. |

### dbentity.db_entity

Entity with database operations.

| Class | Description |
|-------|-------------|
| `DbEntity` | Entity with CRUD operations. Requires `TABLE` attribute. |
| `DbEntityError` | Database entity exception. |

**DbEntity Methods:**

| Method | Description |
|--------|-------------|
| `db_list(db, *args, **kwargs)` | Return list of matching entities. |
| `db_get(db, *args, **kwargs)` | Return first matching entity or None. |
| `db_count(db, *args, **kwargs)` | Return count of matching rows. |
| `db_count_by(db, columns, *args, **kwargs)` | Return count grouped by column(s). |
| `db_exists(db, *args, **kwargs)` | Return True if any match exists. |
| `db_distinct(db, columns, *args, **kwargs)` | Return distinct values for column(s). |
| `create(db, **kwargs)` | Create and return new entity. |
| `db_save(db)` | Insert or update entity. |
| `db_insert(db)` | Insert entity. |
| `db_update(db)` | Update modified attributes. |
| `db_delete(db)` | Delete entity. |
| `delete_by(db, *args, **kwargs)` | Delete matching rows. |

---

## Attributes

### dbentity.attribute

| Attribute | Description |
|-----------|-------------|
| `IndexAttribute(name='uid', db_key='id')` | Primary key (not in INSERT/UPDATE). |
| `CreateIndexAttribute()` | Primary key included in INSERT. |
| `StringAttribute(name)` | Text field. |
| `IntegerAttribute(name, minimal=None, maximal=None)` | Integer with optional range. |
| `FixedPointAttribute(name, fp=2)` | Decimal stored as int (fp=2 → value×100). |
| `BooleanAttribute(name)` | Boolean field. |
| `BytesAttribute(name)` | Binary data. |
| `PasswordAttribute(name)` | Hidden in templates. |
| `DatetimeAttribute(name)` | Datetime with formatting. |
| `LastTimeAttribute(name)` | Elapsed time since timestamp. |
| `ConnectionAttribute(name, sub_entity)` | Foreign key (db_key defaults to `{name}_id`). |
| `SubElementsAttribute(name)` | One-to-many (not persisted). |
| `SumIntegerAttribute(name)` | Integer with SUM aggregation. |
| `SumFixedPointAttribute(name, fp)` | Fixed-point with SUM aggregation. |

**Common parameters:**
- `name` - Attribute name in Python
- `db_key` - Database column name (default: same as name)
- `form_key` - Form field name for data binding
- `default` - Default value

---

## Query Controls

### dbentity.db_control

#### WHERE Conditions

| Control | SQL | Example |
|---------|-----|---------|
| `Where(name='John')` | `name = 'John'` | Equality |
| `Where(age=[25,30])` | `age IN (25, 30)` | List → IN |
| `Where(name=None)` | `name IS NULL` | None → IS NULL |
| `And(a=1, b=2)` | `a = 1 AND b = 2` | AND logic |
| `Or(Where(a=1), Where(b=2))` | `a = 1 OR b = 2` | OR logic |
| `Not(active=True)` | `NOT active = true` | Negation |
| `Lt(age=30)` | `age < 30` | Less than |
| `Gt(age=18)` | `age > 18` | Greater than |
| `Le(age=65)` | `age <= 65` | Less or equal |
| `Ge(age=18)` | `age >= 18` | Greater or equal |
| `Like(name='John%')` | `name LIKE 'John%'` | Pattern match |
| `ILike(name='%john%')` | `name ILIKE '%john%'` | Case insensitive (PostgreSQL) |
| `IsNull('name')` | `name IS NULL` | Explicit NULL check |
| `IsNotNull('name')` | `name IS NOT NULL` | NOT NULL check |
| `Between('age', 18, 65)` | `age BETWEEN 18 AND 65` | Range |
| `BitwiseAnd(flags=4)` | `flags & 4 > 0` | Bitwise check |

#### ORDER, LIMIT, GROUP

| Control | SQL |
|---------|-----|
| `OrderBy('name')` | `ORDER BY name` |
| `OrderByAsc('name')` | `ORDER BY name ASC` |
| `OrderByDesc('age')` | `ORDER BY age DESC` |
| `Limit(10)` | `LIMIT 10` |
| `Offset(20)` | `OFFSET 20` |
| `GroupBy('status')` | `GROUP BY status` |

#### JOIN

| Control | SQL |
|---------|-----|
| `LeftJoin('author')` | `LEFT JOIN ... ON ...` |
| `RightJoin('author')` | `RIGHT JOIN ... ON ...` |
| `InnerJoin('author')` | `INNER JOIN ... ON ...` |
| `FullJoin('author')` | `FULL JOIN ... ON ...` |

---

## SQL Examples

### Basic Queries

```python
# SELECT all
User.db_list(db)
# SQL: SELECT users.id, users.name, users.age FROM users;

# SELECT with WHERE
User.db_list(db, name='John')
# SQL: SELECT ... FROM users WHERE users.name = %s;
# Args: ['John']

# SELECT with multiple conditions
User.db_list(db, name='John', age=30)
# SQL: SELECT ... FROM users WHERE users.name = %s AND users.age = %s;
# Args: ['John', 30]
```

### Comparisons

```python
# Greater than
User.db_list(db, Gt(age=18))
# SQL: SELECT ... FROM users WHERE users.age > %s;
# Args: [18]

# Range with Between
User.db_list(db, Between('age', 18, 65))
# SQL: SELECT ... FROM users WHERE users.age BETWEEN %s AND %s;
# Args: [18, 65]

# IN clause (pass list)
User.db_list(db, age=[25, 30, 35])
# SQL: SELECT ... FROM users WHERE users.age IN (%s, %s, %s);
# Args: [25, 30, 35]
```

### Boolean Logic

```python
# OR
User.db_list(db, Or(Where(name='John'), Where(name='Jane')))
# SQL: SELECT ... FROM users WHERE (users.name = %s OR users.name = %s);
# Args: ['John', 'Jane']

# NOT
User.db_list(db, Not(active=True))
# SQL: SELECT ... FROM users WHERE NOT users.active = %s;
# Args: [True]

# Combined
User.db_list(db, And(Gt(age=18), Lt(age=65)), active=True)
# SQL: SELECT ... WHERE (users.age > %s AND users.age < %s) AND users.active = %s;
```

### Pattern Matching

```python
# LIKE (case sensitive)
User.db_list(db, Like(name='John%'))
# SQL: SELECT ... FROM users WHERE users.name LIKE %s;
# Args: ['John%']

# ILIKE (case insensitive, PostgreSQL)
User.db_list(db, ILike(name='%john%'))
# SQL: SELECT ... FROM users WHERE users.name ILIKE %s;
# Args: ['%john%']
```

### Ordering and Pagination

```python
User.db_list(db, OrderByDesc('age'), Limit(10), Offset(20))
# SQL: SELECT ... FROM users ORDER BY users.age DESC LIMIT 10 OFFSET 20;
```

### JOIN

```python
class Post(DbEntity):
    TABLE = 'posts'
    ITEMS = (
        IndexAttribute(),
        StringAttribute('title'),
        ConnectionAttribute('author', sub_entity=User),
    )

# LEFT JOIN
Post.db_list(db, LeftJoin('author'))
# SQL: SELECT posts.id, posts.title, __author.id, __author.name, __author.age
#      FROM posts
#      LEFT JOIN users AS __author ON posts.author_id = __author.id;

# JOIN with condition
Post.db_list(db, LeftJoin('author', name='John'))
# SQL: SELECT ... FROM posts
#      LEFT JOIN users AS __author ON posts.author_id = __author.id
#      WHERE __author.name = %s;
# Args: ['John']
```

### Count and Exists

```python
# Count
User.db_count(db, active=True)
# SQL: SELECT COUNT(*) FROM users WHERE users.active = %s;
# Args: [True]

# Exists
User.db_exists(db, name='John')
# Returns: True/False
```

### Distinct

```python
# Single column - returns list of values
User.db_distinct(db, 'name')
# SQL: SELECT DISTINCT users.name FROM users ORDER BY users.name;
# Returns: ['Alice', 'Bob', 'John']

# Multiple columns - returns list of tuples
User.db_distinct(db, ('name', 'age'))
# SQL: SELECT DISTINCT users.name, users.age FROM users ORDER BY users.name, users.age;
# Returns: [('Alice', 25), ('Bob', 30), ('John', 35)]

# With WHERE condition
User.db_distinct(db, 'name', active=True)
# SQL: SELECT DISTINCT users.name FROM users WHERE users.active = %s ORDER BY users.name;
# Args: [True]
```

### Count By (GROUP BY)

```python
# Single column - returns list of (value, count) tuples, ordered by count DESC
User.db_count_by(db, 'country')
# SQL: SELECT users.country, COUNT(*) AS _cnt FROM users GROUP BY users.country ORDER BY _cnt DESC;
# Returns: [('SK', 150), ('CZ', 80), ('PL', 45)]

# Multiple columns - returns list of ((values), count) tuples
User.db_count_by(db, ('country', 'role'))
# SQL: SELECT users.country, users.role, COUNT(*) AS _cnt
#      FROM users GROUP BY users.country, users.role ORDER BY _cnt DESC;
# Returns: [(('SK', 'user'), 140), (('SK', 'admin'), 10), (('CZ', 'user'), 75)]

# With WHERE and LIMIT
User.db_count_by(db, 'country', Limit(5), active=True)
# SQL: SELECT users.country, COUNT(*) AS _cnt FROM users
#      WHERE users.active = %s GROUP BY users.country ORDER BY _cnt DESC LIMIT 5;
# Args: [True]

# Order by count ASC (least first)
User.db_count_by(db, 'country', OrderByAsc('_cnt'))
# SQL: SELECT users.country, COUNT(*) AS _cnt FROM users GROUP BY users.country ORDER BY _cnt ASC;
```

### Delete

```python
# Delete single entity
user.db_delete(db)
# SQL: DELETE FROM users WHERE id = %s;

# Delete by condition
User.delete_by(db, active=False)
# SQL: DELETE FROM users WHERE users.active = %s;
# Args: [False]
```

---

## Database Migrations

### dbentity.db_upgrade

```python
from dbentity.db_upgrade import db_upgrade

SQL_UPGRADE_FILES = [
    (1, 'upgrade_001.sql'),
    (2, 'upgrade_002.sql'),
]

db_upgrade(db, log, 'sql/', 'init.sql', SQL_UPGRADE_FILES)
```

The `db_upgrade()` function:
1. Checks for `db_version` table
2. If missing, runs `init.sql` (full schema)
3. If present, runs upgrade files with version > current
4. Updates version after each upgrade

---

## License

MIT
