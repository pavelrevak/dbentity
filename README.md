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
- Non-blocking query execution for `select()`-based event loops
- Lightweight: only `psycopg>=3.1` required

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

**Entity Methods:**

| Method | Description |
|--------|-------------|
| `set_from_data(params)` | Update entity from data dict (e.g. JSON). Only SAVE-able, non-INDEX attributes. |
| `set_from_form_data(params)` | Update entity from form data using form_key mappings. |
| `get_json_data()` | Return dict formatted for JSON serialization. |
| `get_template_data()` | Return dict formatted for templates. |

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
| `create_from_data(db, params, **kwargs)` | Create entity from data dict (e.g. JSON). |
| `create_from_form_data(db, params, **kwargs)` | Create entity from form data using form_key mappings. |
| `db_upsert(db, conflict, update=None, **kwargs)` | INSERT ... ON CONFLICT ... DO UPDATE ... RETURNING. |
| `upsert_from_data(db, params, conflict, update=None, **kwargs)` | Upsert from data dict (e.g. JSON). |
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

# With controls (OrderBy, Limit, Where conditions)
User.db_distinct(db, 'name', Gt(age=18), OrderByDesc('name'), Limit(10))
# SQL: SELECT DISTINCT users.name FROM users WHERE users.age > %s ORDER BY users.name DESC LIMIT %s;
# Args: [18, 10]
```

### Count By (GROUP BY)

```python
# Single column - returns list of (value, count) tuples
User.db_count_by(db, 'country')
# SQL: SELECT users.country, COUNT(*) AS _cnt FROM users GROUP BY users.country;
# Returns: [('SK', 150), ('CZ', 80), ('PL', 45)]

# Multiple columns - returns list of ((values), count) tuples
User.db_count_by(db, ('country', 'role'))
# SQL: SELECT users.country, users.role, COUNT(*) AS _cnt
#      FROM users GROUP BY users.country, users.role;
# Returns: [(('SK', 'user'), 140), (('SK', 'admin'), 10), (('CZ', 'user'), 75)]

# Order by count DESC (most first)
User.db_count_by(db, 'country', OrderByDesc('_cnt'))
# SQL: SELECT ... GROUP BY users.country ORDER BY _cnt DESC;

# Order by count ASC (least first)
User.db_count_by(db, 'country', OrderByAsc('_cnt'))
# SQL: SELECT ... GROUP BY users.country ORDER BY _cnt ASC;

# With WHERE and LIMIT
User.db_count_by(db, 'country', OrderByDesc('_cnt'), Limit(5), active=True)
# SQL: SELECT users.country, COUNT(*) AS _cnt FROM users
#      WHERE users.active = %s GROUP BY users.country ORDER BY _cnt DESC LIMIT 5;
```

### Create/Update from Data (JSON)

```python
# Create from JSON data (only SAVE-able, non-INDEX attributes accepted)
user = User.create_from_data(db, {'name': 'John', 'age': 30})

# Update from JSON data
user.set_from_data({'name': 'Jane', 'age': 25})
user.db_save(db)

# Extra kwargs are passed to create()
user = User.create_from_data(db, {'name': 'John'}, age=30)
```

### Upsert (INSERT ... ON CONFLICT ... DO UPDATE)

`db_upsert()` performs an atomic INSERT-or-UPDATE on conflict and
returns the resulting entity. The conflict target must be backed by
a UNIQUE constraint or unique index in the schema.

```python
# Insert or update on 'email' conflict.
# Default: every column being inserted is updated on conflict
# except the conflict target itself.
user = User.db_upsert(
    db, conflict='email',
    email='john@example.com', name='John', age=30)
# SQL: INSERT INTO users (email, name, age) VALUES (%s, %s, %s)
#      ON CONFLICT (email) DO UPDATE
#      SET name = EXCLUDED.name, age = EXCLUDED.age
#      RETURNING users.id AS uid, users.email, ...;

# Multi-column conflict target.
mapping = ClientDevice.db_upsert(
    db, conflict=('client_id', 'device_id'),
    client_id=42, device_id=7, label='gate-1')

# Selective update — only refresh `last_seen` on conflict.
sess = Session.db_upsert(
    db, conflict='token', update=['last_seen'],
    token='abc...', user_id=1, last_seen=now)

# DO NOTHING — insert if new, otherwise no-op (returns None on conflict).
maybe = AuditLog.db_upsert(
    db, conflict='hash', update=[],
    hash='...', payload='...')

# Upsert from JSON data dict (mirrors create_from_data).
user = User.upsert_from_data(
    db, {'email': 'john@example.com', 'name': 'John', 'age': 30},
    conflict='email')
```

`uid` is returned for both INSERT and UPDATE branches (PG `RETURNING`
fires in both cases).

**Sequence burn warning:** PG calls `nextval()` on the id sequence for
every upsert call, even when the UPDATE branch is taken. With `SERIAL`
PKs at high upsert rates this can burn id space; prefer `BIGSERIAL` or
natural keys for hot upsert paths.

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

## Database Connection Wrapper

### dbentity.db_connection

Optional wrapper around database connection with SQL query logging.

```python
from dbentity.db_connection import DbConnection

db = DbConnection(raw_connection, log=my_logger)

# All queries are now logged at debug level (if log.is_debug is True)
users = User.db_list(db, name='John')
# LOG: SQL: SELECT users.id, users.name, users.age FROM users WHERE users.name = 'John';
```

| Class | Description |
|-------|-------------|
| `DbConnection(db, log=None)` | Wraps connection. Logger needs `is_debug` property and `debug()` method. |

---

## Non-blocking Query Mode

For applications using `select()` event loop (not asyncio), queries can be split into
build and execute phases. This allows sending a query to PostgreSQL and processing the
result later when the socket becomes readable.

### Building queries without execution

```python
from dbentity.db_query import Select, Distinct, CountBy
from dbentity.db_control import Gt, OrderByDesc, Limit

# SELECT query - returns Select object with query_str, args, create_objects()
query = User.db_query(Gt(age=18), OrderByDesc('age'), Limit(10))
query.query_str       # "SELECT ... WHERE users.age > %s ... LIMIT %s;"
query.pg_query_bytes  # same SQL with $1, $2 placeholders for libpq
query.args            # [18, 10]

# DISTINCT query
query = Distinct(User, 'name', Gt(age=18), Limit(5))
query.query_str  # "SELECT DISTINCT users.name FROM users WHERE users.age > %s ..."

# COUNT BY query
query = CountBy(User, 'country', active=True)
query.query_str  # "SELECT users.country, COUNT(*) AS _cnt FROM users WHERE ..."
```

Both `query_str` (with `%s`, for `cursor.execute()`) and `pg_query_bytes`
(with `$1`, `$2`, for `pgconn.send_query_params()`) are exposed. The latter
is cached on first access.

### dbentity.db_async — high-level non-blocking API

`dbentity.db_async` wraps the libpq plumbing in two classes:

- **`AsyncQuery(conn, query)`** — drives one query through psycopg3's
  low-level `pgconn` API. Caller registers `aq.fileno()` in their
  `select()`/`poll()` loop and forwards `on_readable()` / `on_writable()`
  events. When `on_readable()` returns `True`, call `aq.result()` to get
  the same shape as `db_list()`.

- **`AsyncConnectionPool(conninfo, min_size, max_size, ...)`** —
  thread-free, lock-free pool of non-blocking connections designed for a
  single-threaded event-loop worker. Pre-opens `min_size` conns on
  `open()`, grows on demand up to `max_size`, refills back to `min_size`
  on broken-release, and FIFO-rotates idle conns. Includes a connect
  circuit breaker (see below). Includes `prune_idle(ttl)` to drop
  long-idle conns above `min_size` from a periodic event-loop tick.

#### Pool example

```python
from dbentity.db_async import (
    AsyncConnectionPool, AsyncQuery,
    PoolError, PoolTimeout, PoolUnavailable,
)

pool = AsyncConnectionPool(
    'dbname=mydb', min_size=2, max_size=10)
pool.open()

try:
    conn = pool.acquire()
except PoolTimeout:
    # All max_size conns are busy — return 503.
    return http_503()
except PoolUnavailable:
    # DB is down, circuit breaker open — return 503.
    return http_503()

aq = AsyncQuery(conn, User.db_query(Gt(age=18), Limit(10)))
try:
    aq.start()
    # event loop:
    #   register_reader(aq.fileno(), on_readable_cb)
    #   if aq.needs_write(): register_writer(aq.fileno(), on_writable_cb)
    # in on_readable_cb:
    #   if aq.on_readable():
    #       users = aq.result()       # list[User]
    #       pool.release(conn)
except Exception:
    pool.release(conn, broken=True)
    raise
```

#### Pool sizing & lifecycle

- `min_size` connections are opened on `pool.open()` and the pool is
  refilled back to `min_size` after every broken / non-IDLE release
  (no background thread; refill happens inline in `release()`).
- `max_size` is the hard cap on total open conns (idle + busy);
  `acquire()` past it raises `PoolTimeout` immediately — never blocks.
- Idle conns are recycled FIFO (oldest first) so long-lived workers
  don't accumulate stale TCP sockets.
- Call `pool.prune_idle(ttl_seconds)` from a periodic tick (e.g.
  `Worker.on_idle`) to close conns idle longer than `ttl_seconds`,
  but never below `min_size`.

#### Circuit breaker (connect failure)

When `_make_conn` fails, the pool opens a circuit breaker:

- The first failure logs a `WARNING` with the underlying error and the
  cooldown.
- Cooldown follows exponential backoff `1s → 2s → 4s → 8s` (capped at
  `COOLDOWN_MAX`) with ±25 % jitter (`COOLDOWN_JITTER`) to prevent
  multiple workers from stampeding the recovering DB in lockstep.
- During cooldown, every `acquire()` raises `PoolUnavailable`
  immediately without touching the network — caller responds 503.
- Spam protection: only one "still unavailable" `WARNING` per
  `UNAVAILABLE_LOG_INTERVAL` (60 s default), regardless of how many
  requests hit the breaker.
- On the first successful reconnect after a streak, an `INFO` line is
  logged and the breaker resets.

The cooldown parameters can be overridden per pool:

```python
pool = AsyncConnectionPool(
    'dbname=mydb', min_size=2, max_size=10,
    cooldown_initial=0.5, cooldown_max=30.0, cooldown_jitter=0.5)
```

#### Pool exceptions

All pool exceptions inherit from `PoolError`, so a single `except
PoolError` catches every failure mode:

| Exception | Meaning |
|-----------|---------|
| `PoolClosed` | `acquire()` after `close()`. Permanent. |
| `PoolTimeout` | Pool exhausted (`busy == max_size`). Transient. |
| `PoolUnavailable` | Backend connect failed; breaker open. Retry after cooldown. |

#### Pool status / healthcheck

`pool.status()` returns a snapshot dict for healthchecks, metrics, and
worker pause/resume logic:

```python
{
    'min_size': 2, 'max_size': 10,
    'size': 5, 'idle': 3, 'busy': 2,
    'free': 8,                  # max_size - busy; how many acquire() will succeed
    'closed': False,
    'available': True,          # False while breaker is open
    'consecutive_connect_failures': 0,
    'retry_in': 0.0,            # seconds until breaker allows next probe
}
```

`free == 0` means the pool is fully busy and the worker should pause
accepting new requests until a `release()` frees a slot.
`available == False` means the DB is currently unreachable and the
worker should fail-fast new requests with 503.

#### Graceful shutdown

```python
worker.draining = True       # 1. stop accepting new requests
pool.cancel_busy()           # 2. server-side cancel of in-flight queries
worker.run_once()            # 3. tick the loop so callbacks observe errors
pool.close()                 # 4. close everything
```

`pool.cancel_busy()` is **blocking** (libpq `PGcancel` opens a fresh
TCP connection synchronously); only call from shutdown code, never from
inside the event loop.

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
