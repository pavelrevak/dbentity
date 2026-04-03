"""Database connection wrapper with optional query logging."""

import time as _time


class DbConnection:
    """Wrapper around database connection with optional SQL logging.

    Args:
        db: Database connection with execute(), cursor(), commit(), transaction().
        log: Optional logger with is_debug property and debug() method.
    """

    def __init__(self, db, log=None, on_error=None):
        self._db = db
        self._log = log
        self._on_error = on_error
        cur = db.cursor()
        self._has_mogrify = hasattr(cur, 'mogrify')
        cur.close()

    def _mogrify(self, query, args):
        cur = self._db.cursor()
        try:
            q = cur.mogrify(query, args)
            return q.decode() if isinstance(q, bytes) else q
        finally:
            cur.close()

    def execute(self, query, args=None):
        try:
            if self._log and self._log.is_debug:
                sql = self._mogrify(query, args) if self._has_mogrify else None
                t = _time.monotonic()
                cur = self._db.execute(query, args)
                t = _time.monotonic() - t
                if sql is None:
                    sql = f"{query} ARGS: {args}" if args else query
                self._log.debug("SQL (%.3fs): %s", t, sql)
            else:
                cur = self._db.execute(query, args)
            return cur
        except Exception as err:
            if self._on_error:
                self._on_error(err)
            raise

    def cursor(self):
        return self._db.cursor()

    def commit(self):
        return self._db.commit()

    def transaction(self):
        return self._db.transaction()
