"""PostgreSQL non-blocking query execution for dbentity Query objects.

Wraps psycopg3 low-level pgconn API. Caller is responsible for IO event loop
integration: register fileno() in select/poll loop, drive on_readable() /
on_writable() callbacks. This module does NOT do any IO multiplexing itself.

Typical usage with an external select-based event loop:

    from dbentity.db_async import AsyncQuery

    aq = AsyncQuery(conn, query)         # query = Select / Distinct / ...
    aq.start()
    # event loop:
    #   register_reader(aq.fileno(), on_readable_cb)
    #   if aq.needs_write(): register_writer(aq.fileno(), on_writable_cb)
    #
    # in on_readable_cb:
    #   if aq.on_readable():             # True = query complete
    #       objs = aq.result()           # same shape as blocking db_list()
    #
    # in on_writable_cb:
    #   aq.on_writable()
    #   if not aq.needs_write():
    #       unregister_writer(...)
"""

import logging as _logging
import random as _random
import time as _time

import psycopg as _psycopg
import psycopg.adapt as _psycopg_adapt
import psycopg.pq as _psycopg_pq

import dbentity.db_connection as _db_connection


_log = _logging.getLogger(__name__)


class AsyncQueryError(Exception):
    """Non-blocking query execution error."""


class PoolError(Exception):
    """Base for AsyncConnectionPool errors.

    Catch this to handle any pool failure (closed, exhausted, backend
    unreachable) without inspecting str() of the exception.
    """


class PoolClosed(PoolError):
    """Operation attempted on a closed pool."""


class PoolTimeout(PoolError):
    """Pool exhausted: ``max_size`` connections already busy.

    Transient — caller may retry on a later request.
    """


class PoolUnavailable(PoolError):
    """Backend connect failed; circuit breaker is open.

    The pool is currently in a connect-failure cooldown and is not
    attempting new connections to the database. Caller should respond
    503 immediately and not retry until cooldown elapses (see
    ``status()['retry_in']``).
    """


class AsyncConnectionPool:
    """Lazy pool of non-blocking psycopg3 connections.

    No background threads. No locks (single-threaded use only — designed
    for event-loop workers that drive AsyncQuery via select/poll).

    Connections are pre-opened up to ``min_size`` on ``open()``, and grown
    on demand up to ``max_size`` when ``acquire()`` is called and no idle
    connection is available. On any error during use, the caller releases
    the connection with ``broken=True`` and the pool drops it; the next
    ``acquire()`` either returns another idle one or creates a new one.

    Acquire is non-blocking: if the pool is exhausted (``max_size`` already
    busy), ``acquire()`` raises ``PoolTimeout`` immediately. Callers in
    event-loop context should respond with HTTP 503 (overloaded) rather
    than wait, because waiting would freeze the loop.

    Args:
        conninfo: psycopg conninfo string passed to ``psycopg.connect``.
        min_size: number of connections to pre-open in ``open()``. Pool
            also refills back to ``min_size`` on every ``release()`` if
            broken / non-IDLE connections dropped it below the floor.
            ``min_size=0`` is allowed and means a fully lazy pool —
            ``open()`` does nothing and connections are created only
            on demand by ``acquire()``.
        max_size: hard cap on total open connections (idle + busy).

    Example:
        pool = AsyncConnectionPool(
            'dbname=mydb', min_size=5, max_size=10)
        pool.open()
        try:
            conn = pool.acquire()
        except PoolTimeout:
            # respond 503
            return
        try:
            aq = AsyncQuery(conn, query)
            aq.start()
            # ... drive via event loop ...
            result = aq.result()
        except Exception:
            pool.release(conn, broken=True)
            raise
        else:
            pool.release(conn)
    """

    #: Initial cooldown after the first failed connect attempt (seconds).
    COOLDOWN_INITIAL = 1.0
    #: Maximum cooldown between retries (seconds). Backoff doubles
    #: 1 -> 2 -> 4 -> 8 and then stays at this cap.
    COOLDOWN_MAX = 8.0
    #: Random jitter applied to each cooldown as a fraction of the base
    #: (e.g. 0.25 -> ±25 %). Spreads probe attempts across multiple
    #: workers/processes so they do not synchronously stampede the DB.
    COOLDOWN_JITTER = 0.25
    #: While the breaker is open, a "still unavailable" log line is
    #: emitted at most once per this many seconds (to avoid log spam).
    UNAVAILABLE_LOG_INTERVAL = 60.0

    def __init__(
            self, conninfo, min_size=1, max_size=5,
            cooldown_initial=None, cooldown_max=None, cooldown_jitter=None):
        if min_size < 0 or max_size < 1 or min_size > max_size:
            raise ValueError(
                'invalid pool sizes: '
                f'min_size={min_size}, max_size={max_size}')
        self._conninfo = conninfo
        self._min_size = min_size
        self._max_size = max_size
        self._cooldown_initial = (
            self.COOLDOWN_INITIAL
            if cooldown_initial is None else cooldown_initial)
        self._cooldown_max = (
            self.COOLDOWN_MAX if cooldown_max is None else cooldown_max)
        self._cooldown_jitter = (
            self.COOLDOWN_JITTER
            if cooldown_jitter is None else cooldown_jitter)
        # FIFO (pop(0)) so idle connections rotate (oldest reused first),
        # which prevents the long-tail of a never-touched connection
        # silently rotting on NAT/idle timeouts in long-lived workers.
        self._idle = []      # list[psycopg.Connection]
        self._busy = set()   # set[psycopg.Connection]
        # Monotonic timestamp of the moment each idle conn entered the
        # idle list — used by prune_idle() to drop long-idle conns above
        # min_size. Keyed by conn object (hashable).
        self._idle_since = {}
        self._closed = False
        # Circuit breaker state. _next_retry_at is the monotonic time at
        # which a new connect attempt is allowed; None means breaker is
        # closed (normal operation).
        self._consecutive_failures = 0
        self._next_retry_at = None
        self._last_unavailable_log_at = None

    @property
    def size(self):
        """Total open connections (idle + busy)."""
        return len(self._idle) + len(self._busy)

    @property
    def idle_count(self):
        return len(self._idle)

    @property
    def busy_count(self):
        return len(self._busy)

    def open(self):
        """Pre-open connections up to ``min_size``.

        Safe to call multiple times. On connect failure raises
        ``PoolUnavailable`` and arms the circuit breaker; subsequent
        calls during cooldown re-raise without contacting the DB.
        """
        if self._closed:
            raise PoolClosed('pool is closed')
        while self.size < self._min_size:
            self._add_idle(self._try_make_conn())

    def _make_conn(self):
        conn = _psycopg.connect(self._conninfo)
        _db_connection.configure_nonblocking(conn)
        return conn

    def _try_make_conn(self):
        """Wrap ``_make_conn`` with circuit-breaker bookkeeping.

        - If breaker is open and cooldown has not elapsed, raises
          ``PoolUnavailable`` immediately without touching the network.
          Logs a rate-limited "still unavailable" warning.
        - Otherwise attempts a real connect. On failure: records the
          failure, schedules the next retry with exponential backoff
          (capped at ``cooldown_max``) and ±jitter, raises
          ``PoolUnavailable``. The first failure logs a warning; further
          failures only update the rate-limited "still unavailable"
          line.
        - On success: clears all breaker state and (if it was previously
          open) logs a recovery info line.
        """
        now = _time.monotonic()
        if self._next_retry_at is not None and now < self._next_retry_at:
            self._maybe_log_still_unavailable(now)
            raise PoolUnavailable(
                f'db pool unavailable '
                f'({self._consecutive_failures} consecutive failures, '
                f'retry in {self._next_retry_at - now:.1f}s)')
        try:
            conn = self._make_conn()
        except Exception as exc:
            self._consecutive_failures += 1
            base = min(
                self._cooldown_max,
                self._cooldown_initial
                * (2 ** (self._consecutive_failures - 1)))
            jitter = base * self._cooldown_jitter
            cooldown = base + _random.uniform(-jitter, jitter)
            self._next_retry_at = now + cooldown
            if self._consecutive_failures == 1:
                # First failure in a streak — log loudly so ops notice.
                _log.warning(
                    'db pool connect failed: %s; '
                    'circuit breaker open, retry in %.1fs', exc, cooldown)
                self._last_unavailable_log_at = now
            raise PoolUnavailable(str(exc)) from exc
        # Success: reset breaker.
        if self._consecutive_failures > 0:
            _log.info(
                'db pool reconnected after %d failed attempts',
                self._consecutive_failures)
        self._consecutive_failures = 0
        self._next_retry_at = None
        self._last_unavailable_log_at = None
        return conn

    def _maybe_log_still_unavailable(self, now):
        """Emit one 'still unavailable' line per UNAVAILABLE_LOG_INTERVAL."""
        last = self._last_unavailable_log_at
        if last is None or now - last >= self.UNAVAILABLE_LOG_INTERVAL:
            _log.warning(
                'db pool still unavailable after %d failed attempts',
                self._consecutive_failures)
            self._last_unavailable_log_at = now

    def acquire(self):
        """Return a non-blocking connection from the pool.

        Raises:
            PoolClosed: if the pool has been closed.
            PoolTimeout: if no idle connection is available and the pool
                is at ``max_size``. Never blocks.
            PoolUnavailable: if the circuit breaker is open after recent
                connect failures. Caller should respond 503 immediately
                and not retry until cooldown elapses.
        """
        if self._closed:
            raise PoolClosed('pool is closed')
        if self._idle:
            conn = self._idle.pop(0)
            self._idle_since.pop(conn, None)
            self._busy.add(conn)
            return conn
        if self.size < self._max_size:
            conn = self._try_make_conn()
            self._busy.add(conn)
            return conn
        raise PoolTimeout('no free connection')

    def status(self):
        """Snapshot of pool state for healthchecks / metrics / pause-resume.

        Returns a dict with:
            min_size, max_size: configured limits.
            size: total open conns (idle + busy).
            idle, busy: counts.
            free: how many further ``acquire()`` calls would currently
                succeed without raising ``PoolTimeout``
                (= ``max_size - busy``). Workers use this for pause /
                resume decisions.
            closed: True after ``close()``.
            available: False while the circuit breaker is open
                (no new connects allowed yet).
            consecutive_connect_failures: streak length, 0 when healthy.
            retry_in: seconds until breaker allows next probe, or 0 if
                already allowed / breaker closed.
        """
        now = _time.monotonic()
        retry_in = 0.0
        if self._next_retry_at is not None:
            retry_in = max(0.0, self._next_retry_at - now)
        return {
            'min_size': self._min_size,
            'max_size': self._max_size,
            'size': self.size,
            'idle': self.idle_count,
            'busy': self.busy_count,
            'free': self._max_size - self.busy_count,
            'closed': self._closed,
            'available': retry_in == 0.0,
            'consecutive_connect_failures': self._consecutive_failures,
            'retry_in': retry_in,
        }

    def release(self, conn, broken=False):
        """Return a connection to the pool.

        Args:
            conn: connection previously returned by ``acquire()``.
            broken: if True, the connection is closed instead of being
                returned to the idle list. Use after any exception during
                query execution.

        Connections in a non-IDLE transaction state (incomplete query,
        in-transaction, error) are also dropped automatically. After
        any drop, the pool is refilled back up to ``min_size`` so the
        warm baseline is maintained without background threads.

        Raises:
            ValueError: if ``conn`` was not currently checked out from
                this pool (alien connection or double-release).
        """
        if conn not in self._busy:
            raise ValueError(
                'release(): connection not checked out from this pool '
                '(alien conn or double-release)')
        self._busy.remove(conn)
        if broken or self._closed:
            self._close_conn(conn)
            self._refill()
            return
        try:
            tx_status = conn.pgconn.transaction_status
        except Exception:
            self._close_conn(conn)
            self._refill()
            return
        if tx_status != _psycopg_pq.TransactionStatus.IDLE:
            self._close_conn(conn)
            self._refill()
            return
        self._add_idle(conn)

    def _add_idle(self, conn):
        """Append conn to idle list with current timestamp."""
        self._idle.append(conn)
        self._idle_since[conn] = _time.monotonic()

    def _refill(self):
        """Top up idle pool back to ``min_size`` after a drop.

        Connect failures are swallowed silently — the next ``acquire()``
        will retry lazily. We must not raise from ``release()``.
        """
        if self._closed:
            return
        while self.size < self._min_size:
            try:
                self._add_idle(self._try_make_conn())
            except Exception:
                break

    def prune_idle(self, ttl):
        """Drop idle conns older than ``ttl`` seconds, down to min_size.

        Intended to be called periodically from the worker event loop
        (e.g. ``uhttp.workers.Worker.on_idle``). Connections at the front
        of the FIFO idle list are the oldest and are checked first; the
        loop stops as soon as the oldest is younger than ``ttl`` (since
        all subsequent ones are even younger).

        Args:
            ttl: minimum idle age in seconds before a conn is eligible
                for pruning.

        Returns:
            Number of connections closed.
        """
        if self._closed:
            return 0
        now = _time.monotonic()
        closed = 0
        while (self._idle
                and self.size > self._min_size
                and now - self._idle_since.get(
                    self._idle[0], now) >= ttl):
            conn = self._idle.pop(0)
            self._idle_since.pop(conn, None)
            self._close_conn(conn)
            closed += 1
        return closed

    def _close_conn(self, conn):
        try:
            conn.close()
        except Exception:
            pass

    def cancel_busy(self):
        """Send PG cancel request to all currently busy connections.

        Intended for shutdown: tells the server to abort in-flight queries
        so the worker can exit quickly without waiting for slow SELECTs.

        WARNING: ``PGcancel`` is a **blocking** libpq call (opens a fresh
        TCP connection to the server and waits synchronously for the
        cancel ack). Only call from shutdown code, never from inside the
        event loop, or you will freeze it.
        """
        for conn in list(self._busy):
            try:
                conn.pgconn.cancel()
            except Exception:
                pass

    def close(self):
        """Close all connections (idle and busy) without waiting.

        Subsequent ``acquire()`` calls raise ``PoolClosed``.

        Graceful shutdown sequence:
            1. Stop accepting new work (mark worker draining).
            2. ``pool.cancel_busy()`` — server-side abort of in-flight
               queries so they error out fast.
            3. Let the event loop run one more pass so each in-flight
               ``AsyncQuery.on_readable()`` observes the error and the
               caller releases its connection (with ``broken=True``).
            4. ``pool.close()``.

        Calling ``close()`` while ``AsyncQuery`` instances still hold
        connections will close those sockets out from under them; the
        next ``consume_input()`` call will raise. That is acceptable for
        a hard shutdown but not for graceful drain — see steps above.
        """
        self._closed = True
        for conn in self._idle:
            self._close_conn(conn)
        for conn in list(self._busy):
            self._close_conn(conn)
        self._idle.clear()
        self._idle_since.clear()
        self._busy.clear()


class AsyncQuery:
    """Non-blocking execution state for one dbentity query on a psycopg3 conn.

    The connection must already be in non-blocking mode
    (`conn.pgconn.nonblocking = 1`); use `db_connection.configure_nonblocking`
    as a `psycopg_pool.ConnectionPool` configure callback.
    """

    def __init__(self, conn, query):
        if not conn.pgconn.nonblocking:
            raise AsyncQueryError(
                'connection must be in non-blocking mode')
        self._conn = conn
        self._pgconn = conn.pgconn
        self._query = query
        self._tx = _psycopg_adapt.Transformer(conn)
        self._pgres = None
        self._needs_flush = False
        self._done = False

    def fileno(self):
        """Return underlying socket fd for select/poll registration."""
        return self._pgconn.socket

    def start(self):
        """Send query and perform initial flush.

        After calling start(), the caller MUST check needs_write() and
        register the fd as writable in the event loop if it returns True;
        the initial flush may not drain the output buffer in one call.
        """
        args = self._query.args
        params = self._tx.dump_sequence(
            args, [_psycopg_adapt.PyFormat.AUTO] * len(args))
        self._pgconn.send_query_params(
            self._query.pg_query_bytes,
            params,
            param_types=self._tx.types,
            param_formats=self._tx.formats,
            result_format=_psycopg_pq.Format.BINARY)
        self._flush()

    def _flush(self):
        self._needs_flush = (self._pgconn.flush() != 0)

    def needs_write(self):
        """True if pending output buffer requires socket-writable wait."""
        return self._needs_flush

    def on_writable(self):
        """Call from event loop when fd is writable; retries flush."""
        self._flush()

    def on_readable(self):
        """Call from event loop when fd is readable.

        Returns True when the query is complete (call result() next),
        False if more reads are needed. Safe to call after completion;
        in that case it is a no-op and returns True.
        """
        if self._done:
            return True
        self._pgconn.consume_input()
        while not self._pgconn.is_busy():
            r = self._pgconn.get_result()
            if r is None:
                self._done = True
                return True
            if r.error_message:
                raise AsyncQueryError(r.error_message.decode())
            self._pgres = r
        return False

    def done(self):
        return self._done

    def result(self):
        """Return query result in the same shape as the blocking equivalent.

        For Select / Distinct / CountBy: list of entity objects (or tuples
        for Distinct/CountBy) via `query.create_objects(rows)` if available,
        otherwise raw row tuples.

        For Delete / Count / Insert / Update: returns raw rows from the last
        result (caller decides what to do with them — e.g. RETURNING id).
        """
        if not self._done:
            raise AsyncQueryError('AsyncQuery not finished')
        if self._pgres is None or self._pgres.ntuples == 0:
            return []
        self._tx.set_pgresult(self._pgres)
        rows = [
            self._tx.load_row(i, tuple)
            for i in range(self._pgres.ntuples)]
        if hasattr(self._query, 'create_objects'):
            return self._query.create_objects(rows)
        return rows
