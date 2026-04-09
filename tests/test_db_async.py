import time as _time
import unittest
from unittest.mock import MagicMock, patch

import psycopg.pq as _psycopg_pq

from dbentity.db_async import (
    AsyncConnectionPool,
    AsyncQuery,
    AsyncQueryError,
    PoolClosed,
    PoolError,
    PoolTimeout,
    PoolUnavailable,
)
from dbentity.db_connection import configure_nonblocking


class TestAsyncQueryValidation(unittest.TestCase):
    def test_rejects_blocking_connection(self):
        conn = MagicMock()
        conn.pgconn.nonblocking = 0
        with self.assertRaises(AsyncQueryError):
            AsyncQuery(conn, MagicMock())

    def test_accepts_nonblocking_connection(self):
        conn = MagicMock()
        conn.pgconn.nonblocking = 1
        # Should not raise
        AsyncQuery(conn, MagicMock())

    def test_result_before_done_raises(self):
        conn = MagicMock()
        conn.pgconn.nonblocking = 1
        aq = AsyncQuery(conn, MagicMock())
        with self.assertRaises(AsyncQueryError):
            aq.result()

    def test_done_initially_false(self):
        conn = MagicMock()
        conn.pgconn.nonblocking = 1
        aq = AsyncQuery(conn, MagicMock())
        self.assertFalse(aq.done())


class TestConfigureNonblocking(unittest.TestCase):
    def test_sets_autocommit_and_nonblocking(self):
        conn = MagicMock()
        configure_nonblocking(conn)
        self.assertTrue(conn.autocommit)
        self.assertEqual(conn.pgconn.nonblocking, 1)


def _make_fake_conn():
    """Build a mock psycopg connection that looks IDLE on release."""
    conn = MagicMock()
    conn.pgconn.transaction_status = _psycopg_pq.TransactionStatus.IDLE
    return conn


class TestAsyncConnectionPool(unittest.TestCase):
    def test_invalid_sizes(self):
        with self.assertRaises(ValueError):
            AsyncConnectionPool('x', min_size=-1, max_size=5)
        with self.assertRaises(ValueError):
            AsyncConnectionPool('x', min_size=5, max_size=2)
        with self.assertRaises(ValueError):
            AsyncConnectionPool('x', min_size=0, max_size=0)

    def test_min_size_zero_lazy_pool(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn) as mk:
            pool = AsyncConnectionPool('x', min_size=0, max_size=3)
            pool.open()
            self.assertEqual(mk.call_count, 0)
            self.assertEqual(pool.size, 0)
            # Lazy creation on first acquire
            pool.acquire()
            self.assertEqual(mk.call_count, 1)

    def test_open_preopens_min_size(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn) as mk:
            pool = AsyncConnectionPool('x', min_size=3, max_size=5)
            pool.open()
            self.assertEqual(mk.call_count, 3)
            self.assertEqual(pool.idle_count, 3)
            self.assertEqual(pool.busy_count, 0)
            self.assertEqual(pool.size, 3)

    def test_acquire_returns_idle(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn):
            pool = AsyncConnectionPool('x', min_size=2, max_size=5)
            pool.open()
            c1 = pool.acquire()
            self.assertEqual(pool.busy_count, 1)
            self.assertEqual(pool.idle_count, 1)
            c2 = pool.acquire()
            self.assertEqual(pool.busy_count, 2)
            self.assertEqual(pool.idle_count, 0)
            self.assertIsNot(c1, c2)

    def test_acquire_grows_up_to_max(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn) as mk:
            pool = AsyncConnectionPool('x', min_size=1, max_size=3)
            pool.open()
            self.assertEqual(mk.call_count, 1)
            pool.acquire()
            pool.acquire()
            pool.acquire()
            self.assertEqual(pool.size, 3)
            self.assertEqual(mk.call_count, 3)

    def test_acquire_raises_pool_timeout_when_full(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn):
            pool = AsyncConnectionPool('x', min_size=1, max_size=2)
            pool.open()
            pool.acquire()
            pool.acquire()
            with self.assertRaises(PoolTimeout):
                pool.acquire()

    def test_release_returns_to_idle(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn):
            pool = AsyncConnectionPool('x', min_size=1, max_size=3)
            pool.open()
            conn = pool.acquire()
            self.assertEqual(pool.busy_count, 1)
            pool.release(conn)
            self.assertEqual(pool.busy_count, 0)
            self.assertEqual(pool.idle_count, 1)
            # Next acquire returns the same conn (only one in pool)
            self.assertIs(pool.acquire(), conn)

    def test_idle_rotation_is_fifo(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn):
            pool = AsyncConnectionPool('x', min_size=3, max_size=5)
            pool.open()
            # Snapshot of pre-opened conns in their idle order
            initial = list(pool._idle)
            # FIFO: acquire returns oldest first
            self.assertIs(pool.acquire(), initial[0])
            self.assertIs(pool.acquire(), initial[1])
            self.assertIs(pool.acquire(), initial[2])

    def test_release_broken_closes_and_refills(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn):
            pool = AsyncConnectionPool('x', min_size=2, max_size=5)
            pool.open()
            conn = pool.acquire()
            pool.release(conn, broken=True)
            conn.close.assert_called_once()
            self.assertEqual(pool.busy_count, 0)
            # Refilled back to min_size
            self.assertEqual(pool.idle_count, 2)
            self.assertEqual(pool.size, 2)

    def test_release_broken_no_refill_when_above_min(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn) as mk:
            pool = AsyncConnectionPool('x', min_size=1, max_size=5)
            pool.open()
            self.assertEqual(mk.call_count, 1)
            # Acquire 3 (1 from idle + 2 grown)
            cs = [pool.acquire() for _ in range(3)]
            self.assertEqual(mk.call_count, 3)
            pool.release(cs[0], broken=True)
            # size dropped from 3 → 2, still above min_size=1, no refill
            self.assertEqual(mk.call_count, 3)
            self.assertEqual(pool.size, 2)

    def test_release_alien_conn_raises(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn):
            pool = AsyncConnectionPool('x', min_size=1, max_size=3)
            pool.open()
            with self.assertRaises(ValueError):
                pool.release(_make_fake_conn())

    def test_release_double_release_raises(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn):
            pool = AsyncConnectionPool('x', min_size=1, max_size=3)
            pool.open()
            conn = pool.acquire()
            pool.release(conn)
            with self.assertRaises(ValueError):
                pool.release(conn)

    def test_release_drops_non_idle_conn(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn):
            pool = AsyncConnectionPool('x', min_size=2, max_size=5)
            pool.open()
            conn = pool.acquire()
            conn.pgconn.transaction_status = (
                _psycopg_pq.TransactionStatus.INERROR)
            pool.release(conn)
            conn.close.assert_called_once()
            # Refill maintains min_size
            self.assertEqual(pool.idle_count, 2)

    def test_release_active_conn_dropped(self):
        """A conn still mid-query (ACTIVE) on release must be closed."""
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn):
            pool = AsyncConnectionPool('x', min_size=1, max_size=3)
            pool.open()
            conn = pool.acquire()
            conn.pgconn.transaction_status = (
                _psycopg_pq.TransactionStatus.ACTIVE)
            pool.release(conn)
            conn.close.assert_called_once()
            self.assertNotIn(conn, pool._idle)

    def test_refill_swallows_connect_failure(self):
        """release() must never raise even if refill connect fails."""
        calls = {'n': 0}

        def flaky(*_a, **_k):
            calls['n'] += 1
            # First call (open()) succeeds, refill call fails
            if calls['n'] == 1:
                return _make_fake_conn()
            raise RuntimeError('db down')

        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=flaky):
            pool = AsyncConnectionPool('x', min_size=1, max_size=3)
            pool.open()
            conn = pool.acquire()
            # Must not raise even though refill will fail
            pool.release(conn, broken=True)
            self.assertEqual(pool.size, 0)

    def test_acquire_after_close_raises_pool_closed(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn):
            pool = AsyncConnectionPool('x', min_size=1, max_size=3)
            pool.open()
            pool.close()
            with self.assertRaises(PoolClosed):
                pool.acquire()

    def test_open_after_close_raises_pool_closed(self):
        pool = AsyncConnectionPool('x', min_size=1, max_size=3)
        pool.close()
        with self.assertRaises(PoolClosed):
            pool.open()

    def test_close_closes_all_conns(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn):
            pool = AsyncConnectionPool('x', min_size=2, max_size=5)
            pool.open()
            busy = pool.acquire()
            pool.close()
            busy.close.assert_called_once()
            self.assertEqual(pool.size, 0)

    def test_cancel_busy_calls_pgconn_cancel(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn):
            pool = AsyncConnectionPool('x', min_size=2, max_size=5)
            pool.open()
            c1 = pool.acquire()
            c2 = pool.acquire()
            pool.cancel_busy()
            c1.pgconn.cancel.assert_called_once()
            c2.pgconn.cancel.assert_called_once()

    def test_prune_idle_drops_stale_above_min(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn):
            pool = AsyncConnectionPool('x', min_size=2, max_size=5)
            pool.open()
            # Force pool to grow to max by acquiring all 5 then releasing
            all_conns = [pool.acquire() for _ in range(5)]
            for c in all_conns:
                pool.release(c)
            self.assertEqual(pool.size, 5)
            self.assertEqual(pool.idle_count, 5)
            for conn in pool._idle:
                pool._idle_since[conn] -= 1000
            self.assertEqual(pool.prune_idle(ttl=300), 3)
            self.assertEqual(pool.size, 2)

    def test_prune_idle_keeps_min_size(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn):
            pool = AsyncConnectionPool('x', min_size=3, max_size=5)
            pool.open()
            for conn in pool._idle:
                pool._idle_since[conn] -= 1000
            self.assertEqual(pool.prune_idle(ttl=300), 0)
            self.assertEqual(pool.size, 3)

    def test_prune_idle_skips_fresh(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn):
            pool = AsyncConnectionPool('x', min_size=1, max_size=5)
            pool.open()
            # Force grow above min so prune is theoretically possible
            all_conns = [pool.acquire() for _ in range(2)]
            for c in all_conns:
                pool.release(c)
            self.assertEqual(pool.size, 2)
            self.assertEqual(pool.prune_idle(ttl=300), 0)
            self.assertEqual(pool.size, 2)

    def test_prune_idle_stops_at_first_fresh(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn):
            pool = AsyncConnectionPool('x', min_size=1, max_size=5)
            pool.open()
            all_conns = [pool.acquire() for _ in range(3)]
            for c in all_conns:
                pool.release(c)
            self.assertEqual(pool.size, 3)
            # Backdate only the OLDEST one (front of FIFO)
            pool._idle_since[pool._idle[0]] -= 1000
            self.assertEqual(pool.prune_idle(ttl=300), 1)
            self.assertEqual(pool.size, 2)

    def test_prune_idle_after_close(self):
        pool = AsyncConnectionPool('x', min_size=1, max_size=5)
        pool.close()
        self.assertEqual(pool.prune_idle(ttl=10), 0)


class TestAsyncConnectionPoolCircuitBreaker(unittest.TestCase):
    """Tests for connect-failure circuit breaker (variant A: backoff)."""

    def _pool(self, **kwargs):
        # Disable jitter so tests are deterministic.
        kwargs.setdefault('cooldown_initial', 1.0)
        kwargs.setdefault('cooldown_max', 8.0)
        kwargs.setdefault('cooldown_jitter', 0.0)
        return AsyncConnectionPool(
            'x', min_size=0, max_size=3, **kwargs)

    def test_pool_error_is_base(self):
        self.assertTrue(issubclass(PoolClosed, PoolError))
        self.assertTrue(issubclass(PoolTimeout, PoolError))
        self.assertTrue(issubclass(PoolUnavailable, PoolError))

    def test_first_failure_opens_breaker(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=RuntimeError('db down')):
            pool = self._pool()
            with self.assertRaises(PoolUnavailable):
                pool.acquire()
            self.assertEqual(pool._consecutive_failures, 1)
            self.assertIsNotNone(pool._next_retry_at)

    def test_breaker_blocks_during_cooldown(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=RuntimeError('db down')) as mk:
            pool = self._pool()
            with self.assertRaises(PoolUnavailable):
                pool.acquire()
            self.assertEqual(mk.call_count, 1)
            # Subsequent acquires during cooldown must NOT touch _make_conn.
            for _ in range(5):
                with self.assertRaises(PoolUnavailable):
                    pool.acquire()
            self.assertEqual(mk.call_count, 1)

    def test_breaker_allows_retry_after_cooldown(self):
        attempts = {'n': 0}

        def flaky(*_a, **_k):
            attempts['n'] += 1
            if attempts['n'] == 1:
                raise RuntimeError('first fails')
            return _make_fake_conn()

        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=flaky):
            pool = self._pool()
            with self.assertRaises(PoolUnavailable):
                pool.acquire()
            # Force the cooldown to elapse by rewinding _next_retry_at.
            pool._next_retry_at = _time.monotonic() - 0.001
            conn = pool.acquire()
            self.assertIsNotNone(conn)
            # Breaker is reset.
            self.assertEqual(pool._consecutive_failures, 0)
            self.assertIsNone(pool._next_retry_at)

    def test_exponential_backoff_capped(self):
        cooldowns = []
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=RuntimeError('down')):
            pool = self._pool()
            for _ in range(6):
                t_before = _time.monotonic()
                with self.assertRaises(PoolUnavailable):
                    pool.acquire()
                cooldowns.append(pool._next_retry_at - t_before)
                # Force cooldown to elapse so the next acquire actually
                # tries again (and bumps the counter).
                pool._next_retry_at = _time.monotonic() - 0.001
            # Cooldowns: 1, 2, 4, 8, 8, 8 (jitter=0).
            self.assertAlmostEqual(cooldowns[0], 1.0, places=1)
            self.assertAlmostEqual(cooldowns[1], 2.0, places=1)
            self.assertAlmostEqual(cooldowns[2], 4.0, places=1)
            self.assertAlmostEqual(cooldowns[3], 8.0, places=1)
            self.assertAlmostEqual(cooldowns[4], 8.0, places=1)
            self.assertAlmostEqual(cooldowns[5], 8.0, places=1)

    def test_jitter_applied(self):
        # With jitter > 0, repeated cooldowns from the same failure count
        # should not all be exactly equal.
        observed = set()
        for _ in range(20):
            with patch(
                    'dbentity.db_async.AsyncConnectionPool._make_conn',
                    side_effect=RuntimeError('down')):
                pool = AsyncConnectionPool(
                    'x', min_size=0, max_size=3,
                    cooldown_initial=1.0, cooldown_max=8.0,
                    cooldown_jitter=0.5)
                t_before = _time.monotonic()
                with self.assertRaises(PoolUnavailable):
                    pool.acquire()
                observed.add(round(pool._next_retry_at - t_before, 4))
        # Extremely unlikely to collide on a single value across 20 runs
        # with ±50 % jitter.
        self.assertGreater(len(observed), 1)

    def test_status_reports_breaker(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=RuntimeError('down')):
            pool = self._pool()
            s = pool.status()
            self.assertTrue(s['available'])
            self.assertEqual(s['consecutive_connect_failures'], 0)
            self.assertEqual(s['retry_in'], 0.0)
            with self.assertRaises(PoolUnavailable):
                pool.acquire()
            s = pool.status()
            self.assertFalse(s['available'])
            self.assertEqual(s['consecutive_connect_failures'], 1)
            self.assertGreater(s['retry_in'], 0.0)

    def test_status_free_capacity(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=_make_fake_conn):
            pool = AsyncConnectionPool('x', min_size=1, max_size=3)
            pool.open()
            self.assertEqual(pool.status()['free'], 3)
            pool.acquire()
            self.assertEqual(pool.status()['free'], 2)
            pool.acquire()
            pool.acquire()
            self.assertEqual(pool.status()['free'], 0)

    def test_unavailable_log_rate_limited(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=RuntimeError('down')):
            pool = self._pool()
            with patch('dbentity.db_async._log') as log:
                # First failure: one warning.
                with self.assertRaises(PoolUnavailable):
                    pool.acquire()
                self.assertEqual(log.warning.call_count, 1)
                # Many subsequent calls during cooldown: no extra logs.
                for _ in range(10):
                    with self.assertRaises(PoolUnavailable):
                        pool.acquire()
                self.assertEqual(log.warning.call_count, 1)
                # Force log interval to elapse.
                pool._last_unavailable_log_at = (
                    _time.monotonic() - pool.UNAVAILABLE_LOG_INTERVAL - 1)
                with self.assertRaises(PoolUnavailable):
                    pool.acquire()
                self.assertEqual(log.warning.call_count, 2)

    def test_recovery_logs_info(self):
        attempts = {'n': 0}

        def flaky(*_a, **_k):
            attempts['n'] += 1
            if attempts['n'] == 1:
                raise RuntimeError('first fails')
            return _make_fake_conn()

        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=flaky):
            pool = self._pool()
            with patch('dbentity.db_async._log') as log:
                with self.assertRaises(PoolUnavailable):
                    pool.acquire()
                pool._next_retry_at = _time.monotonic() - 0.001
                pool.acquire()
                log.info.assert_called_once()

    def test_open_propagates_pool_unavailable(self):
        with patch(
                'dbentity.db_async.AsyncConnectionPool._make_conn',
                side_effect=RuntimeError('down')):
            pool = AsyncConnectionPool('x', min_size=2, max_size=3)
            with self.assertRaises(PoolUnavailable):
                pool.open()
