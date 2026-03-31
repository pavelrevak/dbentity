import unittest
import os
import tempfile

from dbentity.db_upgrade import (
    _has_table,
    _get_db_version,
    _execute_sql_file,
    db_upgrade,
    DB_VERSION_TABLE,
)


class MockCursor:
    def __init__(self, result=None):
        self._result = result
        self.executed = []

    def execute(self, query, args=None):
        self.executed.append((query, args))

    def fetchone(self):
        return self._result

    def fetchall(self):
        if self._result is None:
            return []
        return [self._result]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class MockDb:
    def __init__(self, has_table=False, version=None):
        self._has_table = has_table
        self._version = version
        self.commits = 0
        self.executed = []
        self._cursor = None

    def execute(self, query, args=None):
        self.executed.append((query, args))
        if 'table_name' in query:
            return MockCursor(('db_version',) if self._has_table else None)
        if 'version FROM' in query:
            return MockCursor((self._version,) if self._version else None)
        return MockCursor()

    def cursor(self):
        self._cursor = MockCursor()
        return self._cursor

    def commit(self):
        self.commits += 1

    def transaction(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class MockLog:
    def __init__(self):
        self.messages = []

    def info(self, msg, *args):
        self.messages.append(msg % args if args else msg)


class TestHasTable(unittest.TestCase):
    def test_has_table_true(self):
        db = MockDb(has_table=True)
        result = _has_table(db, 'db_version')
        self.assertTrue(result)

    def test_has_table_false(self):
        db = MockDb(has_table=False)
        result = _has_table(db, 'nonexistent')
        self.assertFalse(result)


class TestGetDbVersion(unittest.TestCase):
    def test_get_version_no_table(self):
        db = MockDb(has_table=False)
        result = _get_db_version(db)
        self.assertIsNone(result)

    def test_get_version_with_table(self):
        db = MockDb(has_table=True, version=5)
        result = _get_db_version(db)
        self.assertEqual(result, 5)

    def test_get_version_empty_table(self):
        db = MockDb(has_table=True, version=None)
        result = _get_db_version(db)
        self.assertIsNone(result)


class TestExecuteSqlFile(unittest.TestCase):
    def test_execute_sql_file(self):
        f = tempfile.NamedTemporaryFile(
            mode='w', suffix='.sql', delete=False)
        try:
            f.write('CREATE TABLE test (id INT);')
            f.close()
            cursor = MockCursor()
            _execute_sql_file(cursor, f.name)
            self.assertEqual(len(cursor.executed), 1)
            self.assertIn('CREATE TABLE', cursor.executed[0][0])
        finally:
            os.unlink(f.name)


class TestDbUpgrade(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.init_file = 'init.sql'
        with open(os.path.join(self.temp_dir, self.init_file), 'w') as f:
            f.write('CREATE TABLE users (id INT);')

    def tearDown(self):
        for f in os.listdir(self.temp_dir):
            os.unlink(os.path.join(self.temp_dir, f))
        os.rmdir(self.temp_dir)

    def test_db_upgrade_fresh_install(self):
        db = MockDb(has_table=False)
        log = MockLog()
        db_upgrade(db, log, self.temp_dir, self.init_file, [])
        self.assertIn('Creating all tables', log.messages)
        self.assertEqual(db.commits, 1)

    def test_db_upgrade_no_upgrades_needed(self):
        db = MockDb(has_table=True, version=5)
        log = MockLog()
        upgrade_files = [(3, 'v3.sql'), (4, 'v4.sql'), (5, 'v5.sql')]
        db_upgrade(db, log, self.temp_dir, self.init_file, upgrade_files)
        self.assertIn('DB_VERSION: 5', log.messages)
        self.assertNotIn('Upgrading', ' '.join(log.messages))

    def test_db_upgrade_applies_upgrades(self):
        upgrade_file = 'v2.sql'
        with open(os.path.join(self.temp_dir, upgrade_file), 'w') as f:
            f.write('ALTER TABLE users ADD name VARCHAR;')

        db = MockDb(has_table=True, version=1)
        log = MockLog()
        upgrade_files = [(2, upgrade_file)]
        db_upgrade(db, log, self.temp_dir, self.init_file, upgrade_files)
        self.assertTrue(
            any('Upgrading' in msg for msg in log.messages))
        self.assertTrue(
            any('Upgrade successful' in msg for msg in log.messages))

    def test_db_upgrade_skips_old_versions(self):
        v2_file = 'v2.sql'
        v3_file = 'v3.sql'
        with open(os.path.join(self.temp_dir, v2_file), 'w') as f:
            f.write('-- v2')
        with open(os.path.join(self.temp_dir, v3_file), 'w') as f:
            f.write('-- v3')

        db = MockDb(has_table=True, version=2)
        log = MockLog()
        upgrade_files = [(2, v2_file), (3, v3_file)]
        db_upgrade(db, log, self.temp_dir, self.init_file, upgrade_files)
        upgrade_msgs = [m for m in log.messages if 'Upgrading' in m]
        self.assertEqual(len(upgrade_msgs), 1)
        self.assertIn('v3.sql', upgrade_msgs[0])


if __name__ == '__main__':
    unittest.main()
