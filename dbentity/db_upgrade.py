"""Database schema migration utilities."""

import os as _os


DB_VERSION_TABLE = 'db_version'
CHECK_TABLE_EXISTS = 'SELECT table_name FROM information_schema.tables WHERE table_name=%s;'
GET_DB_VERSION = 'SELECT version FROM db_version;'
SET_DB_VERSION = 'UPDATE db_version SET version=%s;'


def _has_table(db, table_name):
    cur = db.execute(CHECK_TABLE_EXISTS, (table_name, ))
    return bool(cur.fetchone())


def _get_db_version(db):
    if _has_table(db, DB_VERSION_TABLE):
        cur = db.execute(GET_DB_VERSION)
        row = cur.fetchone()
        if row:
            return row[0]
    return None


def _execute_sql_file(cur, sql_file_name):
    with open(sql_file_name, 'r', encoding='utf-8') as sql_file:
        cur.execute(sql_file.read())


def db_upgrade(db, log, sql_path, sql_init_file, sql_upgrade_files):
    """Run database migrations.

    Args:
        db: Database connection with execute(), cursor(), commit(), transaction().
        log: Logger with info() method.
        sql_path: Directory containing SQL files.
        sql_init_file: Initial schema file (run if db_version table missing).
        sql_upgrade_files: List of (version, filename) tuples for incremental upgrades.
    """
    db_version = _get_db_version(db)
    if db_version is None:
        log.info("Creating all tables")
        with db.cursor() as cur:
            _execute_sql_file(cur, _os.path.join(sql_path, sql_init_file))
            db.commit()
            return
    log.info("DB_VERSION: %d", db_version)
    with db.transaction():
        upgraded = False
        for db_ver, upgrade_file in sql_upgrade_files:
            if db_ver <= db_version:
                continue
            log.info("Upgrading DB with file: '%s' to version: %d", upgrade_file, db_ver)
            with db.cursor() as cur:
                _execute_sql_file(cur, _os.path.join(sql_path, upgrade_file))
                cur.execute(SET_DB_VERSION, (db_ver,))
            upgraded = True
        if upgraded:
            log.info("Upgrade successful")
