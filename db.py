import sqlite3
import os.path

dbpath = os.path.join(os.environ.get('HOME', '.'), '.fts.db')
db = sqlite3.connect(dbpath)

db.execute("""
    CREATE TABLE IF NOT EXISTS
    config (
        key INTEGER PRIMARY KEY,
        value
    );
""")

db.execute("""
    CREATE TABLE IF NOT EXISTS
    databases (
        id INTEGER PRIMARY KEY,
        name
    );
""")
db.execute("CREATE INDEX IF NOT EXISTS databases_name_idx ON databases(name);")

db.execute("""
    CREATE TABLE IF NOT EXISTS
    dbconfig (
        dbid,
        key,
        value,
        PRIMARY KEY (dbid, key)
    );
""")

# docid references the files_fts
db.execute("""
    CREATE TABLE IF NOT EXISTS
    files (
        docid INTEGER PRIMARY KEY,
        dbid,
        path,
        last_modified INTEGER
    );
""")
db.execute("CREATE INDEX IF NOT EXISTS files_dbid_path_idx ON files(dbid, path);")

db.execute("""
    CREATE VIRTUAL TABLE
    files_fts USING fts4 (
        body TEXT);
""")