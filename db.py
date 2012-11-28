import sqlite3
# import os.path

dbpath = 'fts.db' # os.path.join(os.environ.get('HOME', '.'), '.fts.db')
conn = sqlite3.connect(dbpath)

def createschema(c):
    c.execute("""
        CREATE TABLE IF NOT EXISTS
        config (
            key TEXT PRIMARY KEY,
            value
        );
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS
        databases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            UNIQUE(name)
        );
    """)
    c.execute("CREATE INDEX IF NOT EXISTS databases_name_idx ON databases(name);")

    c.execute("""
        CREATE TABLE IF NOT EXISTS
        dbconfig (
            dbid,
            key TEXT,
            value,
            PRIMARY KEY (dbid, key)
            --FOREIGN KEY dbid REFERENCES databases(id)
        );
    """)

    # docid references the files_fts
    c.execute("""
        CREATE TABLE IF NOT EXISTS
        files (
            docid INTEGER PRIMARY KEY AUTOINCREMENT,
            dbid INTEGER,
            path,
            last_modified INTEGER
            --FOREIGN KEY dbid REFERENCES databases(id)
        );
    """)
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS files_dbid_path_idx ON files(dbid, path);")

    if not c.execute("SELECT DISTINCT tbl_name FROM sqlite_master WHERE tbl_name = 'files_fts'").fetchall():
        # has an invisible docid column
        c.execute("""
            CREATE VIRTUAL TABLE
            files_fts USING fts4 (
                body TEXT);
        """)

c = conn.cursor()
try:
    createschema(c)
finally:
    c.close()
del c

def get_or_createdb(c, name):
    try:
        c.execute("INSERT INTO databases(name) values(?)", (name,))
        return c.lastrowid
    except sqlite3.IntegrityError:
        return c.execute("SELECT id FROM databases WHERE name=?", (name,)).fetchone()[0]

def createdb(c, name):
    c.execute("INSERT INTO databases(name) values(?)", (name,))
    return c.lastrowid

def getdbid(c, name):
    return c.execute("SELECT id FROM databases WHERE name=?", (name,)).fetchone()[0]

def add_document(c, dbid, fname, last_modified, content):
    c.execute("INSERT INTO files(docid, dbid, path, last_modified) VALUES(NULL, ?, ?, ?)",
               (dbid, fname, last_modified))
    docid = c.lastrowid
    c.execute("INSERT INTO files_fts(docid, body) VALUES(?, ?)",
               (docid, content))
    return docid

def remove_document(c, docid):
    c.execute("DELETE FROM files WHERE docid=?", (docid,))
    c.execute("DELETE FROM files_fts WHERE docid=?", (docid,))

def update_document(c, docid, last_modified, content):
    c.execute("UPDATE files SET last_modified=? WHERE docid=?",
               (last_modified, docid))
    c.execute("UPDATE files_fts SET body=? WHERE docid=?",
               (content, docid))