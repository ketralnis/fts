import sqlite3
import os.path
import logging

_db_name = '.fts.db'

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def createschema(c):
    # TODO: make everything case insensitive
    # TODO: track file hashes so that we can avoid reindexing renamed files

    c.execute("""
        CREATE TABLE IF NOT EXISTS
        config (
            key TEXT PRIMARY KEY,
            value
        );
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS
        exclusions (
            prefix TEXT PRIMARY KEY
        );
    """)

    # docid references the files_fts
    c.execute("""
        CREATE TABLE IF NOT EXISTS
        files (
            docid         INTEGER PRIMARY KEY AUTOINCREMENT,
            path NOT NULL,
            last_modified INTEGER NOT NULL
        );
    """)
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS files_path_idx ON files(path);")

    # normally we'd use "IF NOT EXISTS" but fts4 doesn't support it
    if not c.execute("SELECT DISTINCT tbl_name FROM sqlite_master WHERE tbl_name = 'files_fts'").fetchall():
        # has an invisible docid column
        c.execute("""
            CREATE VIRTUAL TABLE
            files_fts USING fts4 (
                body TEXT);
        """)

def connect(fname):
    conn = sqlite3.connect(fname)
    conn.text_factory=str
    conn.isolation_level = 'EXCLUSIVE'
    conn.execute('PRAGMA case_sensitive_like=ON;')
    return conn

class NoDB(Exception):
    pass

def finddb(initroot, root = None):
    # 'root' must be an absolute path

    if root is None:
        root = initroot

    initroot = initroot.rstrip('/')
    root = root.rstrip('/')

    dbfname = os.path.join(root, _db_name)
    if os.path.exists(dbfname):
        assert initroot.startswith(root)

        conn = connect(dbfname)
        prefix = initroot[len(root)+1:]
        return root, prefix, conn

    if root == '/':
        raise NoDB()

    components = os.path.split(root)
    parentcomponents = components[:-1]
    parent = os.path.join(*parentcomponents)

    return finddb(initroot, parent)

def createdb(root):
    # 'root' must be an absolute path
    dbfname = os.path.join(root, _db_name)
    conn = connect(dbfname)
    c = conn.cursor()
    try:
        createschema(c)
        return dbfname, conn
    finally:
        c.close()

def prefix_expr(prefix):
    prefixexpr = prefix.replace('\\', '\\\\').replace('%', '\\%',).replace('_', '\\_') + '%'
    return prefixexpr

def add_document(c, fname, last_modified, content):
    c.execute("INSERT INTO files(docid, path, last_modified) VALUES(NULL, ?, ?)",
               (fname, last_modified))
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