import sqlite3
import os.path
import logging
import zlib
from functools import wraps

try:
    import re2 as re
except ImportError:
    import re

_db_name = '.fts.db'

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("fts")

def createschema(c, compress=False):
    c.execute("""
        CREATE TABLE IF NOT EXISTS
        config (
            key TEXT PRIMARY KEY,
            value
        );
    """)
    if compress:
        c.execute("INSERT INTO config(key, value) VALUES('compressed', 'true')")

    c.execute("""
        CREATE TABLE IF NOT EXISTS
        exclusions (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            type       TEXT NOT NULL COLLATE BINARY,
            expression TEXT NOT NULL COLLATE BINARY
        );
    """)
    c.execute("INSERT INTO exclusions(type, expression) VALUES('glob', '*.pyc')")
    c.execute("INSERT INTO exclusions(type, expression) VALUES('glob', '*~')")
    c.execute("INSERT INTO exclusions(type, expression) VALUES('simple', ?)", (_db_name,))
    c.execute("INSERT INTO exclusions(type, expression) VALUES('simple', '.svn')")
    c.execute("INSERT INTO exclusions(type, expression) VALUES('simple', '.git')")
    c.execute("INSERT INTO exclusions(type, expression) VALUES('simple', '.hg')")

    # docid references the files_fts
    c.execute("""
        CREATE TABLE IF NOT EXISTS
        files (
            docid         INTEGER PRIMARY KEY AUTOINCREMENT,
            path          NOT NULL COLLATE BINARY,
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
                body TEXT COLLATE BINARY NOT NULL
                %(compress)s );
        """ % dict(compress=", compress=GZIP, uncompress=GUNZIP" if compress else ""))

def getconfig(c, key, default=None):
    c.execute("SELECT value FROM config WHERE key= ? ", (key,))
    vals = list(c.fetchall())
    if vals:
        return vals[0][0]
    else:
        return default

def log_errors(fn):
    # sqlite swallows exceptions before reraising its own, so we'll add our own
    # logging
    @wraps(fn)
    def wrapper(*a):
        try:
            return fn(*a)
        except:
            logger.exception("Execution failed in %s", fn.__name__)
            raise
    return wrapper

@log_errors
def compress(body):
    # should be safe to str() because we're relying on binary collation for now
    if body is None:
        body = ''
    elif isinstance(body, unicode):
        try:
            body = body.encode('ascii')
        except UnicodeEncodeError:
            body = body.encode('utf-8')
    return zlib.compress(body)

@log_errors
def uncompress(body):
    return zlib.decompress(body or '')

@log_errors
def regexp(expr, item):
    item = item or ''
    reg = re.compile(expr)
    return reg.search(item) is not None

@log_errors
def ignore_simple(fullpath, pattern):
    r = re.compile('(^|/)'+re.escape(pattern)+'(/|$)')
    return r.search(fullpath) is not None

def connect(fname):
    conn = sqlite3.connect(fname)
    conn.text_factory=str
    conn.isolation_level = 'EXCLUSIVE'
    conn.execute('PRAGMA case_sensitive_like=ON;')

    # install our regex engine
    conn.create_function("REGEXP", 2, regexp)

    conn.create_function("GZIP", 1, compress)
    conn.create_function("GUNZIP", 1, uncompress)

    conn.create_function("IGNORE_SIMPLE", 2, ignore_simple)

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

    # splitting by hand means that we don't have to respect symlinks
    components = os.path.split(root)
    parentcomponents = components[:-1]
    parent = os.path.join(*parentcomponents)

    return finddb(initroot, parent)

def createdb(root, compress=False):
    # 'root' must be an absolute path
    dbfname = os.path.join(root, _db_name)
    conn = connect(dbfname)
    with Cursor(conn) as c:
        createschema(c, compress=compress)
    return dbfname, conn

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

class Cursor(object):
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        c = self.c = self.conn.cursor()
        return c

    def __exit__(self, type, value, traceback):
        self.c.close()
        del self.c
