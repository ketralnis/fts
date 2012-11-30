from ftsdb import Cursor

def list_ignores(conn):
    for (_id, typ, expression) in conn.execute("""
        SELECT id, type, expression FROM exclusions;
    """):
        print '\t'.join(map(str, (_id, typ, expression)))

def add_ignore(conn, typ, expression):
    assert typ in ('re', 'glob', 'simple')

    with Cursor(conn) as c:
        c.execute("INSERT INTO exclusions(id, type, expression) VALUES(NULL, ?, ?)", (typ, expression))
        return c.lastrowid

def rm_ignore(conn, ignoreid):
    with Cursor(conn) as c:
        c.execute("DELETE FROM exclusions WHERE id = ?", (ignoreid,))
