#!/usr/bin/env python

def list_ignores(conn):
    for (_id, typ, expression) in conn.execute("""
        SELECT id, type, expression FROM exclusions;
    """):
        print '\t'.join(map(str, (_id, typ, expression)))

def add_ignore(conn, typ, expression):
    assert typ in ('re', 'glob', 'simple')

    with conn:
        c = conn.cursor()
        try:
            c.execute("INSERT INTO exclusions(id, type, expression) VALUES(NULL, ?, ?)", (typ, expression))
            return c.lastrowid
        finally:
            c.close()

def rm_ignore(conn, ignoreid):
    with conn:
        c = conn.cursor()
        try:
            c.execute("DELETE FROM exclusions WHERE id = ?", (ignoreid,))
        finally:
            c.close()
