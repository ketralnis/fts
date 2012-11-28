#!/usr/bin/env python

import sys

from db import conn
from db import getdbid

def search(dbid, term):
    with conn:
        c = conn.cursor()
        try:
            c.execute("""
                SELECT path
                  FROM files f, files_fts ft
                 WHERE f.dbid = ?
                   AND f.docid = ft.docid
                   AND ft.body MATCH ?
           """, (dbid, term))
            for (path, ) in c:
                yield path
        finally:
            c.close()

def main():
    dbid = getdbid(conn.cursor(), "mydb")
    for fname in search(dbid, sys.argv[1]):
        print fname

if __name__ == '__main__':
    main()