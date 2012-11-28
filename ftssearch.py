#!/usr/bin/env python

import sys
import os

from db import finddb

def search(conn, prefix, term):
    with conn:
        c = conn.cursor()
        try:
            c.execute("""
                SELECT path
                  FROM files f, files_fts ft
                 WHERE f.docid = ft.docid
                   AND ft.body MATCH ?
           """, (term,))
            for (path, ) in c:
                yield path
        finally:
            c.close()

def main():
    root, prefix, conn = finddb(os.getcwd())
    for fname in search(conn, sys.argv[1]):
        print fname

if __name__ == '__main__':
    main()