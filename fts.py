#!/usr/bin/env python

import sys
import os

from db import finddb, prefix_expr

def search(conn, prefix, term):
    # TODO: do a pass over the results to see if the files still exist and if
    # last_modified has changed

    # TODO: get matchinfo and optionally display matches

    # TODO: cmdline switches to display the full path instead of the relative path

    with conn:
        c = conn.cursor()
        try:
            prefix = prefix or ''
            prefixexpr = prefix_expr(prefix)
            c.execute("""
                SELECT path
                  FROM files f, files_fts ft
                 WHERE f.docid = ft.docid
                   AND (? = '' OR f.path LIKE ? ESCAPE '\\') -- use the prefix if present
                   AND ft.body MATCH ?
           """, (prefix, prefixexpr, term,))
            for (path, ) in c:
                yield path[len(prefix)+1:] if prefix else path
        finally:
            c.close()

def main():
    root, prefix, conn = finddb(os.getcwd())
    for fname in search(conn, prefix, sys.argv[1]):
        print fname

if __name__ == '__main__':
    main()