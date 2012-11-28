#!/usr/bin/env python

import sys
import os
import stat

from db import logger
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
                SELECT f.path, f.last_modified
                  FROM files f, files_fts ft
                 WHERE f.docid = ft.docid
                   AND (? = '' OR f.path LIKE ? ESCAPE '\\') -- use the prefix if present
                   AND ft.body MATCH ?
           """, (prefix, prefixexpr, term,))
            for (path, last_modified) in c:

                if prefix:
                    assert path.startswith(prefix)

                shortpath = path[len(prefix)+1:] if prefix else path

                needsync = 0

                try:
                    st = os.stat(shortpath)
                    if int(st[stat.ST_MTIME]) > last_modified:
                        needsync += 1
                except OSError:
                    needsync += 1

                yield shortpath

            if needsync:
                logger.warning("%d files were missing or out-of-date, you may need to resync", needsync)

        finally:
            c.close()

def main():
    root, prefix, conn = finddb(os.getcwd())
    for fname in search(conn, prefix, sys.argv[1]):
        print fname

if __name__ == '__main__':
    main()