#!/usr/bin/env python

import sys
import os
import stat
from argparse import ArgumentParser

from ftsdb import logger, Cursor
from ftsdb import finddb, prefix_expr

from ftsinit import init
from ftssync import sync
from ftsexclude import add_ignore, list_ignores, rm_ignore

def search(conn, prefix, term):
    with Cursor(conn) as c:
        prefix = prefix or ''
        prefixexpr = prefix_expr(prefix)
        needsync = 0
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

            try:
                st = os.stat(shortpath)
                if int(st[stat.ST_MTIME]) > last_modified:
                    needsync += 1
            except OSError:
                needsync += 1

            yield shortpath

        if needsync:
            logger.warning("%d files were missing or out-of-date, you may need to resync", needsync)

def main():
    ap = ArgumentParser('fts', description="a command line full text search engine")

    ap.add_argument("--init", action="store_true", help="Create a new .fts.db in the current directory")
    ap.add_argument("--no-sync", dest='nosync', action="store_true", help="don't sync the database when making a new one. only valid with --init")

    ap.add_argument("--sync", dest='sync', action="store_true", help="sync the fts database with the files on disk")
    ap.add_argument("--optimize", action="store_true", help="optimize the sqlite database for size and performance")

    ap.add_argument("--list-ignores", action='store_true')
    ap.add_argument("--rm-ignore", type=int, metavar='ignoreid')
    ap.add_argument("--ignore-re", metavar='re')
    ap.add_argument("--ignore-simple", metavar='filename')
    ap.add_argument("--ignore-glob", metavar='pattern')

    ap.add_argument("searches", nargs="*")

    args = ap.parse_args()

    cwd = os.getcwd()
    didsomething = False
    exitval = 0

    if args.init:
        didsomething = True
        init(cwd)

    root, prefix, conn = finddb(cwd)

    with conn:
        # all other top-level functions operate in one global transaction
        if args.rm_ignore:
            didsomething = True
            rm_ignore(conn, args.rm_ignore)

        if args.ignore_re:
            didsomething = True
            add_ignore(conn, 're', args.ignore_re)
        if args.ignore_simple:
            didsomething = True
            add_ignore(conn, 'simple', args.ignore_simple)
        if args.ignore_glob:
            didsomething = True
            add_ignore(conn, 'glob', args.ignore_glob)

        if args.list_ignores:
            didsomething = True
            list_ignores(conn)

        if args.sync or (args.init and not args.nosync):
            didsomething = True
            sync(conn, root, prefix)

        if args.optimize:
            didsomething = True
            with Cursor(conn) as c:
                c.execute("INSERT INTO files_fts(files_fts) values('optimize');")
                c.execute("VACUUM ANALYZE;")

        for term in args.searches:
            # for now, ANY search matching a document will return it, and it may be
            # returned twice
            didsomething = True

            exitval = 1

            for fname in search(conn, prefix, sys.argv[1]):
                print fname
                exitval = 0

    if not didsomething:
        ap.print_usage()
        sys.exit(1)

    sys.exit(exitval)

if __name__ == '__main__':
    main()