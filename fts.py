#!/usr/bin/env python2.7

import sys
import os
import logging
import argparse

from ftsdb import re # re or re2

from ftsdb import logger, Cursor
from ftsdb import finddb

from ftsinit import init
from ftssync import sync
from ftsexclude import add_ignore, list_ignores, rm_ignore
from ftssearch import search

def main():
    ap = argparse.ArgumentParser('fts', description="a command line full text search engine")

    ap.add_argument('--logging', default='warn', help=argparse.SUPPRESS,
                    choices = ('error', 'warn', 'info', 'debug'))

    ap.add_argument("--init", action="store_true", help="Create a new .fts.db in the current directory")
    ap.add_argument("--no-sync", dest='nosync', action="store_true", help="don't sync the database when making a new one. only valid with --init")

    ap.add_argument("--sync", dest='sync', action="store_true", help="sync the fts database with the files on disk")
    ap.add_argument("--optimize", action="store_true", help="optimize the sqlite database for size and performance")

    ap.add_argument('--sync-one', metavar='filename', help="sync a single file (unlike the other commands, this one doesn't care about the current directory)")

    ap.add_argument("--list-ignores", action='store_true', default=[])
    ap.add_argument("--rm-ignore", type=int, metavar='ignoreid', action='append', default=[])
    ap.add_argument("--ignore-re", metavar='re', action='append', default=[])
    ap.add_argument("--ignore-simple", metavar='filename', action='append', default=[])
    ap.add_argument('--ignore', "--ignore-glob", dest='ignore_glob', metavar='pattern', action='append', default=[])

    ap.add_argument('-r', '--re', '--regex', '--regexp', dest='searchmode',
                    default='MATCH', action="store_const", const='REGEXP',
                    help="search using a regex instead of MATCH syntax. Much slower!")

    ap.add_argument('-l', dest='display_mode', action='store_const', const='filename_only', help="print only the matching filenames")
    ap.add_argument('--color-mode', dest='color_mode', choices=('yes', 'no', 'auto'), default='auto')
    ap.add_argument('--color', dest='color_mode', action='store_const', const='yes')

    ap.add_argument("search", nargs="*")

    args = ap.parse_args()

    logger.setLevel(getattr(logging, args.logging.upper()))

    if args.color_mode == 'yes':
        color = True
    elif args.color_mode == 'no':
        color = False
    else:
        # it's 'auto'
        color = (os.isatty(sys.stdout.fileno())
                 and args.display_mode != 'filename_only'
                 and args.searchmode != 'REGEXP' # since we don't have snippets working here yet
                 )

    cwd = os.getcwd()
    didsomething = False
    exitval = 0

    if args.init:
        didsomething = True
        init(cwd)

    if args.sync_one:
        # this is designed to be called by tools like procmail or IDEs' on-save
        # hooks, so rather than making them play games with the cwd we have
        # special finddb logic for it. note that because of this we are
        # vulnerable to .fts.db files that shadow the intended one. Also note
        # that we may operate on a different .fts.db than other commands run in
        # the same session.
        # TODO: Maybe we should refuse to allow other commands to operate in the
        # same session for this reason
        fpath = args.sync_one
        if not fpath.startswith('/'):
            fpath = os.path.join(cwd, fpath)
        assert os.path.isfile(fpath)
        dirname, basename = os.path.dirname(fpath), os.path.basename(fpath)

        froot, fprefix, conn = finddb(dirname)

        assert fpath.startswith(os.path.join(froot, fprefix))

        with conn:
            sync(conn, froot, fprefix, files = [basename])

        didsomething = True


    root, prefix, conn = finddb(cwd)

    with conn:
        # all other top-level functions operate in one global transaction
        for a in args.rm_ignore:
            didsomething = True
            rm_ignore(conn, a)

        for a in args.ignore_re:
            didsomething = True
            try:
                re.compile(a)
            except:
                logging.error("Couldn't compile regex %r, are you sure it's valid?", a)
                raise
            add_ignore(conn, 're', a)
        for a in args.ignore_simple:
            didsomething = True
            add_ignore(conn, 'simple', a)
        for a in args.ignore_glob:
            didsomething = True
            add_ignore(conn, 'glob', a)

        if args.list_ignores:
            didsomething = True
            list_ignores(conn)

        dosync = args.sync or (args.init and not args.nosync)

        if dosync:
            didsomething = True
            sync(conn, root, prefix)

        if args.optimize:
            didsomething = True
            with Cursor(conn) as c:
                c.execute("INSERT INTO files_fts(files_fts) values('optimize');")
                c.execute("VACUUM ANALYZE;")

        for term in args.search:
            # for now, ANY search matching a document will return it, and it may be
            # returned twice
            didsomething = True

            for sr in search(conn, prefix, term, args.searchmode,
                             checksync=dosync, color=color):
                if args.display_mode == 'filename_only':
                    print sr.filename
                else:
                    print sr.format(color=color)

                # at least one result was returned
                exitval = 0

    if not didsomething:
        ap.print_usage()
        sys.exit(1)

    sys.exit(exitval)

if __name__ == '__main__':
    main()