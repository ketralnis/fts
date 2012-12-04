#!/usr/bin/env python2.7

import sys
import os
import stat
import logging
import itertools
from collections import namedtuple
import argparse

from ftsdb import logger, Cursor
from ftsdb import finddb, prefix_expr, getconfig

from ftsinit import init
from ftssync import sync
from ftsexclude import add_ignore, list_ignores, rm_ignore

snippet_color        = '\x1b[01;33m'
snippet_end_color    = '\x1b[00m'
snippet_elipsis      = ''.join([snippet_color, '...', snippet_end_color])

filename_color       = '\x1b[01;31m'
filename_end_color   = '\x1b[00m'

def grouper(n, iterable, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # from http://docs.python.org/2/library/itertools.html#recipes
    # grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)

SearchOffset = namedtuple('SearchOffset', ('offset', 'length'))

class SearchResult(object):
    __slots__ = ('filename', 'offsets', 'snippet')

    def __init__(self, filename, offsets, snippet):
        self.filename = filename
        self.offsets = self.parse_offsets(offsets)
        self.snippet = snippet

    def parse_offsets(self, offsets):
        ret = []

        nums = map(int, offsets.split())
        assert len(nums) % 4 == 0
        for colno, termno, offset, length in grouper(4, nums):
            ret.append(SearchOffset(offset, length))

        return ret

    def __repr__(self):
        return "%s(%r, %r, %r)" % (self.__class__.__name__,
                                   self.filename,
                                   self.offsets,
                                   self.snippet)

    def __str__(self):
        return filename_color + self.filename + filename_end_color + ':\n' + '\n'.join('\t' + x for x in self.snippet.split('\n'))


def search(conn, prefix, term, mode, checksync = True):
    assert mode in ('MATCH', 'REGEXP')
    with Cursor(conn) as c:
        prefix = prefix or ''
        prefixexpr = prefix_expr(prefix)
        needsync = 0
        c.execute("""
            SELECT f.path, f.last_modified,
                   offsets(ft.files_fts),
                   snippet(ft.files_fts, ?, ?, ?, -1, -10)
              FROM files f, files_fts ft
             WHERE f.docid = ft.docid
               AND (? = '' OR f.path LIKE ? ESCAPE '\\') -- use the prefix if present
               AND ft.body %(mode)s ?
          -- TODO: this runs simple_rank, which calls a Python function, many
          -- times per row. we can decompose this to a subselect to avoid this
          ORDER BY simple_rank(matchinfo(ft.files_fts))
        """ % dict(mode=mode), (snippet_color, snippet_end_color, snippet_elipsis, prefix, prefixexpr, term,))
        for (path, last_modified, offsets, snippet) in c:

            if prefix:
                assert path.startswith(prefix)

            # if they're in a subdirectory, deprefix the filename
            shortpath = path[len(prefix)+1:] if prefix else path

            if checksync:
                # check if the returned files are known to be out of date. this
                # can be skipped when checksync is False (which means that a
                # sync was done before starting the search)
                try:
                    st = os.stat(shortpath)
                    if int(st[stat.ST_MTIME]) > last_modified:
                        needsync += 1
                except OSError:
                    needsync += 1

            yield SearchResult(shortpath, offsets, snippet)

        if needsync:
            logger.warning("%d files were missing or out-of-date, you may need to resync", needsync)

def main():
    ap = argparse.ArgumentParser('fts', description="a command line full text search engine")

    ap.add_argument('--logging', default='warn', help=argparse.SUPPRESS,
                    choices = ('error', 'warn', 'info', 'debug'))

    ap.add_argument("--init", action="store_true", help="Create a new .fts.db in the current directory")
    ap.add_argument("--no-sync", dest='nosync', action="store_true", help="don't sync the database when making a new one. only valid with --init")
    ap.add_argument("--compress", action="store_true", help="compress file-contents in the database. only valid with --init. disables --regexp queries")

    ap.add_argument("--sync", dest='sync', action="store_true", help="sync the fts database with the files on disk")
    ap.add_argument("--optimize", action="store_true", help="optimize the sqlite database for size and performance")

    ap.add_argument('--sync-one', help="sync a single file (unlike the other commands, this one doesn't care about the current directory)")

    ap.add_argument("--list-ignores", action='store_true')
    ap.add_argument("--rm-ignore", type=int, metavar='ignoreid')
    ap.add_argument("--ignore-re", metavar='re')
    ap.add_argument("--ignore-simple", metavar='filename')
    ap.add_argument("--ignore-glob", metavar='pattern')
    ap.add_argument("--ignore", dest='ignore_glob', metavar='pattern', help="alias for --ignore-glob")

    ap.add_argument('--re', '--regex', '--regexp', dest='searchmode',
                    default='MATCH', action="store_const", const='REGEXP',
                    help="search using a regex instead of MATCH syntax. Much slower!")

    ap.add_argument('-l', dest='display_mode', action='store_const', const='filename_only', help="print only the matching filenames")

    ap.add_argument("search", nargs="*")

    args = ap.parse_args()

    logger.setLevel(getattr(logging, args.logging.upper()))

    cwd = os.getcwd()
    didsomething = False
    exitval = 0

    if args.init:
        didsomething = True
        init(cwd, compress=args.compress)
    elif args.compress:
        # we can't compress existing databases
        args.print_usage()
        sys.exit(1)

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

        dosync = args.sync or (args.init and not args.nosync)

        if dosync:
            didsomething = True
            sync(conn, root, prefix)

        if args.optimize:
            didsomething = True
            with Cursor(conn) as c:
                c.execute("INSERT INTO files_fts(files_fts) values('optimize');")
                c.execute("VACUUM ANALYZE;")

        if args.search:
            exitval = 2
            with Cursor(conn) as c:
                if args.searchmode == 'REGEXP' and getconfig(c, 'compressed'):
                    raise Exception("Can't do regexp matches against compressed database")

        for term in args.search:
            # for now, ANY search matching a document will return it, and it may be
            # returned twice
            didsomething = True

            for sr in search(conn, prefix, term, args.searchmode,
                             checksync=dosync):
                if args.display_mode == 'filename_only':
                    print sr.filename
                else:
                    print sr

                # at least one result was returned
                exitval = 0

    if not didsomething:
        ap.print_usage()
        sys.exit(1)

    sys.exit(exitval)

if __name__ == '__main__':
    main()